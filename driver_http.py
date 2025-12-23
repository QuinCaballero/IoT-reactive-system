import asyncio
from aiohttp import web
import reactivex as rx
from reactivex import operators as ops
import logging
import json
from models import SensorData, ValidationError as PydanticValidationError


def http_driver(sink, loop):
    app = None
    runner = None

    def on_subscribe(observer, scheduler):
        def add_route(app, methods, path):
            async def on_request_data(request, path):
                data = await request.read()
                response_future = asyncio.Future()
                request_item = {
                    'method': request.method,
                    'path': path,
                    'match_info': request.match_info,
                    'data': data,
                    'context': response_future,
                }
                observer.on_next(request_item)
                await response_future
                data, status = response_future.result()

                response = web.StreamResponse(status=status, reason=None)
                response.content_type = "application/json"
                await response.prepare(request)
                if data is not None:
                    # If needed do something with the data
                    await response.write(data)
                return response # Send the payload

            for method in methods:
                app.router.add_route(
                    method, path,
                    lambda r: on_request_data(r, path))

        def start_server(host, port, app):
            runner = web.AppRunner(app)

            async def _start_server(runner):
                await runner.setup()
                site = web.TCPSite(runner, host, port)
                await site.start()

            loop.create_task(_start_server(runner))
            return runner

        def stop_server(runner):
            async def _stop_server():
                await runner.cleanup()

            loop.create_task(_stop_server())

        def on_sink_item(i):
            nonlocal runner
            # Remote Procedure Calls
            if i['what'] == 'response':
                response_future = i['context']
                response_future.set_result((i['data'], i['status']))
            elif i['what'] == 'add_route':
                add_route(app, i['methods'], i['path'])
            elif i['what'] == 'start_server':
                runner = start_server(i['host'], i['port'], app)
            elif i['what'] == 'stop_server':
                stop_server(runner)

        def on_sink_error(e):
            observer.on_error(e)

        def on_sink_completed():
            observer.on_completed()

        app = web.Application()
        sink.subscribe(
            on_next=on_sink_item,
            on_error=on_sink_error,
            on_completed=on_sink_completed)

    return rx.create(on_subscribe)


def app_server(sources):
    init = rx.from_([
        {'what': 'add_route', 'methods': ['POST'], 'path': '/sensor_data'},
        {'what': 'add_route', 'methods': ['GET'],  'path': '/data'},
        {'what': 'start_server', 'host': 'localhost', 'port': 8080}
    ])
    
    def handle_request(request_item):
        method = request_item['method']
        path = request_item['path']  # Sdefines in add_route

        # ====== POST /sensor_data ======
        if method == 'POST' and path == '/sensor_data':
            data_bytes = request_item['data']

            if not data_bytes:
                error_resp = json.dumps({"error": "Empty body"}).encode('utf-8')
                return {
                    'what': 'response',
                    'status': 400,
                    'context': request_item['context'],
                    'data': error_resp
                }

            try:
                payload_dict = json.loads(data_bytes)
            except json.JSONDecodeError as e:
                error_resp = json.dumps({
                    "error": "Invalid JSON",
                    "details": str(e)
                }).encode('utf-8')
                return {
                    'what': 'response',
                    'status': 400,
                    'context': request_item['context'],
                    'data': error_resp
                }

            # Validation with Pydantic
            try:
                if isinstance(payload_dict, list):
                    validated = [SensorData(**item) for item in payload_dict]
                    validated_payload = [item.dict() for item in validated]
                    count = len(validated)
                else:
                    validated = SensorData(**payload_dict)
                    validated_payload = validated.dict()
                    count = 1
            except PydanticValidationError as e:
                error_list = [f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
                error_resp = json.dumps({
                    "error": "Validation failed",
                    "details": error_list
                }).encode('utf-8')
                return {
                    'what': 'response',
                    'status': 400,
                    'context': request_item['context'],
                    'data': error_resp
                }

            # SUCCESS! Send to processor
            sources['processor'].on_next({
                'payload': validated_payload,
                'raw_data': data_bytes,
            })

            success_resp = json.dumps({
                "status": "accepted",
                "received_items": count
            }).encode('utf-8')

            return {
                'what': 'response',
                'status': 200,
                'context': request_item['context'],
                'data': success_resp
            }

        # ====== GET /data ======
        elif method == 'GET' and path == '/data':
            current_df = sources['dataframe'].value  # BehaviorSubject

            if current_df.empty:
                data_json = []
                rows = 0
            else:
                data_json = current_df.to_dict(orient='records')
                rows = len(current_df)

            response = json.dumps({
                "rows": rows,
                "data": data_json
            }).encode('utf-8')

            return {
                'what': 'response',
                'status': 200,
                'context': request_item['context'],
                'data': response
            }

        # ====== 404 ======
        else:
            return {
                'what': 'response',
                'status': 404,
                'context': request_item['context'],
                'data': json.dumps({"error": "Not found"}).encode('utf-8')
            }

    listener = sources['http'].pipe(
        ops.map(handle_request)
    )

    return {
        'http': rx.merge(init, listener),
    }