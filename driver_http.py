import asyncio
from aiohttp import web
import reactivex as rx
from reactivex import operators as ops
import json
import logging
from pathlib import Path
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
                data_bytes, status = response_future.result()

                response = web.StreamResponse(status=status)
                
                # Detectar si es HTML
                if isinstance(data_bytes, bytes) and data_bytes:
                    stripped = data_bytes.lstrip()
                    if stripped.startswith(b'<!DOCTYPE html>') or stripped.startswith(b'<html'):
                        response.content_type = "text/html; charset=utf-8"
                    else:
                        response.content_type = "application/json"
                else:
                    response.content_type = "application/json"
                
                
                await response.prepare(request)
                if data_bytes is not None:
                    await response.write(data_bytes)
                await response.drain()
                return response

            for method in methods:
                app.router.add_route(method, path, lambda r: on_request_data(r, path))

        def start_server(host, port, app):
            nonlocal runner
            runner = web.AppRunner(app)
            async def _start():
                await runner.setup()
                site = web.TCPSite(runner, host, port)
                await site.start()
            loop.create_task(_start())

        def on_sink_item(i):
            nonlocal runner
            if i['what'] == 'response':
                i['context'].set_result((i.get('data', b''), i.get('status', 200)))
            elif i['what'] == 'add_route':
                add_route(app, i['methods'], i['path'])
            elif i['what'] == 'start_server':
                start_server(i['host'], i['port'], app)

        app = web.Application()
        sink.subscribe(on_next=on_sink_item)

    return rx.create(on_subscribe)


def app_server(sources):
    init = rx.from_([
        {'what': 'add_route', 'methods': ['POST'], 'path': '/sensor_data'},
        {'what': 'add_route', 'methods': ['GET'],  'path': '/data'},
        {'what': 'add_route', 'methods': ['GET'],  'path': '/'},  # Dashboard
        {'what': 'start_server', 'host': 'localhost', 'port': 8080}
    ])

    def handle_request(request_item):
        try:
            method = request_item['method']
            path = request_item['path']

            if method == 'POST' and path == '/sensor_data':
                # === Tu código POST completo (validación, processor, etc.) ===
                data_bytes = request_item['data']
                if not data_bytes:
                    return {
                        'what': 'response',
                        'status': 400,
                        'context': request_item['context'],
                        'data': json.dumps({"error": "Empty body"}).encode('utf-8')
                    }

                try:
                    payload_dict = json.loads(data_bytes)
                except json.JSONDecodeError as e:
                    return {
                        'what': 'response',
                        'status': 400,
                        'context': request_item['context'],
                        'data': json.dumps({"error": "Invalid JSON", "details": str(e)}).encode('utf-8')
                    }

                try:
                    if isinstance(payload_dict, list):
                        validated = [SensorData(**item) for item in payload_dict]
                        validated_payload = [item.model_dump() for item in validated]
                        count = len(validated)
                    else:
                        validated = SensorData(**payload_dict)
                        validated_payload = validated.model_dump()
                        count = 1
                except PydanticValidationError as e:
                    error_list = [f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
                    return {
                        'what': 'response',
                        'status': 400,
                        'context': request_item['context'],
                        'data': json.dumps({"error": "Validation failed", "details": error_list}).encode('utf-8')
                    }

                sources['processor'].on_next({
                    'payload': validated_payload,
                    'raw_data': data_bytes,
                })

                return {
                    'what': 'response',
                    'status': 200,
                    'context': request_item['context'],
                    'data': json.dumps({"status": "accepted", "received_items": count}).encode('utf-8')
                }

            elif method == 'GET' and path == '/data':
                current_df = sources['dataframe'].value
                if current_df.empty or 'timestamp' not in current_df.columns:
                    data_json = []
                else:
                    data_json = current_df.sort_values('timestamp', ascending=False).to_dict(orient='records')

                return {
                    'what': 'response',
                    'status': 200,
                    'context': request_item['context'],
                    'data': json.dumps({"rows": len(current_df), "data": data_json}).encode('utf-8')
                }

            elif method == 'GET' and path == '/':
                try:
                    # Ruta absoluta al index.html, relativa al ubicación de este archivo
                    base_dir = Path(__file__).parent.resolve()  # Directorio de driver_http.py
                    html_path = base_dir / "static" / "index.html"

                    if not html_path.exists():
                        logging.error(f"Archivo HTML no encontrado: {html_path}")
                        return {
                            'what': 'response',
                            'status': 500,
                            'context': request_item['context'],
                            'data': b"Internal Server Error: index.html not found"
                        }

                    html = html_path.read_text(encoding='utf-8')

                    return {
                        'what': 'response',
                        'status': 200,
                        'context': request_item['context'],
                        'data': html.encode('utf-8')
                    }
                except Exception as e:
                    logging.error(f"Error cargando HTML: {e}")
                    return {
                        'what': 'response',
                        'status': 500,
                        'context': request_item['context'],
                        'data': b"Internal Server Error"
                    }

            else:
                return {
                    'what': 'response',
                    'status': 404,
                    'context': request_item['context'],
                    'data': json.dumps({"error": "Not found"}).encode('utf-8')
                }

        except Exception as e:
            logging.error(f"Error inesperado en handle_request: {e}", exc_info=True)
            return {
                'what': 'response',
                'status': 500,
                'context': request_item['context'],
                'data': b"Internal Server Error"
            }

    listener = sources['http'].pipe(ops.map(handle_request))

    return {'http': rx.merge(init, listener)}