import asyncio
from aiohttp import web
import reactivex as rx
from reactivex import operators as ops
import json
import logging
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
                html = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Sensor Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-gray-100 min-h-screen py-8">
    <div class="container mx-auto px-4 max-w-7xl">
        <h1 class="text-4xl font-bold text-center mb-12 text-gray-800">IoT Sensor Dashboard</h1>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
            <div class="bg-white rounded-xl shadow-lg p-6">
                <h2 class="text-2xl font-semibold mb-4">Temperatura (°C)</h2>
                <canvas id="tempChart" height="300" maintainAspectRatio: false></canvas>
            </div>
            <div class="bg-white rounded-xl shadow-lg p-6">
                <h2 class="text-2xl font-semibold mb-4">Humedad (%)</h2>
                <canvas id="humChart" height="300" maintainAspectRatio: false></canvas>
            </div>
        </div>

        <div class="bg-white rounded-xl shadow-lg p-6">
            <h2 class="text-2xl font-semibold mb-4">Últimos Registros</h2>
            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Sensor ID</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Temperatura</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Humedad</th>
                        </tr>
                    </thead>
                    <tbody id="table-body" class="bg-white divide-y divide-gray-200">
                        <tr><td colspan="4" class="px-6 py-4 text-center text-gray-500">Esperando datos...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        let tempChart = null;
        let humChart = null;

        function updateDashboard() {
            fetch('/data')
                .then(response => response.json())
                .then(json => {
                    const data = json.data;

                    // Actualizar tabla
                    const tbody = document.getElementById('table-body');
                    tbody.innerHTML = '';
                    if (data.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="4" class="px-6 py-4 text-center text-gray-500">No hay datos aún</td></tr>';
                    } else {
                        data.forEach(row => {
                            const tr = document.createElement('tr');
                            const ts = row.timestamp ? new Date(row.timestamp * 1000).toLocaleString() : 'N/A';
                            const hum = row.humedad !== null ? row.humedad.toFixed(1) + ' %' : 'N/A';
                            tr.innerHTML = `
                                <td class="px-6 py-4 text-sm text-gray-900">${ts}</td>
                                <td class="px-6 py-4 text-sm text-gray-900">${row.sensor_id}</td>
                                <td class="px-6 py-4 text-sm text-gray-900">${row.temperatura.toFixed(1)} °C</td>
                                <td class="px-6 py-4 text-sm text-gray-900">${hum}</td>
                            `;
                            tbody.appendChild(tr);
                        });
                    }

                    // Actualizar gráficas
                    if (data.length > 0) {
                        const sensors = [...new Set(data.map(d => d.sensor_id))];
                        const colors = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6'];

                        const labels = data.map(d => new Date(d.timestamp * 1000).toLocaleTimeString());

                        const datasetsTemp = sensors.map((s, i) => ({
                            label: s,
                            data: data.filter(d => d.sensor_id === s).map(d => d.temperatura),
                            borderColor: colors[i % colors.length],
                            tension: 0.4,
                            fill: false
                        }));

                        const datasetsHum = sensors.map((s, i) => ({
                            label: s,
                            data: data.filter(d => d.sensor_id === s).map(d => d.humedad || null),
                            borderColor: colors[i % colors.length],
                            tension: 0.4,
                            fill: false
                        }));

                        if (!tempChart) {
                            tempChart = new Chart(document.getElementById('tempChart'), {
                                type: 'line',
                                data: { labels, datasets: datasetsTemp },
                                options: { responsive: true, maintainAspectRatio: false }
                            });
                        } else {
                            tempChart.data = { labels, datasets: datasetsTemp };
                            tempChart.update();
                        }

                        if (!humChart) {
                            humChart = new Chart(document.getElementById('humChart'), {
                                type: 'line',
                                data: { labels, datasets: datasetsHum },
                                options: { responsive: true, maintainAspectRatio: false, scales: { y: { min: 0, max: 100 } } }
                            });
                        } else {
                            humChart.data = { labels, datasets: datasetsHum };
                            humChart.update();
                        }
                    }
                })
                .catch(err => console.error('Error:', err));
        }

        // Carga inicial y cada 5 segundos
        updateDashboard();
        setInterval(updateDashboard, 5000);
    </script>
</body>
</html>
                """

                return {
                    'what': 'response',
                    'status': 200,
                    'context': request_item['context'],
                    'data': html.encode('utf-8')
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