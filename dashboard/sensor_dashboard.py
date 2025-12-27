from dash import Dash, html, dcc, Input, Output, State
import plotly.express as px
import dash_bootstrap_components as dbc
from datetime import datetime
from pymongo import MongoClient
import pandas as pd


mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client["sensor_db"]
collection = db["sensor_data"]

def get_data(limit=5000):
    try:
        # Cursor síncrono → iterable directamente
        cursor = collection.find({}).sort("timestamp", -1).limit(limit)
        data = list(cursor)  # Convertimos a lista
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # Convertimos _id a string y timestamp a datetime legible
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)
        
        if 'timestamp' in df.columns:
            df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='s')
            df = df.sort_values('timestamp_dt')  # Cronológico para gráficas
        
        return df
    
    except Exception as e:
        print(f"Error al obtener datos de MongoDB: {e}")
        return pd.DataFrame()

app = Dash(__name__, title="IoT Sensor Dashboard", external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1("IoT Sensor Dashboard", className="text-4xl font-bold text-center my-8 text-gray-800"),
    
    dcc.Dropdown(id='sensor-filter', placeholder="Filtrar por sensor", multi=False),
    
    dcc.Graph(id='temp-graph'),
    dcc.Graph(id='hum-graph'),
    
    html.Div(id='stats', className="my-8 text-center text-gray-600"),
    
    dcc.Interval(id='interval', interval=10*1000, n_intervals=0)  # Cada 10s
])

@app.callback(
    Output('temp-graph', 'figure'),
    Output('hum-graph', 'figure'),
    Output('sensor-filter', 'options'),
    Output('sensor-filter', 'value'),
    Output('stats', 'children'),
    Input('interval', 'n_intervals'),
    Input('sensor-filter', 'value')
)
def update_dashboard(n, selected_sensor):
    df = get_data(limit=5000)  # Ajusta según tus datos
    
    if df.empty:
        empty_fig = px.line(title="No hay datos aún")
        return empty_fig, empty_fig, [], None, "No hay datos disponibles"
    
    # Opciones dropdown
    sensors = sorted(df['sensor_id'].unique())
    options = [{'label': s, 'value': s} for s in sensors]
    
    # Filtrar
    if selected_sensor:
        df = df[df['sensor_id'] == selected_sensor]
    
    # Estadísticas
    total = len(df)
    stats = f"Mostrando {total} registros | Sensores activos: {len(sensors)}"
    
    # Gráficas
    fig_temp = px.line(
        df, x='timestamp_dt', y='temperatura', color='sensor_id',
        title='Temperatura por Sensor',
        labels={'timestamp_dt': 'Fecha/Hora', 'temperatura': '°C'}
    )
    
    fig_hum = px.line(
        df, x='timestamp_dt', y='humedad', color='sensor_id',
        title='Humedad por Sensor',
        labels={'timestamp_dt': 'Fecha/Hora', 'humedad': '%'}
    )
    
    fig_temp.update_layout(transition_duration=500)
    fig_hum.update_layout(transition_duration=500)
    
    return fig_temp, fig_hum, options, selected_sensor, stats

if __name__ == '__main__':
    app.run(port=8050, debug=True)
