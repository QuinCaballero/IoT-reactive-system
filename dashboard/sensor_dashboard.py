from dash import Dash, html, dcc, dash_table, Input, Output, State
import plotly.express as px
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
from flask_caching import Cache


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
        
        return df.sort_values('timestamp_dt', ascending=False)
    
    except Exception as e:
        print(f"Error al obtener datos de MongoDB: {e}")
        return pd.DataFrame()

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Asegurar que timestamp_dt sea datetime
    df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"])

    # Crear columnas Fecha y Hora
    df["Fecha"] = df["timestamp_dt"].dt.strftime("%d/%m/%Y")
    df["Hora"] = df["timestamp_dt"].dt.strftime("%H:%M:%S")

    # Construir el dataframe final
    df_final = pd.DataFrame({
        "Fecha": df["Fecha"].astype(str),
        "Hora": df["Hora"].astype(str),
        "Sensor": df["sensor_id"].astype(str),
        "Temperatura": df["temperatura"].astype(float),
        "Humedad": df["humedad"].astype(float)
    })

    return df_final

df = get_data()

app = Dash(__name__, title="IoT Sensor Dashboard", external_stylesheets=[dbc.themes.MINTY])

cache = Cache(
    app.server,
    config={
        "CACHE_TYPE": "SimpleCache",
        "CACHE_DEFAULT_TIMEOUT": 10  # segundos
    }
)

@cache.memoize()
def get_data_cached(limit=5000):
    return get_data(limit)

app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.H2("IoT Sensor Dashboard", className="text-4xl font-bold text-center my-8 text-gray-800"),
            )
        ),
        dbc.Row(
            [
                 dbc.Col(
                        dbc.Col(dcc.Graph(id="temp-graph", className="mb-2", style={"height": 400, "with": 600, "autosize": False}), className="pt-4"),
                    ),
                    dbc.Col(
                        dbc.Col(dcc.Graph(id="hum-graph", className="mb-2", style={"height": 325, "with": 600,  "autosize": False}), className="pt-4"),
                    ),
            ]
        ),
        dbc.Row(
            [
                dcc.Dropdown(id='sensor-filter', placeholder="Filtrar por sensor", multi=True, style={"with": 300, "autosize": False}),
                html.Div(id='stats', className="my-8 text-center text-gray-600"),
                dcc.Interval(id='interval', interval=10*1000, n_intervals=0, disabled=False)  # Cada 10s
            ]
        ),
       
       dbc.Row(
           dbc.Col(
               dash_table.DataTable(
                   id="data-table",
                   columns=(
                       [
                           {"id": "Fecha", "name": "Fecha", "type": "text"},
                           {"id": "Hora", "name": "Hora", "type": "text"},
                           {"id": "Sensor", "name": "Sensor", "type": "text"},
                           {"id": "Temperatura", "name": "Temperatura", "type": "numeric"},
                           {"id": "Humedad", "name": "Humedad", "type": "numeric"},
                       ]
                   ),
                   data=transform_df(df).to_dict("records"),
                   sort_action="native",
                   page_size=15,
                   page_current=0,
                   style_table={"height": "400px","overflowY": "auto"},
                )
           )
        )
       
    ],
    fluid=True,
)


@app.callback(
    Output('temp-graph', 'figure'),
    Output('hum-graph', 'figure'),
    Output('sensor-filter', 'options'),
    Output('stats', 'children'),
    Output('data-table','data'),
    Input('interval', 'n_intervals'),
    Input('sensor-filter', 'value')
)
def update_dashboard(n, selected_sensor):
    df = get_data_cached(5000)
    
    if df.empty:
        empty_fig = px.line(title="No hay datos aún")
        return empty_fig, empty_fig, [], None, "No hay datos disponibles"
    
    # Opciones dropdown
    sensors = sorted(df['sensor_id'].unique())
    options = [{'label': s, 'value': s} for s in sensors]
    
    # Filtrar
    if selected_sensor:
        df = df[df['sensor_id'].isin(selected_sensor)]
    
    df2 = transform_df(df.sort_values('timestamp_dt', ascending=False))
    
    # Estadísticas
    total = len(df)
    stats = (
        f"Mostrando {len(df)} registros | "
        f"Sensores seleccionados: {len(selected_sensor) if selected_sensor else len(sensors)}"
    )
    
    df_plot = df.sort_values('timestamp_dt')
    
    # Gráficas
    fig_temp = px.line(
        df_plot, x='timestamp_dt', y='temperatura', color='sensor_id',
        title='Temperatura por Sensor',
        labels={'timestamp_dt': 'Fecha/Hora', 'temperatura': '°C'}
    )
    
    fig_hum = px.line(
        df_plot, x='timestamp_dt', y='humedad', color='sensor_id',
        title='Humedad por Sensor',
        labels={'timestamp_dt': 'Fecha/Hora', 'humedad': '%'}
    )
    
    fig_temp.update_layout(transition_duration=500)
    fig_hum.update_layout(transition_duration=500)
    
    # Tabla
    table_data = df2.to_dict("records")
    
    return fig_temp, fig_hum, options, stats, table_data

if __name__ == '__main__':
    app.run(port=8050, debug=False)