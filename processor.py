import pandas as pd
import numpy as np
from reactivex.subject import BehaviorSubject
import logging

def data_processor(sources, df_subject):
    """
    sources: dict with 'processor' (Subject)
    dataframe_subject: BehaviorSubject where emit DataFrame
    """
    current_df = pd.DataFrame()

    def on_payload(item):
        nonlocal current_df
        payload = item['payload']

        # One dict
        if isinstance(payload, dict):
            new_row = pd.DataFrame([payload])
        # Multiple dicts
        elif isinstance(payload, list):
            new_row = pd.DataFrame(payload)
        else:
            print("Payload not convertible to a DataFrame:", payload)
            return

        current_df = pd.concat([current_df, new_row], ignore_index=True)
        
        print("\n=== New DataFrame aggregated ===")
        print(current_df.tail(5))  # Show DataFrame tail
        print(f"Total rows: {len(current_df)}\n")

        # Emit new state of DatafFrame to subscribers
        df_subject.on_next(current_df.copy())

        # Send to csv
        # current_df.to_csv('sensor_data.csv', index=False)
        '''
        Proximos pasos:
        - Añadir un endpoint GET /data que devuelva el DataFrame actual en JSON.
        - Guardar automáticamente el CSV cada N filas o cada X segundos.
        - Añadir validación de esquema (ej: con pydantic) y rechazar payloads inválidos con 400.
        - Crear un driver WebSocket para que un frontend vea el DataFrame en tiempo real.
        - Añadir un timer que cada minuto imprima estadísticas (media de temperatura, etc.).
        - Nuevo handler para envío a MongoDB insert_many(df.to_dict('records'))
        '''

    sources['processor'].subscribe(on_next=on_payload)

    # return {
    #     'dataframe': df_subject,  # Observable : the Dataframe
    # }