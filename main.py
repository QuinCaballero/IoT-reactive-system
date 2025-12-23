# Usage example:
# curl -X POST http://localhost:8080/sensor_data -H "Content-Type: application/json" -d "{\"temperatura\":25.5,\"sensor_id\":\"S1\"}"
# curl -X POST http://localhost:8080/sensor_data -H "Content-Type: application/json" -d @data.json

# Global imports
import asyncio
from reactivex.subject import Subject, BehaviorSubject
import pandas as pd
import logging

# Components
from driver_http import http_driver, app_server  # app_server ahora es función
from processor import data_processor

# Config the logging system
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##########
# main
##########

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    http_proxy = Subject()
    processor_proxy = Subject()
    
    dataframe_subject = BehaviorSubject(pd.DataFrame())
    
    sources = {
        'http': http_driver(http_proxy, loop),
        'processor': processor_proxy,
        'dataframe': dataframe_subject,
    }
    
    # Conectamos to processor getting dataframe_subject
    data_processor(sources, dataframe_subject)
    
    # Driver settings
    http_sinks = app_server(sources)  # ← Aquí le pasamos sources
    http_sinks["http"].subscribe(http_proxy)

    # Processor settings
    # processor_outputs = data_processor(sources)

    # # Optional: Subscription of the actualized DataFrame
    # processor_outputs['dataframe'].subscribe(
    #     on_next=lambda df: logging.info(f"DataFrame actualizado: {df.shape}")
    # )
    
    dataframe_subject.subscribe(
        on_next=lambda df: logging.info(f"DataFrame actualizado: {df.shape} filas")
    )
    # Ver próximos pasos

    print("=== Server ready ===")
    print("POST http://localhost:8080/sensor_data → send data")
    print("GET  http://localhost:8080/data         → check DataFrame aggregated")
    print("Ctrl+C to exit")

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nServer shutdown...")
    finally:
        loop.close()

if __name__ == '__main__':
    main()
    
    '''
    Próximos pasos para conectar procesadores lanchain y redes neuronales a la salida del procesador:
    En main:
    from ai_modules import detector_fraude, procesador_langchain
    
    Añadir la bifurcación a processor outputs:
    ia_pipeline = processor_outputs['dataframe'].pipe(
        ops.map(lambda df: df.iloc[-1].to_dict()),    # Tomamos el último registro
        ops.filter(lambda data: data.get('tipo') == 'reclamo'), # Filtro opcional
        # Aquí vendría tu lógica de LangChain o Red Neuronal
    )

    # La suscribimos de forma independiente
        ia_pipeline.subscribe(
        on_next=lambda res: print(f"Resultado del análisis IA: {res}"),
        on_error=lambda e: logging.error(f"Fallo en el módulo de IA: {e}")
    )

    Se enchufa el detector de fraude a la salida del procesador actual -processor
    info_detectada = processor_outputs['dataframe'].pipe(
        ops.map(detector_fraude)
    )

    # Y también el procesador LangChain para los casos sospechosos
    resumenes_ia = info_detectada.pipe(
        ops.filter(lambda x: x['fraude'] == True),
        ops.flat_map(procesador_langchain)
    )
    
    ############################
    Para conectar los componentes mediante un brige (para despliegue, sustituye intercambio de Subject en memoria por uno que envia a la red):
    ############################
    Para mandar los mensajes de forma asíncrona usamos un broker de mensajes NATS, RabbitMQ o Redis como el "Proxy" externo.

    El Driver publica en una cola.

    El Procesador se suscribe a esa cola.

    RxPy sigue gestionando la lógica interna de cada pod, pero la "tubería" entre ellos es la cola.
    Por ejemplo para Redis (driver de red):
    
    class RedisBridgeSubject:
        def on_next(self, item):
            redis_client.publish('canal_datos', json.dumps(item))
    '''
