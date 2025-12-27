import asyncio
import logging
import time
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Any

def mongo_persistence(sources, mongo_client: AsyncIOMotorClient):
    """
    Persiste los payloads validados en MongoDB de forma asíncrona
    """
    db = mongo_client["sensor_db"]
    collection = db["sensor_data"]

    async def insert_payload(item):
        payload = item['payload']

        # Normalizamos: siempre una lista de documentos
        if isinstance(payload, dict):
            documents = [payload]
        else:
            documents = list(payload)  # por si viene tupla o iterable

        if not documents:
            return

        try:
            result = await collection.insert_many(documents, ordered=False)
            logging.info(f"Insertados {len(result.inserted_ids)} documentos en MongoDB")
        except Exception as e:
            logging.error(f"Error al insertar en MongoDB: {e}")

    # Suscripción al stream de payloads validados
    sources['processor'].subscribe(
        on_next=lambda item: asyncio.create_task(insert_payload(item)),
        on_error=lambda e: logging.error(f"Error en stream processor: {e}")
    )
    
    # Limpieza de la base de datos
    async def cleanup_old_records():
        while True:
            try:
                now = time.time()
                cutoff_timestamp = now - (2 * 365 * 24 * 3600)  # 2 años en segundos

                # Condición: timestamp ausente, None, no numérico, o más antiguo de 2 años
                result = await collection.delete_many({
                    "$or": [
                        {"timestamp": {"$exists": False}},
                        {"timestamp": None},
                        {"timestamp": {"$not": {"$type": "number"}}},
                        {"timestamp": {"$lt": cutoff_timestamp}}
                    ]
                })

                if result.deleted_count > 0:
                    logging.info(f"Limpieza completada: eliminados {result.deleted_count} registros antiguos o inválidos")
                else:
                    logging.debug("Limpieza ejecutada: no hay registros para eliminar")

            except Exception as e:
                logging.error(f"Error durante la limpieza de MongoDB: {e}")

            # Espera 1 hora antes de la siguiente limpieza
            await asyncio.sleep(3600)  # 1 hora

    def start_cleanup_task(loop):
        loop.create_task(cleanup_old_records())
        logging.info("Tarea de limpieza automática programada (cada hora)")

    return {
        'collection': collection,
        'start_cleanup': start_cleanup_task  # ← Función que main llamará
    }