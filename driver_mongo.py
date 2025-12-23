import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient

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

    # Opcional: devolvemos la colección por si queremos leer más adelante
    return {'collection': collection}