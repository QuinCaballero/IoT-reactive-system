import time
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

class SensorData(BaseModel):
    temperatura: float                    # Mandatory
    humedad: Optional[float] = None       # Optional
    sensor_id: str = Field(..., alias="sensor")                   # Mandatory
    timestamp: float = Field(default_factory=time.time)

    model_config = {
        "extra": "forbid",
        "populate_by_name": True,  # Permite alias
    }