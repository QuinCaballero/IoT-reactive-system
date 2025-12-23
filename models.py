import time
from pydantic import BaseModel, ValidationError, Field, field_validator
from typing import Optional

class SensorData(BaseModel):
    temperatura: float                    # Mandatory
    humedad: Optional[float] = None       # Optional
    sensor_id: str                        # Mandatory
    timestamp: Optional[float] = None      # Optional (From the server if needed)

    model_config = {
        "extra": "forbid",
        "populate_by_name": True,  # Permite alias
    }

    @field_validator('timestamp', mode='before')
    @classmethod
    def set_timestamp(cls, v):
        return v if v is not None else time.time()