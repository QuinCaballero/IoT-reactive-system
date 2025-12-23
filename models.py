import time
from pydantic import BaseModel, ValidationError, Field, validator
from typing import Optional

class SensorData(BaseModel):
    temperatura: float                    # Mandatory
    humedad: Optional[float] = None       # Optional
    sensor_id: str                        # Mandatory
    timestamp: Optional[float] = None      # Optional (From the server if needed)

    @validator('timestamp', always=True)  # Always execute
    def set_timestamp(cls, v):
        return v or time.time()  # If missing add now Unix timestamp
    
    class Config:
        extra = "forbid"  # Reject unknown fields
        allow_population_by_field_name = True