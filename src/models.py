from pydantic import BaseModel, Field, ConfigDict
from typing import Optional

class AirLocationRecord(BaseModel):
    # Apply the modern Pydantic configuration dictionary style
    model_config = ConfigDict(populate_by_name=True)

    location: str
    city: str
    country: str
    
'''    magnitude: float = Field(alias="mag")
    time_epoch: int = Field(alias="time")
    tsunami: int
    felt_reports: Optional[int] = Field(default=0, alias="felt")'''

class AirQualityRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    location_id: int
    parameter: str
    value: float
    unit: str
    last_updated: Optional[str] = Field(default=None, alias="lastUpdated")