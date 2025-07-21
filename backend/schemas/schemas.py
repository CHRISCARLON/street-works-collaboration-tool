from pydantic import BaseModel, Field
from datetime import datetime


class WellbeingResponse(BaseModel):
    """Response schema for wellbeing impact calculations"""

    success: bool
    project_id: str
    project_duration_days: int
    wellbeing_postcode_count: int = Field(
        ..., description="Number of postcodes within 500m affected by the project"
    )
    wellbeing_total_population: int = Field(
        ..., description="Total population within 500m affected by the project"
    )
    wellbeing_households_affected: int = Field(
        ..., description="Number of households within 500m affected by the project"
    )
    wellbeing_total_impact: float = Field(
        ..., description="Total wellbeing impact in £"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class TransportResponse(BaseModel):
    """Response schema for transport impact calculations"""

    success: bool
    project_id: str
    project_duration_days: int
    transport_stops_affected: int = Field(
        ..., description="Number of bus stops affected by the project"
    )
    transport_operators_count: int = Field(
        ..., description="Number of unique bus operators affected"
    )
    transport_routes_count: int = Field(
        ..., description="Number of unique bus routes affected"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class NetworkResponse(BaseModel):
    """Response schema for network impact calculations"""

    success: bool
    project_id: str
    project_duration_days: int
    traffic_sensitive: bool = Field(
        ..., description="Whether the project is traffic sensitive"
    )
    
    strategic_routes_count: bool = Field(
        ..., description="Whether the project is a strategic route"
    )
    winter_maintenance_routes_count: bool = Field(
        ..., description="Whether the project is a winter maintenance route"
    )
    unique_usrn_count: int = Field(
        0, description="Number of unique street reference numbers (USRNs) affected"
    )
    
    traffic_signals_count: int = Field(
        0, description="Number of traffic signals and pedestrian crossings affected"
    )
    traffic_control_systems: list[str] = Field(
        default_factory=list, description="Types of traffic control systems (UTC, SCOOT, MOVA, etc.)"
    )
    
    total_geometry_length_meters: float = Field(
        0.0, description="Total length of affected road network in meters"
    )
    
    responsible_authorities: list[str] = Field(
        default_factory=list, description="List of responsible authorities for affected features"
    )
    authority_count: int = Field(
        0, description="Number of different authorities responsible for affected features"
    )
    
    designation_types: list[str] = Field(
        default_factory=list, description="Types of special designations affected (Strategic Route, Traffic Sensitive, etc.)"
    )
    operational_states: list[str] = Field(
        default_factory=list, description="Operational states of affected features"
    )
    
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")
