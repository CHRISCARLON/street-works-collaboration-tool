from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, List


class HouseholdsResponse(BaseModel):
    """Response schema for household demographics data"""

    success: bool
    project_id: str
    project_duration_days: int
    postcode_count: int = Field(
        ..., description="Number of postcodes within 250m of the project"
    )
    total_population: int = Field(
        ..., description="Total population within 250m of the project"
    )
    total_households: int = Field(
        ..., description="Number of households within 250m of the project"
    )
    female_population: int = Field(
        ..., description="Female population within 250m of the project"
    )
    male_population: int = Field(
        ..., description="Male population within 250m of the project"
    )
    postcodes: List[str] = Field(
        ..., description="List of postcodes within 250m of the project"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


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
    transport_services_count: int = Field(
        ..., description="Number of unique bus services affected"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class BusNetworkResponse(BaseModel):
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
        default_factory=list,
        description="Types of traffic control systems (UTC, SCOOT, MOVA, etc.)",
    )

    total_geometry_length_meters: float = Field(
        0.0, description="Total length of affected road network in meters"
    )

    responsible_authorities: list[str] = Field(
        default_factory=list,
        description="List of responsible authorities for affected features",
    )
    authority_count: int = Field(
        0,
        description="Number of different authorities responsible for affected features",
    )

    designation_types: list[str] = Field(
        default_factory=list,
        description="Types of special designations affected (Strategic Route, Traffic Sensitive, etc.)",
    )
    operational_states: list[str] = Field(
        default_factory=list, description="Operational states of affected features"
    )

    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class AssetResponse(BaseModel):
    """Response schema for asset impact calculations"""

    success: bool
    project_id: str
    project_duration_days: int
    usrn: int
    asset_count: int = Field(
        ..., description="Number of assets affected by the project"
    )
    bbox: str = Field(
        ...,
        description="Bounding box of the USRN geometry in format 'minx,miny,maxx,maxy'",
    )
    hex_grid_count: int = Field(
        ..., description="Number of hex grids intersecting with USRN"
    )
    asset_density: float = Field(
        ..., description="Asset density per intersecting hex grid"
    )
    clipping_applied: bool = Field(
        ..., description="Whether USRN geometry clipping was successfully applied"
    )
    intersecting_hex_grids: list[dict] = Field(
        default_factory=list,
        description="List of intersecting hex grids with their details",
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class ProjectCreate(BaseModel):
    """Schema for creating a new project via form data"""

    # Required fields
    programme_id: str = Field(description="Programme identifier")
    swa_code: str = Field(description="SWA code")
    usrn: int = Field(description="Unique Street Reference Number")
    activity_type: str = Field(
        description="Type of activity (e.g., new_installation, repair, upgrade)"
    )
    programme_type: str = Field(
        description="Programme type (e.g., capital_investment, routine_maintenance)"
    )
    location_type: str = Field(
        description="Location type (footway, carriageway, verge, mix)"
    )
    sector_type: str = Field(
        description="Sector type (water, telco, gas, electricity, highway)"
    )
    ttro_required: str = Field(description="TTRO required (yes/no)")
    installation_method: str = Field(
        description="Installation method (e.g., open_cut, directional_drilling)"
    )

    # Optional fields
    source: Optional[str] = None
    contact: Optional[str] = None
    department: Optional[str] = None
    tele: Optional[str] = None
    email: Optional[str] = None

    title: Optional[str] = None
    scheme: Optional[str] = None
    simple_theme: Optional[str] = None
    multi_theme: Optional[str] = None
    comments: Optional[str] = None

    geo_point: Optional[str] = Field(
        None, description="Point coordinates as string 'lat,lon'"
    )
    geometry_coordinates: Optional[List[float]] = Field(
        None, description="Point coordinates as [longitude, latitude]"
    )
    geo_shape_coordinates: Optional[List[List[float]]] = Field(
        None, description="Line coordinates as [[lon,lat], [lon,lat], ...]"
    )
    post_code: Optional[str] = None
    site_area: Optional[float] = None
    location_meta: Optional[str] = None

    asset_type: Optional[str] = None
    pressure: Optional[str] = None
    material: Optional[str] = None
    diameter: Optional[float] = None
    diam_unit: Optional[str] = None
    carr_mat: Optional[str] = None
    carr_dia: Optional[float] = None
    carr_di_un: Optional[str] = None
    asset_id: Optional[str] = None
    depth: Optional[float] = None
    ag_ind: Optional[bool] = False
    inst_date: Optional[date] = None
    length: Optional[float] = None
    length_unit: Optional[str] = None

    start_date: Optional[date] = None
    start_date_yy: Optional[int] = None
    start_date_meta: Optional[str] = None
    completion_date: Optional[date] = None
    completion_date_yy: Optional[int] = None
    completion_date_meta: Optional[str] = None
    dates_yy_range: Optional[str] = None
    flexibility: Optional[int] = None

    programme_value: Optional[float] = None
    programme_range: Optional[str] = None
    programme_value_meta: Optional[str] = None
    project_value: Optional[float] = None
    project_range: Optional[str] = None
    project_value_meta: Optional[str] = None

    funding_status: Optional[str] = None
    planning_status: Optional[str] = None
    collaboration: Optional[bool] = True
    restrictions: Optional[str] = None


class ProjectResponse(BaseModel):
    """Response schema for project creation"""

    success: bool
    project_id: str
    message: str
    created_at: datetime = Field(default_factory=datetime.now)


class WorkHistoryResponse(BaseModel):
    """Response schema for work history impact calculations"""

    success: bool
    project_id: str
    project_duration_days: int
    works_count: int = Field(
        ..., description="Total count of completed works in the last 6 months"
    )
    works_by_promoter: dict[str, int] = Field(
        default_factory=dict,
        description="Breakdown of completed works count by promoter organisation",
    )
