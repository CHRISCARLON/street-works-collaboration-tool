from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, List, Literal


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


class RoadNetworkResponse(BaseModel):
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


class Section58Data(BaseModel):
    """Individual Section 58 record data"""

    section_58_reference_number: str = Field(
        ..., description="Section 58 reference number"
    )
    usrn: Optional[int] = Field(None, description="Unique Street Reference Number")
    status: Literal["proposed", "in_force", "cancelled", "expired", "closed"] = Field(
        ..., description="Current status of the Section 58"
    )
    start_date: Optional[date] = Field(None, description="Start date of the Section 58")
    end_date: Optional[date] = Field(None, description="End date of the Section 58")
    duration: Optional[
        Literal["six_months", "twelve_months", "two_years", "three_years"]
    ] = Field(None, description="Duration of the Section 58")
    extent: Optional[Literal["whole_road", "part_of_the_road"]] = Field(
        None, description="Extent of road covered"
    )
    location_type: Optional[str] = Field(
        None, description="Type of location (e.g., Carriageway, Footway)"
    )
    status_change_date: Optional[datetime] = Field(
        None, description="Date when status was last changed"
    )
    highway_authority_swa_code: Optional[int] = Field(
        None, description="Highway authority SWA code"
    )
    highway_authority: Optional[str] = Field(None, description="Highway authority name")
    street_name: Optional[str] = Field(None, description="Name of the street")
    area_name: Optional[str] = Field(None, description="Area name")
    town: Optional[str] = Field(None, description="Town name")
    event_type: Optional[str] = Field(
        None, description="Type of event (e.g., SECTION_58_CREATED)"
    )
    event_time: Optional[datetime] = Field(None, description="Time when event occurred")
    valid_from: Optional[datetime] = Field(None, description="Valid from timestamp")
    valid_to: Optional[datetime] = Field(None, description="Valid to timestamp")
    is_current: Optional[bool] = Field(
        None, description="Whether this is the current record"
    )


class Section58Response(BaseModel):
    """Response schema for Section 58 data"""

    success: bool
    project_id: str
    project_duration_days: int
    usrn: Optional[int] = Field(None, description="USRN associated with the project")
    section_58_count: int = Field(0, description="Number of Section 58 records found")
    section_58_records: List[Section58Data] = Field(
        default_factory=list, description="List of Section 58 records"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")


class BdukPremisesData(BaseModel):
    """Individual BDUK premises record data"""

    uprn: Optional[int] = Field(None, description="Unique Property Reference Number")
    struprn: Optional[str] = Field(None, description="String UPRN")
    bduk_recognised_premises: Optional[bool] = Field(
        None, description="Whether premises is BDUK recognised"
    )
    country: Optional[str] = Field(None, description="Country")
    postcode: Optional[str] = Field(None, description="Postcode")
    lot_id: Optional[int] = Field(None, description="Lot ID")
    lot_name: Optional[str] = Field(None, description="Lot name")
    subsidy_control_status: Optional[str] = Field(
        None, description="Subsidy control status"
    )
    current_gigabit: Optional[bool] = Field(
        None, description="Current gigabit availability"
    )
    future_gigabit: Optional[bool] = Field(
        None, description="Future gigabit availability"
    )
    local_authority_district_ons_code: Optional[str] = Field(
        None, description="Local authority district ONS code"
    )
    local_authority_district_ons: Optional[str] = Field(
        None, description="Local authority district ONS name"
    )
    region_ons_code: Optional[str] = Field(None, description="Region ONS code")
    region_ons: Optional[str] = Field(None, description="Region ONS name")
    bduk_gis: Optional[bool] = Field(None, description="BDUK GIS coverage")
    bduk_gis_contract_scope: Optional[str] = Field(
        None, description="BDUK GIS contract scope"
    )
    bduk_gis_final_coverage_date: Optional[str] = Field(
        None, description="BDUK GIS final coverage date"
    )
    bduk_gis_contract_name: Optional[str] = Field(
        None, description="BDUK GIS contract name"
    )
    bduk_gis_supplier: Optional[str] = Field(None, description="BDUK GIS supplier")
    bduk_vouchers: Optional[bool] = Field(
        None, description="BDUK vouchers availability"
    )
    bduk_vouchers_contract_name: Optional[str] = Field(
        None, description="BDUK vouchers contract name"
    )
    bduk_vouchers_supplier: Optional[str] = Field(
        None, description="BDUK vouchers supplier"
    )
    bduk_superfast: Optional[bool] = Field(
        None, description="BDUK superfast availability"
    )
    bduk_superfast_contract_name: Optional[str] = Field(
        None, description="BDUK superfast contract name"
    )
    bduk_superfast_supplier: Optional[str] = Field(
        None, description="BDUK superfast supplier"
    )
    bduk_hubs: Optional[bool] = Field(None, description="BDUK hubs availability")
    bduk_hubs_contract_name: Optional[str] = Field(
        None, description="BDUK hubs contract name"
    )
    bduk_hubs_supplier: Optional[str] = Field(None, description="BDUK hubs supplier")
    usrn: Optional[int] = Field(None, description="Unique Street Reference Number")


class BdukResponse(BaseModel):
    """Response schema for BDUK broadband data"""

    success: bool
    project_id: str
    project_duration_days: int
    usrn: Optional[int] = Field(None, description="USRN associated with the project")
    bduk_premises_count: int = Field(
        0, description="Number of BDUK premises records found"
    )
    bduk_premises_records: List[BdukPremisesData] = Field(
        default_factory=list, description="List of BDUK premises records"
    )
    calculated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="1.0")
