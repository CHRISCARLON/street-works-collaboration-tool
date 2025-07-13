from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import date, datetime


class Project(BaseModel):
    """Simple data model for project derived from asset data"""

    # Project Identification - derived from asset
    project_id: str = Field(
        ..., description="Unique project identifier (derived from asset_id)"
    )
    programme_id: Optional[str] = Field(
        None,
        description="Unique alphanumeric identifier for an individual programme of works",
    )
    source: str = Field(..., description="Data provider name")
    swa_code: str = Field(..., description="SWA code of the promoter")
    contact: Optional[str] = Field(None, description="Project contact name")
    department: Optional[str] = Field(None, description="Project contact department")
    tele: Optional[str] = Field(None, description="Project contact number")
    email: Optional[str] = Field(None, description="Project contact email")

    # Project Details - derived from asset
    title: str = Field(..., description="Name/title of the project")
    scheme: str = Field(..., description="Detailed description of the project")
    simple_theme: str = Field(..., description="The primary theme of a project")
    multi_theme: Optional[str] = Field(
        None,
        description="If a project contains multiple themes of work, additional themes can be added",
    )
    comments: Optional[str] = Field(
        None, description="Free text string to add any additional information"
    )

    # Location Information - from asset geo data
    geo_point: str = Field(
        ...,
        description="Lat, Long coordinates as string (e.g., '53.496572840571446, -2.2205016817669714')",
    )
    geo_shape: Optional[Dict[str, Any]] = Field(
        None, description="GeoJSON geometry object"
    )
    easting: float = Field(..., description="British National Grid easting coordinate")
    northing: float = Field(
        ..., description="British National Grid northing coordinate"
    )
    usrn: Optional[int] = Field(None, description="Unique street reference number")
    post_code: Optional[str] = Field(None, description="Postcode of project/programme")
    site_area: Optional[float] = Field(
        None, description="Project site area in hectares"
    )
    location_meta: Optional[str] = Field(
        None, description="How accurate the location of a project is known"
    )

    # Asset Information - from original asset data
    asset_type: str = Field(
        ..., description="Type of asset (Main Pipe, Service Pipe, etc.)"
    )
    pressure: Optional[str] = Field(None, description="Pressure rating (LP, MP, HP)")
    material: Optional[str] = Field(None, description="Material type (PE, Steel, etc.)")
    diameter: Optional[float] = Field(None, description="Diameter value")
    diam_unit: Optional[str] = Field(None, description="Diameter unit (MM, INCH)")
    carr_mat: Optional[str] = Field(None, description="Carrier material")
    carr_dia: Optional[float] = Field(None, description="Carrier diameter")
    carr_di_un: Optional[str] = Field(None, description="Carrier diameter unit")
    asset_id: str = Field(..., description="Original asset identifier")
    depth: Optional[float] = Field(None, description="Depth of asset")
    ag_ind: bool = Field(False, description="Above ground indicator")
    inst_date: Optional[date] = Field(None, description="Installation date")
    length: Optional[float] = Field(None, description="Length of asset")
    length_unit: Optional[str] = Field(None, description="Length unit (M, KM)")

    # Timing and Scheduling
    start_date: Optional[date] = Field(None, description="Date project is due to start")
    start_date_yy: int = Field(..., description="Year the project is due to start")
    start_date_meta: Optional[str] = Field(
        None, description="Description of how certain the start date is"
    )
    completion_date: Optional[date] = Field(
        None, description="Date project is due to complete"
    )
    completion_date_yy: int = Field(
        ..., description="Year the project is due to complete"
    )
    completion_date_meta: Optional[str] = Field(
        None, description="Description of how certain the complete date is"
    )
    dates_yy_range: Optional[str] = Field(
        None, description="The range of years the project will be in delivery"
    )
    flexibility: Optional[int] = Field(
        None,
        description="A measure of how flexible a project can be in terms of delivery time",
    )

    # Financial Information
    programme_value: Optional[float] = Field(
        None, description="Cost of the overall programme of work in GBP"
    )
    programme_range: Optional[str] = Field(
        None, description="The cost range that the overall programme of work falls into"
    )
    programme_value_meta: Optional[str] = Field(
        None, description="Description of how certain the cost value is"
    )
    project_value: Optional[float] = Field(
        None, description="Cost of the project in GBP"
    )
    project_range: Optional[str] = Field(
        None, description="The cost range that the project falls into"
    )
    project_value_meta: Optional[str] = Field(
        None, description="Description of how certain the cost value is"
    )

    # Status and Governance
    funding_status: str = Field(..., description="The funding status of a project")
    planning_status: Optional[str] = Field(
        None, description="The status of the project within the planning system"
    )
    collaboration: bool = Field(
        True,
        description="Indicate 'no' if a project is unsuitable for collaborative work with other providers",
    )
    restrictions: Optional[str] = Field(
        None,
        description="Text string to indicate any restrictions on the data provided",
    )

    # Additional Information
    more_url: Optional[str] = Field(
        None, description="URL to further programme details"
    )
    provider_db_date: date = Field(
        ...,
        description="Date the data was extracted from the providers database/system",
    )

    # System Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(None)
    processing_status: str = Field(default="pending")

    @property
    def duration_days(self) -> Optional[int]:
        """Calculate project duration in days if both dates available"""
        if self.start_date and self.completion_date:
            return (self.completion_date - self.start_date).days + 1
        return None

    @property
    def latitude(self) -> Optional[float]:
        """Extract latitude from geo_point string"""
        try:
            lat_str, _ = self.geo_point.split(", ")
            return float(lat_str)
        except (ValueError, AttributeError):
            return None

    @property
    def longitude(self) -> Optional[float]:
        """Extract longitude from geo_point string"""
        try:
            _, lon_str = self.geo_point.split(", ")
            return float(lon_str)
        except (ValueError, AttributeError):
            return None


class ImpactScore(BaseModel):
    """Impact score schema to store calculated metrics for each project"""

    # Primary key and reference
    impact_score_id: str = Field(
        ..., description="Unique identifier for the impact score record"
    )
    project_id: str = Field(
        ..., description="Reference to the project this impact score belongs to"
    )

    # Project duration
    project_duration_days: int = Field(
        ..., description="Duration of the project in days"
    )

    # Wellbeing metrics
    wellbeing_postcode_count: Optional[int] = Field(
        None, description="Number of postcodes within 500m affected by the project"
    )
    wellbeing_total_population: Optional[int] = Field(
        None, description="Total population within 500m affected by the project"
    )

    wellbeing_households_affected: Optional[int] = Field(
        None, description="Number of households within 500m affected by the project"
    )

    wellbeing_total_impact: Optional[float] = Field(
        None, description="Total wellbeing impact in £"
    )

    # Status
    is_valid: bool = Field(
        True, description="Whether the impact score is valid/current"
    )

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(None)
    version: str = Field(
        default="1.0", description="Version of the impact score calculation"
    )
