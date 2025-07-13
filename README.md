# Collab Tool 2.0

A FastAPI-based collaboration tool for calculating comprehensive impact metrics for streetworks/roadworks projects.

## Overview

This tool analyses streetworks/roadworks projects and calculates multi-dimensional impact scores on communities, businesses, and the environment.

## Impact Metrics

### Currently Implemented

- ✅ **Resident Wellbeing Index** (£): Economic impact on local communities
  - Formula: `£1.61 × Days × Households_Affected`
  - Uses UK postcode data with population and household statistics
  - Analyses 500m radius around a project's locations and finds the total population and households affected

### Planned Implementation

- **Days of Disruption Index**
- **Road User Impact Index**
- **Business Loss Index**
- **Carbon Emissions Index**
- **Public Transport Impact Index**
- **Historic Wellbeing Index**
- **Asset Strike Risk Index**

### Current Core Functionality

- **Wellbeing Impact Calculation**: Calculates the economic impact of streetworks/roadworks projects on local communities

### Data Sources

- **PostgreSQL Database**: Utility asset/project data
- **MotherDuck (DuckDB Cloud)**: Ordnance Survey Open Code Point and ONS UK postcode data (P001 & P002) with population and household statistics

### API Endpoints

- `GET /health` - Health check endpoint
- `GET /` - API information and available endpoints
- `GET /calculate-wellbeing/{project_id}` - Calculate wellbeing impact for a specific project

## Example Usage

```bash
# Calculate wellbeing impact for a project
curl http://localhost:8000/calculate-wellbeing/PROJ_CDT440003968937
```

**Current Response:**

```json
{
  "success": true,
  "project_id": "PROJ_CDT626864553",
  "impact_score": {
    "impact_score_id": "4f5286ec-c792-4d87-bfc8-a33a3a17f948",
    "project_id": "PROJ_CDT626864553",
    "project_duration_days": 20,
    "wellbeing_postcode_count": 215,
    "wellbeing_total_population": 7797,
    "wellbeing_households_affected": 2477,
    "wellbeing_total_impact": 79759.40000000001,
    "is_valid": true,
    "created_at": "2025-07-13T18:29:41.857235",
    "updated_at": "2025-07-13T18:29:41.857207",
    "version": "1.0"
  }
```

### Project Impact Summary

**Project:** PROJ_CDT626864553  
**Duration:** 20 days of work

**Area Affected:**

- 215 postcodes within 500 meters of the project site
- 7,797 people living in the affected area
- 2,477 households directly impacted by the disruption

**Economic Impact:**

- £79,759.40 total wellbeing cost to the community

This breaks down to:

- £1.61 per day per household (the standard rate)
- × 20 days of disruption
- × 2,477 affected households
- = £79,759.40 total economic impact

**What This Means:**
This 20-day project will cause £79,759 worth of wellbeing disruption to nearly 8,000 local residents across 215 postcodes.

**Context:**
This calculation uses an established methodology that quantifies the daily inconvenience, stress, noise, dust, and general disruption that residents experience during streetworks - putting a monetary value on the community impact that's often overlooked in project planning.

The £1.61 daily rate per household is based on economic research into how much residents would theoretically pay to avoid the disruption, or conversely, how much they should be compensated for enduring it.
