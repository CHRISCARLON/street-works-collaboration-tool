# Collaboration Tool

A FastAPI tool that calculates impact metrics for streetworks and roadworks projects.

## What It Does

Analyses how streetworks projects affect local communities and calculates impact scores.

## Current Features

### Wellbeing Impact Calculator

- Calculates economic impact on residents within 500m of a project
- Formula: `£1.61 × Project Days × Affected Households`
- Uses UK postcode and population data

### Transport Impact Calculator

- Finds affected bus stops and routes
- Counts operators and services impacted

### Network Impact Calculator

- Analyses affected road infrastructure
- Identifies traffic signals, strategic routes, and road classifications

## API Endpoints

- `GET /health` - Health check
- `GET /calculate-wellbeing/{project_id}` - Calculate wellbeing impact
- `GET /calculate-transport/{project_id}` - Calculate transport impact
- `GET /calculate-network/{project_id}` - Calculate network impact

## Example

```bash
curl http://localhost:8000/calculate-transport/PROJ_CDT440003968937
```

Output:

```json
{
  "success": true,
  "project_id": "PROJ_CDT440003968937",
  "project_duration_days": 30,
  "transport_stops_affected": 3,
  "transport_operators_count": 1,
  "transport_routes_count": 12,
  "calculated_at": "2025-07-21T23:06:03.721424",
  "version": "1.0"
}
```

## Data Sources

- PostgreSQL: Project data
- MotherDuck: UK postcode, population, and transport data
- OS NGD API: Road network and infrastructure data
