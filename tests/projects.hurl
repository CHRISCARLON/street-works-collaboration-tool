# Test Projects API endpoints
# Asset Network Calculation Test
# Run: hurl --test tests/projects.hurl

# Test health check
GET http://localhost:8000/projects/health
HTTP 200

# Test creating a Cadent Gas project
POST http://localhost:8000/projects/create
Content-Type: application/json
{
    "source": "Cadent Gas",
    "swa_code": "SWA_CDT_001",
    "title": "Main Pipe Asset Maintenance - London",
    "scheme": "Maintenance work on main gas pipe asset CDT1021657515",
    "simple_theme": "Gas",
    "geo_point": "51.53788595115742, -0.09974588865375973",
    "geometry_coordinates": [-0.09974588865375973, 51.53788595115742],
    "geo_shape_coordinates": [
        [-0.100267922164017, 51.53743438161744],
        [-0.100252857275593, 51.53744987456288],
        [-0.100274004594968, 51.53746145767693],
        [-0.099951908088231, 51.53770803172655],
        [-0.099617503110198, 51.53799037559009],
        [-0.099497916349845, 51.53809185224498],
        [-0.099210176066717, 51.538335608676924],
        [-0.09919506371428, 51.538352224784134]
    ],
    "usrn": 21604817,
    "asset_type": "Main Pipe",
    "pressure": "LP",
    "material": "PE",
    "diameter": 125.0,
    "diam_unit": "MM",
    "asset_id": "CDT1021657515",
    "ag_ind": false,
    "inst_date": "1982-01-07",
    "length": 100.0,
    "length_unit": "M",
    "start_date": "2026-04-01",
    "start_date_yy": 2026,
    "completion_date": "2026-04-30",
    "completion_date_yy": 2026,
    "funding_status": "Confirmed",
    "planning_status": "Confirmed",
    "collaboration": true
}
HTTP 200
