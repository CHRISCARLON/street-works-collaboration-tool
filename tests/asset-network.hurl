# Asset Network Calculation Test
# Run: hurl --test tests/asset-network.hurl
# Run with repeats: hurl --test --repeat 30 tests/asset-network.hurl

# Test with default zoom level
GET http://localhost:8000/calculate-asset-network/PROJ_CDT1021657513
HTTP 200

# Test with custom zoom level
GET http://localhost:8000/calculate-asset-network/PROJ_CDT1021657513?zoom_level=12
HTTP 200
