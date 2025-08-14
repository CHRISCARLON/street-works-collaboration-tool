# Asset Network Calculation Test
# Run: hurl --test tests/asset-network.hurl
# Run with repeats: hurl --test --repeat 30 tests/asset-network.hurl

# Test with default zoom level
GET http://localhost:8000/calculate-asset-network/PROJ_6F0E754C8F57
HTTP 200

# Test with custom zoom level
GET http://localhost:8000/calculate-asset-network/PROJ_6F0E754C8F57?zoom_level=12
HTTP 200
