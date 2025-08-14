# Error Cases Test
# Run: hurl --test tests/error-cases.hurl

# Test non-existent project ID
GET http://localhost:8000/calculate-wellbeing/PROJ_NONEXISTENT
HTTP 500

# Test invalid project ID format
GET http://localhost:8000/calculate-bus-network/INVALID_ID
HTTP 400

# Test another invalid endpoint
GET http://localhost:8000/calculate-road-network/BADID123
HTTP 400

# Test asset network with invalid project
GET http://localhost:8000/calculate-asset-network/NOTREAL
HTTP 400
