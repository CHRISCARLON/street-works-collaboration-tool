# Root Endpoint Test
# Run: hurl --test tests/root.hurl

GET http://localhost:8000/
HTTP 200
[Asserts]
jsonpath "$.message" == "Welcome to Collaboration Tool API"
jsonpath "$.health_check" == "/health"
jsonpath "$.docs" == "/docs"
jsonpath "$.metrics.bus_network" exists
jsonpath "$.metrics.road_network" exists
jsonpath "$.metrics.asset_network" exists
jsonpath "$.complex_metrics.wellbeing" exists