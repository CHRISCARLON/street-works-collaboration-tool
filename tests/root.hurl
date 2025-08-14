# Root Endpoint Test
# Run: hurl --test tests/root.hurl

GET http://localhost:8000/
HTTP 200
[Asserts]
jsonpath "$.message" == "Welcome to Collaboration Tool API"
jsonpath "$.health_check" == "/health"
jsonpath "$.docs" == "/docs"
jsonpath "$.calculate_wellbeing" exists
jsonpath "$.calculate_bus_network" exists
jsonpath "$.calculate_road_network" exists
jsonpath "$.calculate_asset_network" exists