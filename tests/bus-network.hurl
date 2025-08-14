# Bus Network Calculation Test
# Run: hurl --test tests/bus-network.hurl
# Run with repeats: hurl --test --repeat 30 tests/bus-network.hurl

GET http://localhost:8000/calculate-bus-network/PROJ_CDT1021657513
HTTP 200
