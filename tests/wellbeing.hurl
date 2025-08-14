# Wellbeing Calculation Test
# Run: hurl --test tests/wellbeing.hurl
# Run with repeats: hurl --test --repeat 30 tests/wellbeing.hurl

GET http://localhost:8000/calculate-wellbeing/PROJ_CDT1021657513
HTTP 200