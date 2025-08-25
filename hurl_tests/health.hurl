# Health Check Test
# Run: hurl --test tests/health.hurl

GET http://localhost:8000/health
HTTP 200
