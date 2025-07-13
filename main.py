from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Collaboration Tool API",
    description="A simple collaboration tool API",
    version="0.1.0",
)


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the API is running"""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "message": "API is running successfully",
            "version": "0.1.0",
        },
    )


@app.get("/")
async def root():
    """Root endpoint with basic API information"""
    return {
        "message": "Welcome to Collaboration Tool API",
        "health_check": "/health",
        "docs": "/docs",
    }
