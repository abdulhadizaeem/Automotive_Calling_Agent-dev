from .router import router
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from urllib.request import Request
from datetime import datetime

def create_app():
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(
        title="Auth",
        description="Assist the user using the Knowledgebase",
        version="0.1.0",
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.max_request_size = 200 * 1024 * 1024

    # Set up CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Specify the correct frontend origin here
        allow_credentials=True,
        allow_methods=["*"],  # Allows all methods
        allow_headers=["*"],  # Allows all headers
    )

    app.include_router(router, tags=["Auth"], prefix="/api")

    # Route Handlers
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    
    @app.exception_handler(HTTPException)
    async def custom_http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}  # ✅ frontend ke format mein
    )

    return app