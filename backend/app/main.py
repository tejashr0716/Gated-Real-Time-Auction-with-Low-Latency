import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.services.redis_client import redis_service
from app.services.database import db_service
from app.services.lua_engine import lua_engine
from app.services.websocket_manager import ws_manager
from app.routers import (
    auth_router,
    auctions_router,
    bids_router,
    escrow_router,
    telemetry_router,
    ws_router,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(current_dir, "templates")
templates = Jinja2Templates(directory=templates_dir)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Enforce security validation at application startup before any resources or background tasks are initialized
    settings.validate_security_configuration()

    # Startup sequence
    await redis_service.connect()
    await db_service.connect()
    await lua_engine.register_scripts()
    await ws_manager.start_heartbeat_loop()
    print("=" * 70)
    print("⚡ Gated Real-Time Auction Engine Initialized Successfully!")
    print(f"📡 Telemetry pipeline listening for 500+ clients on ws://localhost:{settings.PORT}/ws/auctions/{{id}}")
    print(f"🔒 Atomic Lua Scripts compiled and cached in Redis.")
    print("=" * 70)
    yield
    # Shutdown sequence
    await ws_manager.stop_heartbeat_loop()
    await redis_service.close()
    await db_service.close()
    print("🛑 Auction Engine shutdown cleanly.")

app = FastAPI(
    title="Gated Real-Time Auction Engine",
    description="Low-latency event-driven telemetry pipeline with Redis Lua atomic validations, WebSockets, and PostgreSQL persistence.",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API Routers
app.include_router(auth_router)
app.include_router(auctions_router)
app.include_router(bids_router)
app.include_router(escrow_router)
app.include_router(telemetry_router)
app.include_router(ws_router)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "app": settings.APP_NAME,
        "engine": "FastAPI + Redis Lua + PostgreSQL",
        "telemetry_clients": ws_manager.total_connections
    }

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/showcase", response_class=HTMLResponse)
async def serve_showcase(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")
