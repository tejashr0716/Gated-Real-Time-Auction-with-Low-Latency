from fastapi import APIRouter
from app.services.telemetry_metrics import telemetry_metrics
from app.services.database import db_service

router = APIRouter(prefix="/api/telemetry", tags=["Telemetry & Observability"])

@router.get("/metrics")
async def get_realtime_telemetry():
    # Fetch active auctions count
    count_row = await db_service.fetchrow("SELECT COUNT(*) as c FROM auctions WHERE status = 'ACTIVE'")
    active_count = count_row["c"] if count_row else 4
    return telemetry_metrics.get_snapshot(active_auctions_count=active_count)

@router.get("/history")
async def get_telemetry_history():
    rows = await db_service.fetch(
        """SELECT active_ws_clients, bids_per_second, p50_latency_ms, p95_latency_ms, p99_latency_ms, recorded_at
           FROM telemetry_metrics
           ORDER BY recorded_at DESC
           LIMIT 30"""
    )
    return [
        {
            "active_ws_clients": r["active_ws_clients"],
            "bids_per_second": float(r["bids_per_second"]),
            "p50_latency_ms": float(r["p50_latency_ms"]),
            "p95_latency_ms": float(r["p95_latency_ms"]),
            "p99_latency_ms": float(r["p99_latency_ms"]),
            "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None
        }
        for r in rows
    ]
