from pydantic import BaseModel
from typing import Dict, List, Any

class TelemetrySnapshot(BaseModel):
    timestamp_ms: int
    active_ws_connections: int
    total_bids_processed: int
    bids_per_second: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    system_load: float
    active_auctions_count: int

class MetricWindow(BaseModel):
    time_label: str
    bids_count: int
    avg_latency_ms: float
