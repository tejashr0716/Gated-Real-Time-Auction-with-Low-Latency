from pydantic import BaseModel, Field
from typing import Optional

class BidRequest(BaseModel):
    auction_id: str = Field(..., json_schema_extra={"example": "auc_shelby_gt500"})
    bidder_id: str = Field(..., json_schema_extra={"example": "a0000001-0000-0000-0000-000000000001"})
    bidder_name: str = Field(..., json_schema_extra={"example": "apex_collector"})
    bid_amount: float = Field(..., gt=0, json_schema_extra={"example": 147500.0})

class BidResponse(BaseModel):
    ok: bool
    code: str
    message: Optional[str] = None
    auction_id: str
    sequence_id: Optional[int] = None
    bid_amount: Optional[float] = None
    bidder_id: Optional[str] = None
    bidder_name: Optional[str] = None
    timestamp: Optional[int] = None
    end_time_ms: Optional[int] = None
    extended: Optional[bool] = None
    latency_ms: Optional[float] = None

class BidTelemetryEvent(BaseModel):
    event: str = "BID_ACCEPTED"
    auction_id: str
    sequence_id: int
    bid_amount: float
    bidder_id: str
    bidder_name: str
    timestamp: int
    end_time_ms: int
    extended: bool
    bid_count: int
    min_next_bid: float
    reserve_met: bool
