from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class AuctionBase(BaseModel):
    title: str = Field(..., json_schema_extra={"example": "1967 Shelby GT500 Super Snake"})
    description: str = Field(..., json_schema_extra={"example": "Experimental 427 CI fastback"})
    category: str = Field(..., json_schema_extra={"example": "Exotic Automobilia"})
    starting_price: float = Field(..., gt=0, json_schema_extra={"example": 120000.0})
    reserve_price: float = Field(..., gt=0, json_schema_extra={"example": 150000.0})
    min_increment: float = Field(default=2500.0, gt=0, json_schema_extra={"example": 2500.0})
    min_escrow_required: float = Field(default=10000.0, ge=0, json_schema_extra={"example": 10000.0})
    soft_close_window_seconds: int = Field(default=15, ge=5)
    extension_seconds: int = Field(default=30, ge=10)
    image_url: Optional[str] = None

class AuctionCreate(AuctionBase):
    id: str = Field(..., json_schema_extra={"example": "auc_shelby_gt500"})
    duration_minutes: int = Field(default=30, ge=1)

class AuctionResponse(AuctionBase):
    id: str
    current_price: float
    highest_bidder_id: Optional[str] = None
    highest_bidder_name: Optional[str] = None
    bid_count: int = 0
    sequence_id: int = 0
    status: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    end_time_ms: Optional[int] = None
    remaining_seconds: Optional[float] = None
    reserve_met: bool = False
    created_at: Optional[datetime] = None

class AuctionSyncRequest(BaseModel):
    auction_id: str
