from pydantic import BaseModel, Field
from typing import Optional

class UserLogin(BaseModel):
    username: str = Field(..., json_schema_extra={"example": "apex_collector"})
    password: str = Field(..., json_schema_extra={"example": "password123"})

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    username: str
    full_name: str
    tier: str
    kyc_status: str

class UserProfile(BaseModel):
    id: str
    username: str
    email: str
    full_name: str
    kyc_status: str
    tier: str
    total_balance: float
    locked_collateral: float
    available_balance: float

class GatePassRequest(BaseModel):
    auction_id: str
    required_collateral: float

class GatePassResponse(BaseModel):
    cleared: bool
    gate_token: str
    auction_id: str
    user_id: str
    username: str
    tier: str
    available_collateral: float
    message: str
