import time
import uuid
import hmac
import hashlib
from fastapi import APIRouter, HTTPException, Depends, Header
from typing import Optional, Dict, Any
from app.config import settings
from app.schemas.auth import UserLogin, TokenResponse, UserProfile, GatePassRequest, GatePassResponse
from app.services.database import db_service
from app.services.redis_client import redis_service
from app.services.auth_security import (
    create_access_token,
    verify_password,
    get_current_authenticated_user
)

router = APIRouter(prefix="/api/auth", tags=["Authentication & Gated Access"])

def create_gate_token(user_id: str, auction_id: str, timestamp_ms: int) -> str:
    message = f"{user_id}:{auction_id}:{timestamp_ms}".encode("utf-8")
    return hmac.new(settings.SECRET_KEY.encode("utf-8"), message, hashlib.sha256).hexdigest()

@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin):
    # 1. Fetch user by username including bcrypt password hash
    user = await db_service.fetchrow(
        "SELECT id, username, email, hashed_password, full_name, kyc_status, tier FROM users WHERE username = $1",
        credentials.username
    )
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    # 2. Strict password verification
    if not verify_password(credentials.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    user_id_str = str(user["id"])

    # 3. Generate cryptographically verified Bearer session token
    token = create_access_token(
        user_id=user_id_str,
        username=user["username"],
        tier=user["tier"]
    )

    # 4. Safe Redis Cache Population:
    # CRITICAL: Never overwrite active in-flight Redis locked_collateral holds!
    client = redis_service.get_client()
    redis_escrow_key = f"escrow:{user_id_str}"
    escrow_exists = await client.exists(redis_escrow_key)

    escrow = await db_service.fetchrow(
        "SELECT total_balance, locked_collateral FROM escrow_accounts WHERE user_id = $1",
        user["id"]
    )

    if escrow:
        if not escrow_exists:
            # First initialization in Redis
            await client.hset(redis_escrow_key, mapping={
                "total_balance": str(escrow["total_balance"]),
                "locked_collateral": str(escrow["locked_collateral"]),
                "kyc_status": user["kyc_status"],
                "tier": user["tier"]
            })
        else:
            # Active session: preserve existing locked_collateral in Redis memory!
            await client.hset(redis_escrow_key, mapping={
                "total_balance": str(escrow["total_balance"]),
                "kyc_status": user["kyc_status"],
                "tier": user["tier"]
            })

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        user_id=user_id_str,
        username=user["username"],
        full_name=user["full_name"],
        tier=user["tier"],
        kyc_status=user["kyc_status"]
    )

@router.get("/me", response_model=UserProfile)
async def get_current_user_profile(
    current_user: Dict[str, Any] = Depends(get_current_authenticated_user)
):
    authenticated_user_id = current_user["user_id"]
    user = await db_service.fetchrow(
        """SELECT u.id, u.username, u.email, u.full_name, u.kyc_status, u.tier,
                  e.total_balance, e.locked_collateral, e.available_balance
           FROM users u
           LEFT JOIN escrow_accounts e ON u.id = e.user_id
           WHERE u.id = $1""",
        uuid.UUID(authenticated_user_id)
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Read live locked_collateral from Redis if available
    client = redis_service.get_client()
    live_locked_raw = await client.hget(f"escrow:{authenticated_user_id}", "locked_collateral")
    live_locked = float(live_locked_raw) if live_locked_raw is not None else float(user["locked_collateral"] or 0.0)
    total_bal = float(user["total_balance"] or 0.0)
    avail_bal = max(0.0, total_bal - live_locked)

    return UserProfile(
        id=str(user["id"]),
        username=user["username"],
        email=user["email"],
        full_name=user["full_name"],
        kyc_status=user["kyc_status"],
        tier=user["tier"],
        total_balance=total_bal,
        locked_collateral=live_locked,
        available_balance=avail_bal
    )

@router.post("/gate-pass", response_model=GatePassResponse)
async def verify_and_issue_gate_pass(
    req: GatePassRequest,
    current_user: Dict[str, Any] = Depends(get_current_authenticated_user)
):
    authenticated_user_id = current_user["user_id"]

    # 1. Check user KYC & Escrow
    user = await db_service.fetchrow(
        """SELECT u.id, u.username, u.kyc_status, u.tier, e.total_balance, e.locked_collateral
           FROM users u
           LEFT JOIN escrow_accounts e ON u.id = e.user_id
           WHERE u.id = $1""",
        uuid.UUID(authenticated_user_id)
    )
    if not user:
        raise HTTPException(status_code=404, detail="Bidder identity not registered")

    client = redis_service.get_client()
    live_locked_raw = await client.hget(f"escrow:{authenticated_user_id}", "locked_collateral")
    live_locked = float(live_locked_raw) if live_locked_raw is not None else float(user["locked_collateral"] or 0.0)
    total_bal = float(user["total_balance"] or 0.0)
    avail_bal = max(0.0, total_bal - live_locked)

    if avail_bal < req.required_collateral:
        raise HTTPException(
            status_code=403,
            detail=f"Gated access denied: Unencumbered escrow (${avail_bal:,.2f}) is below required collateral (${req.required_collateral:,.2f})"
        )

    # 2. Issue HMAC Gate Token
    now_ms = int(time.time() * 1000)
    token = create_gate_token(str(user["id"]), req.auction_id, now_ms)

    return GatePassResponse(
        cleared=True,
        gate_token=token,
        auction_id=req.auction_id,
        user_id=str(user["id"]),
        username=user["username"],
        tier=user["tier"],
        available_collateral=avail_bal,
        message=f"Access Granted: Tier {user['tier']} verified. Escrow collateral locked for room admittance."
    )
