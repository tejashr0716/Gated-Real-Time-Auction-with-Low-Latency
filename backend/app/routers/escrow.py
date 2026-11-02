import uuid
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, List, Dict, Any
from app.services.database import db_service
from app.services.redis_client import redis_service
from app.services.auth_security import get_current_authenticated_user

router = APIRouter(prefix="/api/escrow", tags=["Escrow & Collateral"])

class DepositRequest(BaseModel):
    amount: float = Field(..., gt=0, example=50000.0)

@router.get("/balance")
async def get_escrow_balance(
    current_user: Dict[str, Any] = Depends(get_current_authenticated_user)
):
    authenticated_user_id = current_user["user_id"]
    account = await db_service.fetchrow(
        """SELECT e.id, e.user_id, e.total_balance, e.locked_collateral, e.available_balance, e.currency,
                  u.username, u.tier, u.kyc_status
           FROM escrow_accounts e
           JOIN users u ON e.user_id = u.id
           WHERE e.user_id = $1""",
        uuid.UUID(authenticated_user_id)
    )
    if not account:
        raise HTTPException(status_code=404, detail="Escrow account not found")

    client = redis_service.get_client()
    live_locked_raw = await client.hget(f"escrow:{authenticated_user_id}", "locked_collateral")
    live_locked = float(live_locked_raw) if live_locked_raw is not None else float(account["locked_collateral"])
    total_bal = float(account["total_balance"])
    avail_bal = max(0.0, total_bal - live_locked)

    return {
        "id": str(account["id"]),
        "user_id": str(account["user_id"]),
        "username": account["username"],
        "tier": account["tier"],
        "kyc_status": account["kyc_status"],
        "total_balance": total_bal,
        "locked_collateral": live_locked,
        "available_balance": avail_bal,
        "currency": account["currency"]
    }

@router.post("/deposit")
async def deposit_escrow(
    req: DepositRequest,
    current_user: Dict[str, Any] = Depends(get_current_authenticated_user)
):
    authenticated_user_id = current_user["user_id"]
    
    # 1. Update PostgreSQL in a single transaction
    if not db_service.pool:
        row = await db_service.fetchrow(
            """UPDATE escrow_accounts
               SET total_balance = total_balance + $1, updated_at = NOW()
               WHERE user_id = $2
               RETURNING id, total_balance, locked_collateral, available_balance""",
            req.amount, uuid.UUID(authenticated_user_id)
        )
        if not row:
            raise HTTPException(status_code=404, detail="Escrow account not found")
        idempotency = f"DEP-{uuid.uuid4().hex[:12].upper()}"
        await db_service.execute(
            """INSERT INTO escrow_transactions (escrow_id, transaction_type, amount, idempotency_key)
               VALUES ($1, 'DEPOSIT', $2, $3)""",
            row["id"], req.amount, idempotency
        )
    else:
        async with db_service.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """UPDATE escrow_accounts
                       SET total_balance = total_balance + $1, updated_at = NOW()
                       WHERE user_id = $2
                       RETURNING id, total_balance, locked_collateral, available_balance""",
                    req.amount, uuid.UUID(authenticated_user_id)
                )
                if not row:
                    raise HTTPException(status_code=404, detail="Escrow account not found")
                idempotency = f"DEP-{uuid.uuid4().hex[:12].upper()}"
                await conn.execute(
                    """INSERT INTO escrow_transactions (escrow_id, transaction_type, amount, idempotency_key)
                       VALUES ($1, 'DEPOSIT', $2, $3)""",
                    row["id"], req.amount, idempotency
                )

    # 2. Update Redis memory, carefully preserving existing locked_collateral
    client = redis_service.get_client()
    live_locked_raw = await client.hget(f"escrow:{authenticated_user_id}", "locked_collateral")
    live_locked = float(live_locked_raw) if live_locked_raw is not None else float(row["locked_collateral"])
    new_total = float(row["total_balance"])
    avail_bal = max(0.0, new_total - live_locked)

    await client.hset(f"escrow:{authenticated_user_id}", mapping={
        "total_balance": str(new_total)
    })

    return {
        "ok": True,
        "message": f"Successfully deposited ${req.amount:,.2f} into escrow vault.",
        "total_balance": new_total,
        "locked_collateral": live_locked,
        "available_balance": avail_bal,
        "idempotency_key": idempotency
    }

@router.get("/transactions")
async def get_escrow_transactions(
    current_user: Dict[str, Any] = Depends(get_current_authenticated_user)
):
    authenticated_user_id = current_user["user_id"]
    rows = await db_service.fetch(
        """SELECT t.id, t.transaction_type, t.amount, t.idempotency_key, t.created_at
           FROM escrow_transactions t
           JOIN escrow_accounts e ON t.escrow_id = e.id
           WHERE e.user_id = $1
           ORDER BY t.created_at DESC
           LIMIT 20""",
        uuid.UUID(authenticated_user_id)
    )
    return [
        {
            "id": str(r["id"]),
            "type": r["transaction_type"],
            "amount": float(r["amount"]),
            "idempotency_key": r["idempotency_key"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None
        }
        for r in rows
    ]
