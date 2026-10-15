import uuid
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Header
from typing import Optional, List
from app.services.database import db_service
from app.services.redis_client import redis_service

router = APIRouter(prefix="/api/escrow", tags=["Escrow & Collateral"])

class DepositRequest(BaseModel):
    amount: float = Field(..., gt=0, example=50000.0)

@router.get("/balance")
async def get_escrow_balance(user_id: Optional[str] = Header(None, alias="X-User-Id")):
    effective_user_id = user_id or "a0000001-0000-0000-0000-000000000001"
    account = await db_service.fetchrow(
        """SELECT e.id, e.user_id, e.total_balance, e.locked_collateral, e.available_balance, e.currency,
                  u.username, u.tier, u.kyc_status
           FROM escrow_accounts e
           JOIN users u ON e.user_id = u.id
           WHERE e.user_id = $1""",
        uuid.UUID(effective_user_id)
    )
    if not account:
        raise HTTPException(status_code=404, detail="Escrow account not found")

    return {
        "id": str(account["id"]),
        "user_id": str(account["user_id"]),
        "username": account["username"],
        "tier": account["tier"],
        "kyc_status": account["kyc_status"],
        "total_balance": float(account["total_balance"]),
        "locked_collateral": float(account["locked_collateral"]),
        "available_balance": float(account["available_balance"]),
        "currency": account["currency"]
    }

@router.post("/deposit")
async def deposit_escrow(req: DepositRequest, user_id: Optional[str] = Header(None, alias="X-User-Id")):
    effective_user_id = user_id or "a0000001-0000-0000-0000-000000000001"
    
    # Update PostgreSQL
    row = await db_service.fetchrow(
        """UPDATE escrow_accounts
           SET total_balance = total_balance + $1, updated_at = NOW()
           WHERE user_id = $2
           RETURNING id, total_balance, locked_collateral, available_balance""",
        req.amount, uuid.UUID(effective_user_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Escrow account not found")

    # Add audit log
    idempotency = f"DEP-{uuid.uuid4().hex[:12].upper()}"
    await db_service.execute(
        """INSERT INTO escrow_transactions (escrow_id, transaction_type, amount, idempotency_key)
           VALUES ($1, 'DEPOSIT', $2, $3)""",
        row["id"], req.amount, idempotency
    )

    # Mirror to Redis
    client = redis_service.get_client()
    await client.hset(f"escrow:{effective_user_id}", mapping={
        "total_balance": str(row["total_balance"]),
        "locked_collateral": str(row["locked_collateral"])
    })

    return {
        "ok": True,
        "message": f"Successfully deposited ${req.amount:,.2f} into escrow vault.",
        "total_balance": float(row["total_balance"]),
        "locked_collateral": float(row["locked_collateral"]),
        "available_balance": float(row["available_balance"]),
        "idempotency_key": idempotency
    }

@router.get("/transactions")
async def get_escrow_transactions(user_id: Optional[str] = Header(None, alias="X-User-Id")):
    effective_user_id = user_id or "a0000001-0000-0000-0000-000000000001"
    rows = await db_service.fetch(
        """SELECT t.id, t.transaction_type, t.amount, t.idempotency_key, t.created_at
           FROM escrow_transactions t
           JOIN escrow_accounts e ON t.escrow_id = e.id
           WHERE e.user_id = $1
           ORDER BY t.created_at DESC
           LIMIT 20""",
        uuid.UUID(effective_user_id)
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
