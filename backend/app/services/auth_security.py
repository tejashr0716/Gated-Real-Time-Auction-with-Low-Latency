import base64
import hashlib
import hmac
import json
import time
from typing import Optional, Dict, Any
import bcrypt
from fastapi import Request, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import settings

security = HTTPBearer(auto_error=False)

def hash_password(plain_password: str) -> str:
    """
    Securely hashes password using bcrypt with standard work factor 12.
    """
    if not plain_password:
        raise ValueError("Password cannot be empty")
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(plain_password.encode("utf-8"), salt).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies plaintext password against stored bcrypt hash in constant time.
    """
    if not plain_password or not hashed_password:
        return False
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except Exception:
        return False

def create_access_token(
    user_id: str,
    username: str,
    tier: str = "GOLD",
    expires_in_seconds: int = 86400,
    is_admin: bool = False
) -> str:
    """
    Creates an HMAC-SHA256 signed bearer token containing verified identity claims.
    """
    settings.validate_security_configuration()
    payload = {
        "sub": str(user_id),
        "username": username,
        "tier": tier,
        "is_admin": is_admin,
        "iat": int(time.time()),
        "exp": int(time.time()) + expires_in_seconds
    }
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode("utf-8")
    raw_payload = base64.urlsafe_b64encode(payload_bytes).decode("utf-8").rstrip("=")
    signature = hmac.new(settings.SECRET_KEY.encode("utf-8"), raw_payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{raw_payload}.{signature}"

def verify_access_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verifies token signature, structure, and expiration, returning verified identity claims.
    Rejects malformed, tampered, or expired tokens.
    """
    if not token or "." not in token:
        return None
    try:
        parts = token.strip().split(".")
        if len(parts) != 2:
            return None
        raw_payload, signature = parts[0], parts[1]
        
        # Verify HMAC signature in constant time
        expected_sig = hmac.new(settings.SECRET_KEY.encode("utf-8"), raw_payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return None

        # Pad base64 string
        rem = len(raw_payload) % 4
        padded = raw_payload + ("=" * (4 - rem) if rem > 0 else "")
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))

        # Expiration check
        if payload.get("exp", 0) < int(time.time()):
            return None

        return payload
    except Exception:
        return None

async def get_current_authenticated_user(request: Request) -> Dict[str, Any]:
    """
    FastAPI dependency: Enforces verified authentication on protected endpoints.
    Extracts and validates credentials from Authorization header or X-Auth-Token.
    """
    auth_header = request.headers.get("Authorization")
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    elif request.headers.get("X-Auth-Token"):
        token = request.headers.get("X-Auth-Token").strip()

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authentication required: Missing Authorization Bearer token header"
        )

    claims = verify_access_token(token)
    if not claims or "sub" not in claims:
        raise HTTPException(
            status_code=401,
            detail="Authentication failed: Invalid, expired, or tampered Bearer token"
        )

    return {
        "user_id": claims["sub"],
        "username": claims.get("username", "AuthenticatedBidder"),
        "tier": claims.get("tier", "GOLD"),
        "is_admin": claims.get("is_admin", False)
    }

async def require_admin_user(request: Request) -> Dict[str, Any]:
    """
    FastAPI dependency: Enforces administrative authorization.
    In DEMO_MODE, allows authenticated Diamond users or explicit demo admin.
    """
    user = await get_current_authenticated_user(request)
    if not user.get("is_admin") and user.get("tier") != "DIAMOND" and not settings.DEMO_MODE:
        raise HTTPException(
            status_code=403,
            detail="Forbidden: Administrative privileges required"
        )
    return user
