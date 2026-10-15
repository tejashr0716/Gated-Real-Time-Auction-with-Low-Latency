import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

@pytest.mark.asyncio
async def test_health_check_endpoint(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "FastAPI" in data["engine"]

@pytest.mark.asyncio
async def test_list_auctions_endpoint(async_client):
    response = await async_client.get("/api/auctions")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 4
    first = data[0]
    assert "id" in first
    assert "current_price" in first
    assert "min_increment" in first

@pytest.mark.asyncio
async def test_get_single_auction(async_client):
    response = await async_client.get("/api/auctions/auc_shelby_gt500")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "auc_shelby_gt500"
    assert data["status"] == "ACTIVE"
    assert data["reserve_met"] is not None

@pytest.mark.asyncio
async def test_gate_pass_clearance(async_client):
    response = await async_client.post(
        "/api/auth/gate-pass",
        headers={"X-User-Id": "a0000001-0000-0000-0000-000000000001"},
        json={"auction_id": "auc_shelby_gt500", "required_collateral": 15000.0}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["cleared"] is True
    assert "gate_token" in data
    assert data["tier"] == "DIAMOND"

@pytest.mark.asyncio
async def test_escrow_balance_and_deposit(async_client):
    user_id = "a0000001-0000-0000-0000-000000000001"
    res1 = await async_client.get("/api/escrow/balance", headers={"X-User-Id": user_id})
    assert res1.status_code == 200
    bal1 = res1.json()["total_balance"]

    res2 = await async_client.post("/api/escrow/deposit", headers={"X-User-Id": user_id}, json={"amount": 25000.0})
    assert res2.status_code == 200
    bal2 = res2.json()["total_balance"]
    assert bal2 == bal1 + 25000.0

@pytest.mark.asyncio
async def test_telemetry_metrics_endpoint(async_client):
    response = await async_client.get("/api/telemetry/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "p50_latency_ms" in data
    assert "p99_latency_ms" in data
    assert "bids_per_second" in data
