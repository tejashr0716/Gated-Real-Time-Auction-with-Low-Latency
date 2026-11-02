"""
Regression tests for Auction Dashboard client count display.
Verifies that:
1. The dashboard HTML no longer contains hardcoded '500+' client connection claims.
2. The server-reported active connection count correctly increments on connection and decrements on disconnection.
3. Handshake and telemetry ticks accurately convey per-process connection metrics.
4. Browser DOM client count update logic accurately handles single, plural, zero, and disconnected states.
"""

import os
import json
import pytest
from unittest.mock import AsyncMock
from app.services.websocket_manager import WebSocketManager
from app.services.telemetry_metrics import telemetry_metrics
from app.services.redis_client import redis_service

TEMPLATE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "app", "templates", "index.html")
)

def test_dashboard_html_contains_no_hardcoded_500_plus_clients():
    """1. Confirm the dashboard no longer presents a hardcoded 500+ count as current connections."""
    assert os.path.exists(TEMPLATE_PATH), f"Template not found at {TEMPLATE_PATH}"
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    # Must NOT claim 500+ connected clients
    assert "500+ Clients Connected" not in html, "Hardcoded '500+ Clients Connected' still present in template"
    assert "500+" not in html, "Any hardcoded '500+' claim found in dashboard template"

    # Must contain telemetry-clients-count element with per-process documentation
    assert 'id="telemetry-clients-count"' in html, "Missing telemetry-clients-count DOM element"
    assert "Process" in html, "Connection count should explicitly document whether it is per-process"

@pytest.mark.asyncio
async def test_server_connection_and_disconnection_updates():
    """2. Verify server-reported active connection count accurately tracks connections and disconnections."""
    manager = WebSocketManager()
    auc_id = "auc_shelby_gt500"

    client = redis_service.get_client()
    await client.hset(f"auction:{auc_id}:meta", mapping={
        "status": "ACTIVE",
        "current_price": "145000.00",
        "sequence_id": "18"
    })

    ws1 = AsyncMock()
    ws1.accept = AsyncMock()
    ws1.send_text = AsyncMock()

    ws2 = AsyncMock()
    ws2.accept = AsyncMock()
    ws2.send_text = AsyncMock()

    initial_conns = manager.total_connections

    # Connect client 1
    await manager.connect(auc_id, ws1)
    assert manager.total_connections == initial_conns + 1
    assert telemetry_metrics.active_ws_connections == manager.total_connections

    # Verify handshake payload contains accurate active connection count
    assert ws1.send_text.await_count >= 1
    handshake = json.loads(ws1.send_text.call_args_list[0][0][0])
    assert handshake["event"] == "ROOM_HANDSHAKE"
    assert handshake["active_ws_connections"] == manager.total_connections
    assert handshake["active_room_clients"] == 1

    # Connect client 2
    await manager.connect(auc_id, ws2)
    assert manager.total_connections == initial_conns + 2
    assert telemetry_metrics.active_ws_connections == manager.total_connections

    # Disconnect client 1
    await manager.disconnect(auc_id, ws1)
    assert manager.total_connections == initial_conns + 1
    assert telemetry_metrics.active_ws_connections == manager.total_connections

    # Disconnect client 2
    await manager.disconnect(auc_id, ws2)
    assert manager.total_connections == initial_conns
    assert telemetry_metrics.active_ws_connections == initial_conns

@pytest.mark.asyncio
async def test_telemetry_metrics_snapshot_reports_real_count():
    """3. Verify telemetry snapshot accurately exposes per-process active WebSocket connection count."""
    telemetry_metrics.set_ws_connections(42)
    snapshot = telemetry_metrics.get_snapshot(active_auctions_count=1)
    assert snapshot["active_ws_connections"] == 42
    # Clean up
    telemetry_metrics.set_ws_connections(0)

@pytest.mark.asyncio
async def test_browser_dashboard_updates_client_count_via_playwright():
    """4. Browser headless test verifying UI client count updates on connection and disconnection."""
    from playwright.async_api import async_playwright

    chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if not os.path.exists(chrome_path):
        pytest.skip("Google Chrome binary not installed")

    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=chrome_path, headless=True)
        page = await browser.new_page()
        await page.goto(f"file://{TEMPLATE_PATH}")

        # Check initial text
        initial_text = await page.inner_text("#telemetry-clients-count")
        assert "500+" not in initial_text
        assert "Process" in initial_text

        # Test single connection update (1 Client)
        await page.evaluate("() => updateActiveClientsCount(1)")
        t1 = await page.inner_text("#telemetry-clients-count")
        assert t1 == "1 Client Connected (Process)"

        # Test plural connection update (5 Clients)
        await page.evaluate("() => updateActiveClientsCount(5)")
        t5 = await page.inner_text("#telemetry-clients-count")
        assert t5 == "5 Clients Connected (Process)"

        # Test zero connection update (0 Clients)
        await page.evaluate("() => updateActiveClientsCount(0)")
        t0 = await page.inner_text("#telemetry-clients-count")
        assert t0 == "0 Clients Connected (Process)"

        # Test disconnect fallback
        await page.evaluate("() => updateActiveClientsCount(null)")
        td = await page.inner_text("#telemetry-clients-count")
        assert td == "Disconnected (0 Clients)"

        await browser.close()
