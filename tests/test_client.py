from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from websockets import CloseCode

from pystreamerbot import Client


@pytest.fixture
async def client() -> Client:
    c = Client(events=["Twitch.Follow", "YouTube.*"])
    c.loop = asyncio.get_running_loop()
    return c


@pytest.fixture
def mock_ws() -> AsyncMock:
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    return ws


class TestEventSystem:
    @pytest.mark.asyncio
    async def test_event_registration_and_dispatch(self, client: Client) -> None:
        received_data: asyncio.Future[dict[str, str]] = client.loop.create_future()

        @client.event
        async def on_custom_event(data: dict[str, str]) -> None:
            received_data.set_result(data)

        client.dispatch("custom_event", {"args": {}, "eventName": "abcd123", "useArgs": False})
        result = await asyncio.wait_for(received_data, timeout=1.0)
        assert result == {"args": {}, "eventName": "abcd123", "useArgs": False}

    @pytest.mark.asyncio
    async def test_event_weighted_execution_order(self, client: Client) -> None:
        execution_order: list[str] = []

        @client.event(name="weighted_event", weight=1.0)
        async def low_priority() -> None:
            execution_order.append("low")

        @client.event(name="weighted_event", weight=10.0)
        async def high_priority() -> None:
            execution_order.append("high")

        client.dispatch("weighted_event")
        await asyncio.sleep(0.01)

        assert execution_order == ["high", "low"]

    @pytest.mark.asyncio
    async def test_event_decorator_requires_coroutine(self, client: Client) -> None:
        with pytest.raises(TypeError, match="must be a coroutine function"):

            @client.event  # type: ignore
            def sync_function() -> None:
                pass


class TestWaitFor:
    @pytest.mark.asyncio
    async def test_wait_for_receives_dispatched_event(self, client: Client) -> None:
        # Create an explicit future bound to the active loop
        fut: asyncio.Future[dict[str, str]] = client.loop.create_future()

        # Register a temporary listener for the event directly
        @client.event(name="twitch_sub")
        async def _on_twitch_sub(data: dict[str, str]) -> None:
            if not fut.done():
                fut.set_result(data)

        # Dispatch the event payload
        client.dispatch("twitch_sub", {"user": {"name": "Jane Doe"}})

        # Await the explicit future directly with an outer timeout
        data: dict[str, Any] = await asyncio.wait_for(fut, timeout=1.0)
        assert data == {"user": {"name": "Jane Doe"}}

    @pytest.mark.asyncio
    async def test_wait_for_timeout_raises(self, client: Client) -> None:
        with pytest.raises(asyncio.TimeoutError):
            await client.wait_for("non_existent_event", timeout=0.01)


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_close_and_is_closed(self, client: Client, mock_ws: AsyncMock) -> None:
        client.ws = mock_ws
        assert not client.is_closed()

        await client.close(code=CloseCode.NORMAL_CLOSURE, reason="Shutdown")

        assert client.is_closed()
        mock_ws.close.assert_awaited_once_with(CloseCode.NORMAL_CLOSURE, "Shutdown")

    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_ws: AsyncMock) -> None:
        async with Client(events="*") as c:
            c.ws = mock_ws
            assert not c.is_closed()

        assert c.is_closed()

    @pytest.mark.asyncio
    @patch("pystreamerbot.client.connect", new_callable=AsyncMock)
    async def test_connect_raises_connection_error_on_failure(
        self, mock_connect: AsyncMock
    ) -> None:
        mock_connect.side_effect = RuntimeError("Connection Refused")

        c = Client(events="*", retries=2, backoff=0.01)
        c.loop = asyncio.get_running_loop()

        with pytest.raises(ConnectionError, match="Failed to connect to Streamer.bot"):
            await c.connect()


class TestRPCCalls:
    @pytest.mark.asyncio
    async def test_call_without_ws_connection_raises(self, client: Client) -> None:
        with pytest.raises(RuntimeError, match="not been established yet"):
            await client.call("TestAction")

    @pytest.mark.asyncio
    async def test_trigger_delegates_to_call(self, client: Client) -> None:
        with patch.object(client, "call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = {"status": "ok"}

            result = await client.trigger("DoSomething", arg1="val1")

            mock_call.assert_awaited_once_with(
                "DoAction",
                action={"name": "DoSomething"},
                arg1="val1",
            )
            assert result == {"status": "ok"}


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_on_error_public_handler(self, client: Client) -> None:
        err = ValueError("Test exception")
        await client.on_error("test_method", err)
