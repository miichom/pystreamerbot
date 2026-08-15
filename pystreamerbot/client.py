from __future__ import annotations

import asyncio
import inspect
import json
import logging
import sys
import traceback
from collections import defaultdict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, TypedDict, TypeVar, overload
from uuid import uuid4

from websockets import CloseCode
from websockets.asyncio.client import ClientConnection, connect

if TYPE_CHECKING:
    from typing_extensions import NotRequired, Self, Unpack

    class _ClientOptions(TypedDict, total=False):
        schema: NotRequired[Literal["ws", "wss"]]
        host: NotRequired[str]
        port: NotRequired[int]
        endpoint: NotRequired[str]
        password: str | None
        retries: NotRequired[int]
        backoff: NotRequired[float]


__all__ = ["Client"]

T = TypeVar("T")
Coro = Coroutine[Any, Any, T]
CoroT = TypeVar("CoroT", bound=Callable[..., Coro[Any]])

_log = logging.getLogger(__name__)


@dataclass(order=True)
class _Handler:
    sort_priority: float = field(init=False)
    weight: float
    coro: Callable[..., Coro[Any]] = field(compare=False)
    name: str = field(compare=False)

    def __post_init__(self) -> None:
        self.sort_priority = -self.weight


class Client:
    """Represents a client connection that connects to Streamer.bot.

    This class handles the WebSocket lifecycle, event dispatching,
    and JSON-RPC communication with Streamer.bot.

    Args:
        events (str | list[str]): Event pattern or list of event patterns to
            subscribe to (e.g. `"Twitch.*"`).
        **options (Unpack[_ClientOptions]): Connection configuration options.
            schema (Literal["ws", "wss"]): Connection scheme. Defaults to `"ws"`.
            host (str): Gateway host address. Defaults to `"127.0.0.1"`.
            port (int): Gateway port number. Defaults to `8080`.
            endpoint (str): Endpoint path. Defaults to `"/"`.
            password (str | None): Authentication password if configured in Streamer.bot.
            retries (int): Maximum connection attempt count. Defaults to `5`.
            backoff (float): Exponential retry delay in seconds. Defaults to `1.0`.

    Attributes:
        loop (asyncio.AbstractEventLoop): The active asyncio event loop assigned to the client.
        ws (ClientConnection): The underlying WebSocket connection instance.

    Example:
    ```python
    import asyncio
    import pystreamerbot

    client = pystreamerbot.Client(events=["Twitch.*"])

    @client.event
    async def on_ready():
        print("Client is ready!")
    ```
    """

    def __init__(self, *, events: str | list[str], **options: Unpack[_ClientOptions]) -> None:
        self.loop: asyncio.AbstractEventLoop = None  # type: ignore
        # self.ws gets assigned in self.connnect
        self.ws: ClientConnection = None  # type: ignore

        self._events = self._normalize_events(events)
        self._handlers: dict[str, list[_Handler]] = defaultdict(list)
        self._listeners: dict[str, list[tuple[asyncio.Future[Any], Callable[..., bool]]]] = (
            defaultdict(list)
        )
        self._pending: dict[str, asyncio.Future[Any]] = {}

        self._closed = asyncio.Event()
        self._options = options

    @overload
    def event(self, coro: CoroT, /) -> CoroT: ...

    @overload
    def event(
        self, *, name: str | None = None, weight: float = 0.0
    ) -> Callable[[CoroT], CoroT]: ...

    def event(self, coro: CoroT | None = None, name: str | None = None, weight: float = 0.0) -> Any:
        """Decorator to register a coroutine function as an event listener.

        Args:
            coro (CoroT | None): Target coroutine function to register.
            name (str | None): Explicit event name override (e.g. `"twitch_cheer"`).
            weight (float): Execution priority weight. Higher values run first. Defaults to `0.0`.

        Returns:
            Any: The original coroutine function or a decorator wrapper.

        Raises:
            TypeError: If the decorated object is not a coroutine function.
        """

        def decorator(fn: CoroT) -> CoroT:
            if not inspect.iscoroutinefunction(fn):
                raise TypeError("An event listener must be a coroutine function.")

            event_name = name or fn.__name__
            if not event_name.startswith("on_"):
                event_name = f"on_{event_name.lower()}"

            handler = _Handler(weight=weight, coro=fn, name=event_name)
            self._handlers[event_name].append(handler)
            self._handlers[event_name].sort()

            if name is None:
                setattr(self, fn.__name__, fn)

            _log.debug(
                "Registered event handler %r for %s with weight=%.1f",
                fn.__name__,
                event_name,
                weight,
            )
            return fn

        if coro is not None:
            return decorator(coro)

        return decorator

    def dispatch(self, event_name: str, *args: Any, **kwargs: Any) -> None:
        """Dispatches an event to all registered listeners.

        Args:
            event_name (str): Name of the event to dispatch (e.g. `"ready"` or `"twitch_cheer"`).
            *args (Any): Positional arguments passed to event listeners.
            **kwargs (Any): Keyword arguments passed to event listeners.
        """
        method_name = (
            event_name.lower() if event_name.startswith("on_") else f"on_{event_name.lower()}"
        )

        _log.debug("Dispatching event %s", method_name)

        handlers = self._handlers.get(method_name, [])
        for handler in handlers:
            self._schedule_event(handler.coro, method_name, *args, **kwargs)

        subclass_handler = getattr(self, method_name, None)
        if subclass_handler is not None and not any(h.coro == subclass_handler for h in handlers):
            self._schedule_event(subclass_handler, method_name, *args, **kwargs)

    def _schedule_event(
        self, coro: Callable[..., Coro[Any]], event_name: str, *args: Any, **kwargs: Any
    ) -> asyncio.Task[Any]:
        wrapped = self._run_event(coro, event_name, *args, **kwargs)
        return self.loop.create_task(wrapped, name=f"pystreamerbot:{event_name}")

    async def _run_event(
        self, coro: Callable[..., Coro[Any]], event_name: str, *args: Any, **kwargs: Any
    ) -> None:
        try:
            await coro(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            await self.on_error(event_name, exc, *args, **kwargs)

    async def on_error(self, method: str, error: Exception, *args: Any, **kwargs: Any) -> None:
        """Default error handler called when an exception occurs inside an event listener.

        Args:
            method (str): The name of the event method where the error occurred.
            error (Exception): The exception instance raised.
            *args (Any): Positional arguments passed to the event listener.
            **kwargs (Any): Keyword arguments passed to the event listener.
        """
        _log.error("Ignoring exception in %s", method, exc_info=error)
        traceback.print_exc(file=sys.stderr)

    def run(self) -> None:
        """Synchronously starts the event loop and blocks until the client disconnects."""

        async def runner() -> None:
            async with self:
                await self.listen()

        try:
            asyncio.run(runner())
        except KeyboardInterrupt:
            pass

    async def listen(self) -> None:
        """Starts the connection flow asynchronously and blocks until closed."""
        self.loop = asyncio.get_running_loop()
        await self.connect()
        await self._closed.wait()

    async def connect(self) -> None:
        """Establishes the WebSocket connection to Streamer.bot.

        Raises:
            ConnectionError: If connection attempts exceed the configured retry limit.
        """
        uri = self._build_uri()
        retries = self._options.get("retries", 5)
        backoff = self._options.get("backoff", 1.0)

        for attempt in range(retries):
            try:
                self.ws = await connect(uri, ping_interval=20, ping_timeout=10, max_size=None)
                await self._handshake()

                self.loop.create_task(self._raw_listen(), name="pystreamerbot:ws_listen")

                response = await self.call("Subscribe", events=self._events)
                _log.debug("Subscription acknowledged: %s", response)

                self.dispatch("ready")
                return

            except Exception as exc:  # noqa: BLE001
                _log.error("Failed connection attempt (%s)", exc)
                self.dispatch("error", exc)
                if attempt < retries - 1:
                    _log.debug(
                        "Connection attempt %d/%d to %s failed (%s). Retrying in %.2fs...",
                        attempt + 1,
                        retries,
                        uri,
                        exc,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2

        raise ConnectionError(
            f"Failed to connect to Streamer.bot at '{uri}' after {retries} attempts."
        )

    async def close(
        self, code: CloseCode | int = CloseCode.NORMAL_CLOSURE, reason: str = ""
    ) -> None:
        """Closes the WebSocket connection and cleans up pending futures.

        Args:
            code (CloseCode | int): The status code to close with. Defaults to
                `CloseCode.NORMAL_CLOSURE`.
            reason (str): Optional close reason message string. Defaults to `""`.
        """
        if self._closed.is_set():
            return

        self._closed.set()
        if self.ws:
            await self.ws.close(code, reason)

        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

        self.dispatch("close", code, reason)

    def is_closed(self) -> bool:
        return self._closed.is_set()

    async def __aenter__(self) -> Self:
        self.loop = asyncio.get_running_loop()
        self._closed.clear()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self.is_closed():
            await self.close()

    async def wait_for(
        self,
        event: str,
        *,
        check: Callable[..., bool] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Waits for a single event frame matching an optional check predicate.

        Args:
            event (str): Name of the event to wait for (e.g. `"twitch_cheer"`).
            check (Callable[..., bool] | None): Predicate callable returning True when event
                data matches. Defaults to None.
            timeout (float | None): Maximum allowed duration in seconds before timing out.
                Defaults to None.

        Returns:
            Any: The payload data passed to the matched event.

        Raises:
            asyncio.TimeoutError: If a matching event is not received within `timeout`.
        """
        key = f"on_{event.lower()}"
        chk = check or (lambda *args, **kwargs: True)

        future = self.loop.create_future()
        self._listeners[key].append((future, chk))

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise
        finally:
            self._listeners[key] = [
                (fut, c) for fut, c in self._listeners[key] if fut is not future
            ]

    def _process_listeners(self, event: str, *args: Any) -> None:
        listeners = self._listeners.get(f"on_{event.lower()}")
        if not listeners:
            return

        to_remove = []
        for idx, (fut, check) in enumerate(listeners):
            if fut.cancelled():
                to_remove.append(idx)
                continue

            try:
                if check(*args):
                    fut.set_result(args[0] if len(args) == 1 else args)
                    to_remove.append(idx)
            except Exception as exc:  # noqa: BLE001
                fut.set_exception(exc)
                to_remove.append(idx)

        for idx in reversed(to_remove):
            del listeners[idx]

    async def call(self, request: str, **kwargs: Any) -> dict[str, Any]:
        """Executes a JSON-RPC request to Streamer.bot and awaits the response.

        Args:
            request (str): Command method identifier (e.g. `"GetActions"`).
            **kwargs (Any): Extra parameters included in the request payload.

        Returns:
            dict[str, Any]: The raw response dictionary returned from Streamer.bot.

        Raises:
            RuntimeError: If called before establishing a WebSocket connection.
        """
        if not self.ws:
            raise RuntimeError("A WebSocket connection has not been established yet.")

        rid = self._generate_id()
        fut: asyncio.Future[dict[str, Any]] = self.loop.create_future()
        self._pending[rid] = fut

        payload = {"id": rid, "request": request, **kwargs}
        await self.ws.send(json.dumps(payload))
        return await fut

    async def trigger(self, action_name: str, **kwargs: Any) -> dict[str, Any]:
        """Convenience method to trigger a Streamer.bot action by name.

        Args:
            action_name (str): Name of the action inside Streamer.bot.
            **kwargs (Any): Parameters passed into the action execution context.

        Returns:
            dict[str, Any]: The raw response dictionary returned from Streamer.bot.
        """
        return await self.call("DoAction", action={"name": action_name}, **kwargs)

    async def _raw_listen(self) -> None:
        if not self.ws:
            return

        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                await self._exec(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.dispatch("error", exc)
        finally:
            await self.close(CloseCode.INTERNAL_ERROR)

    async def _exec(self, msg: dict[str, Any]) -> None:
        rid = msg.get("id")

        _log.debug("Received message ID %r | Pending IDs: %r", rid, list(self._pending.keys()))

        if rid is not None and rid in self._pending:
            fut = self._pending.pop(rid)
            if not fut.done():
                fut.set_result(msg)
            return

        event = msg.get("event")
        method = None

        if isinstance(event, dict):
            source = event.get("source", "")
            event_type = event.get("type", "").replace(".", "_")
            method = f"{source}_{event_type}".strip("_").lower()

        elif isinstance(event, str):
            method = event.replace(".", "_").lower()

        if method is not None:
            data = msg.get("data", {})
            self._process_listeners(method, data)
            self.dispatch(method, data)
            return

        self.dispatch("response", msg)

    async def _handshake(self) -> None:
        if not self.ws:
            return

        raw = await self.ws.recv()
        msg = json.loads(raw)
        if msg.get("request") == "Hello" and self._options.get("password"):
            await self._authenticate(msg)

    async def _authenticate(self, msg: dict[str, Any]) -> None:
        if not self.ws:
            return

        from base64 import b64encode
        from hashlib import sha256

        auth = msg.get("authentication", {})
        password = self._options.get("password", "")

        secret = b64encode(sha256((password + auth["salt"]).encode()).digest()).decode()
        authentication = b64encode(sha256((secret + auth["challenge"]).encode()).digest()).decode()

        await self.ws.send(
            json.dumps(
                {
                    "id": self._generate_id("auth"),
                    "request": "Authenticate",
                    "authentication": authentication,
                }
            )
        )

    def _build_uri(self) -> str:
        schema = self._options.get("schema", "ws")
        host = self._options.get("host", "127.0.0.1")
        port = self._options.get("port", 8080)
        endpoint = self._options.get("endpoint", "/").lstrip("/")
        return f"{schema}://{host}:{port}/{endpoint}"

    @staticmethod
    def _generate_id(prefix: str = "req") -> str:
        timestamp = datetime.now(timezone.utc).timestamp()
        return f"sb:client:{prefix}:{timestamp}-{uuid4().hex}"

    @staticmethod
    def _normalize_events(events: str | list[str]) -> dict[str, list[str]]:
        if isinstance(events, str):
            events = [events]

        if "*" in events:
            return {"*": ["*"]}

        result: dict[str, list[str]] = {}
        for pattern in events:
            if pattern.endswith(".*"):
                result[pattern[:-2]] = ["*"]
            elif "." in pattern:
                source, event_type = pattern.split(".", 1)
                result.setdefault(source, []).append(event_type)
            else:
                result[pattern] = ["*"]

        return result
