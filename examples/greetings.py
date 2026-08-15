import asyncio
import logging
import os
import random
from typing import Any

from pystreamerbot import Client

logging.basicConfig(level=logging.DEBUG)

client = Client(
    events="Twitch.ChatMessage",
    password=os.environ.get("password"),  # Required to send messages
)


@client.event
async def on_ready() -> None:
    print("Streamer.bot WebSocket (%s) is ready!", client.ws.id)


@client.event
async def on_twitch_chatmessage(data: dict[str, Any]) -> None:
    if random.randint(0, 100) <= 25:  # 25% chance to respond
        user: dict[str, Any] = data.get("user", {})
        username: str | None = user.get("name")

        # Implement response here...
        # If you are unsure, try https://github.com/ollama/ollama-python
        message: str = f"Hi, {username}!" if username is not None else "Greetings!"

        await client.call("SendMessage", platform="twitch", bot=True, message=message)


async def main() -> None:
    await client.listen()


asyncio.run(main())
