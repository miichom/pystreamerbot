import asyncio
import logging
import random
from typing import Any

from pystreamerbot import Client

logging.basicConfig(level=logging.DEBUG)

_log = logging.getLogger(__name__)

client = Client(events="Twitch.ChatMessage")


@client.event
async def on_ready() -> None:
    print("Streamer.bot WebSocket (%s) is ready!", client.ws.id)


@client.event
async def on_twitch_chatmessage(data: dict[str, Any]) -> None:
    if random.randint(0, 100) <= 25:  # 25% chance to respond
        user: dict[str, Any] = data.get("user", {})
        _log.debug("Responding to %s...", user.get("name", ""))

        # Implement response here...
        # If you are unsure, try https://github.com/ollama/ollama-python
        message: str = "This is a test message"

        # You must create an action and provide the trigger function
        # with the same name in order to send messages to Speaker.bot.
        await client.trigger("TTS", args={"message": message})


async def main() -> None:
    await client.listen()


asyncio.run(main())
