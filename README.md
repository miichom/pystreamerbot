# pystreamerbot

[![PyPI - Version](https://img.shields.io/pypi/v/pystreamerbot.svg)](https://pypi.org/project/pystreamerbot)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pystreamerbot.svg)](https://pypi.org/project/pystreamerbot)

Python client for interacting with the [Streamer.bot](https://streamer.bot/) WebSocket API.

## Installation

```bash
pip install -U pystreamerbot
```

## Basic Usage

```python
import asyncio
from typing import Any

from pystreamerbot import Client

client = Client(events="Twitch.ChatMessage")

@client.event
async def on_ready() -> None:
    print(f"Streamer.bot WebSocket ({client.ws.id}) is ready!")

@client.event
async def on_twitch_chatmessage(data: dict[str, Any]) -> None:
    username = data.get("user", {}).get("name", "Unknown")
    print(f"Hello, {username}!")

async def main() -> None:
    await client.listen()

if __name__ == "__main__":
    asyncio.run(main())
```

You can find more examples in the [examples directory](examples/) or alternatively you can look into the [documentation](https://miichom.github.io/pystreamerbot/).

## Attribution

Thank you to:

- [nate1280](https://github.com/nate1280) for creating [Streamer.bot](https://streamer.bot/).
- [Whipstickgostop](https://github.com/whipstickgostop) for creating the official [TypeScript client](https://github.com/Streamerbot/client) for Streamer.bot.
- [Rapptz](https://github.com/Rapptz) & the [discord.py](https://github.com/Rapptz/discord.py) team for creating the architecture and design that inspired this library.

## Contributing

Thanks for your interest in contributing! We welcome contributions of all kinds, including bug fixes, new features, and documentation improvements.

By contributing to this repository, you agree to follow our [Contributing Guidelines](.github/CONTRIBUTING.md) and [Code of Conduct](.github/CODE_OF_CONDUCT.md).

### Getting Started

You can either [fork](https://github.com/miichom/pystreamerbot/fork) or manually clone it by doing the following:

```bash
git clone https://github.com/miichom/pystreamerbot.git
cd pystreamerbot
```

This package uses [Hatch](https://hatch.pypa.io/) for its development, you can install it by running `pip install hatch` in your terminal.

```bash
# Creates a new hatch environment
hatch env create dev

# Avaliable hatch scripts
hatch run dev:lint
hatch run dev:test
hatch run dev:types
```

## License

`pystreamerbot` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
