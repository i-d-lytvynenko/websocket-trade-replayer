import argparse
import asyncio
import json
import logging
from typing import cast

import websockets
from websockets.asyncio.client import ClientConnection, connect


async def listen_to_trades(uri: str, show_first_n: int, summary_interval: int) -> None:
    """Connects to a WebSocket server and listens for trade messages."""
    async with connect(uri) as websocket:
        websocket: ClientConnection
        logging.info(f"Connected to {uri}")
        message_count: int = 0
        try:
            while True:
                message = await websocket.recv()
                message_count += 1
                data = json.loads(message)  # pyright: ignore[reportAny]
                if isinstance(data, dict) and "status" in data:
                    logging.info(f"Server status: {data['status']}")
                    if data["status"] == "Replay finished.":
                        break
                else:
                    if message_count <= show_first_n:
                        logging.info(f"Received trade: {data}")
                    elif message_count % summary_interval == 0:
                        logging.info(f"Received {message_count} trades so far.")

        except websockets.exceptions.ConnectionClosed:
            logging.info("Connection to server closed.")
        except asyncio.CancelledError:
            logging.info("Client task cancelled.")
        finally:
            logging.info(f"Total trades received: {message_count}")


def main() -> None:
    """Parses command-line arguments and runs the WebSocket client."""
    parser = argparse.ArgumentParser(
        description="Connect to a WebSocket server to receive trade data."
    )
    parser.add_argument(
        "--host", type=str, default="localhost", help="WebSocket server host."
    )
    parser.add_argument("--port", type=int, default=8765, help="WebSocket server port.")
    parser.add_argument(
        "--show-first-n",
        type=int,
        default=10,
        help="Log the first N trades in detail.",
    )
    parser.add_argument(
        "--summary-interval",
        type=int,
        default=100,
        help="Log a summary every N trades.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )
    args = parser.parse_args()

    level = cast(str, args.log_level)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    uri = f"ws://{args.host}:{args.port}"  # pyright: ignore[reportAny]
    try:
        asyncio.run(listen_to_trades(uri, args.show_first_n, args.summary_interval))  # pyright: ignore[reportAny]
    except KeyboardInterrupt:
        logging.info("Client stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()
