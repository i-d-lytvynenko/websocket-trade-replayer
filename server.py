import argparse
import asyncio
import json
import logging
from functools import partial
from typing import Literal, cast

import pandas as pd
import websockets
from pandas.core.groupby.generic import DataFrameGroupBy
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

TradeQueue = asyncio.Queue[tuple[pd.Timestamp, list[str], int] | None]


async def produce_trades(queue: TradeQueue, trades_df: pd.DataFrame) -> None:
    """Processes trades from a DataFrame and puts them into a queue."""
    logging.info("Producer: Starting to process trades.")
    try:
        grouped_trades: DataFrameGroupBy[pd.Timestamp, Literal[True]] = (
            trades_df.groupby("timestamp")
        )  # pyright: ignore[reportAssignmentType]
        for timestamp, group in grouped_trades:
            messages = [
                json.dumps(record, default=str)
                for record in group.to_dict(orient="records")
            ]
            await queue.put((timestamp, messages, len(group)))
    except asyncio.CancelledError:
        logging.info("Producer: Task was cancelled.")
    finally:
        await queue.put(None)
        logging.info("Producer: Finished processing trades.")


async def replay_trades(
    websocket: ServerConnection, trade_file: str, max_queue_size: int
) -> None:
    """Handles a client connection, replaying trade data from a file."""
    logging.info(f"Client connected from {websocket.remote_address}")  # pyright: ignore[reportAny]
    producer_task = None
    try:
        try:
            trades_df = pd.read_parquet(trade_file)
        except FileNotFoundError:
            logging.error(f"Trade data file not found at {trade_file}")
            await websocket.send(json.dumps({"error": f"File not found: {trade_file}"}))
            return
        except Exception as e:
            logging.error(f"Error reading parquet file: {e}", exc_info=True)
            await websocket.send(json.dumps({"error": "Could not read trade data."}))
            return

        trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"])
        trades_df = trades_df.sort_values(by="timestamp").reset_index(drop=True)

        queue: TradeQueue = asyncio.Queue(maxsize=max_queue_size)
        producer_task = asyncio.create_task(produce_trades(queue, trades_df))

        await websocket.send(json.dumps({"status": "Data loaded. Starting replay."}))

        first_item = await queue.get()
        if first_item is None:
            logging.info("No trades to replay.")
            await websocket.send(json.dumps({"status": "Replay finished."}))
            return

        first_timestamp, first_messages, num_trades = first_item
        replay_start_time = asyncio.get_event_loop().time()

        await asyncio.gather(*(websocket.send(m) for m in first_messages))
        logging.info(
            f"Sent {num_trades:4} trades for timestamp {first_timestamp} (initial)"
        )

        latencies = []
        while True:
            item = await queue.get()
            if item is None:
                break

            timestamp, messages, num_trades = item

            time_since_start = (timestamp - first_timestamp).total_seconds()
            target_send_time = replay_start_time + time_since_start

            current_time = asyncio.get_event_loop().time()
            delay = target_send_time - current_time
            if delay > 0:
                await asyncio.sleep(delay)

            actual_send_time = asyncio.get_event_loop().time()
            latencies.append(actual_send_time - target_send_time)

            await asyncio.gather(*(websocket.send(m) for m in messages))

            if delay < 0:
                logging.warning(
                    f"Sent {num_trades:4} trades for timestamp {timestamp} (LAGGING by {-delay:.4f} sec)"
                )
            else:
                logging.info(
                    f"Sent {num_trades:4} trades for timestamp {timestamp} (wait for {delay:.4f} sec)"
                )

        logging.info("Replay finished.")
        await websocket.send(json.dumps({"status": "Replay finished."}))
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            logging.info(f"Average latency: {avg_latency * 1000:.3f} ms")
        else:
            logging.info("No latency measurements available.")

    except websockets.exceptions.ConnectionClosed:
        logging.info(f"Client {websocket.remote_address} disconnected.")  # pyright: ignore[reportAny]
    except Exception as e:
        logging.error(f"An error occurred during replay: {e}", exc_info=True)
        try:
            await websocket.send(
                json.dumps({"error": f"An unexpected error occurred: {str(e)}"})
            )
        except (ConnectionClosedError, ConnectionClosedOK):
            logging.warning(
                f"Client {websocket.remote_address} was already disconnected."  # pyright: ignore[reportAny]
            )
    finally:
        if producer_task:
            producer_task.cancel()
        logging.info(f"Connection handler finished for {websocket.remote_address}")  # pyright: ignore[reportAny]


async def start_server(
    host: str, port: int, trade_file: str, max_queue_size: int
) -> None:
    """Starts the WebSocket server with the given configuration."""
    logging.info(f"Starting WebSocket server on ws://{host}:{port}")
    handler = partial(
        replay_trades, trade_file=trade_file, max_queue_size=max_queue_size
    )
    async with serve(handler, host, port):
        await asyncio.Future()  # Run forever


def main() -> None:
    """Parses arguments and starts the WebSocket server."""
    parser = argparse.ArgumentParser(
        description="Start a WebSocket server to replay trade data."
    )
    parser.add_argument(
        "--host", type=str, default="localhost", help="Host to bind the server to."
    )
    parser.add_argument("--port", type=int, default=8765, help="Port to listen on.")
    parser.add_argument(
        "--trade-file",
        type=str,
        default="trades_sample.parquet",
        help="Path to the Parquet file with trade data.",
    )
    parser.add_argument(
        "--max-queue-size",
        type=int,
        default=100,
        help="Maximum size of the in-memory trade queue.",
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

    try:
        asyncio.run(
            start_server(args.host, args.port, args.trade_file, args.max_queue_size)  # pyright: ignore[reportAny]
        )
    except KeyboardInterrupt:
        logging.info("Server stopped by user.")
    except Exception as e:
        logging.critical(f"Server failed to start: {e}", exc_info=True)


if __name__ == "__main__":
    main()
