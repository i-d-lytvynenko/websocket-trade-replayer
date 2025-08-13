# Trade Replay WebSocket Server

This project implements a WebSocket server that asynchronously replays historical trade data from a Parquet file. The server reads trades, orders them by timestamp, and sends them to connected clients in a real-time simulation.

## Features

- **Chronological Replay**: Trades are replayed strictly in the order of their timestamps.
- **Real-Time Simulation**: The delay between trades in the original data is maintained during replay to simulate a live feed.
- **Concurrent Messaging**: Trades with the exact same timestamp are bundled and sent to the client simultaneously.
- **Asynchronous Architecture**: Built with Python's `asyncio` library, allowing for efficient handling of WebSocket connections and data processing.
- **JSON Format**: All trade data is sent as JSON objects.
- **Configurable**: Server and client can be configured via command-line arguments.

## Project Structure

- `server.py`: The main WebSocket server application. It reads the trade data, manages the replay logic, and handles client connections.
- `client.py`: An example client that connects to the WebSocket server, receives the trade data, and prints it to the console.
- `trades_sample.parquet`: A sample Parquet file containing trade data for demonstration.
- `pyproject.toml`: Project metadata and dependencies.

## Prerequisites

- Python 3.12+
- Poetry for dependency management

## Data Format

Here's the expected format of your input file:

| timestamp               | price      | volume          | ticker                       |
|-------------------------|------------|-----------------|------------------------------|
| 2022-07-01 00:00:29.338 | 6035000    | 1000000000000   | 10000NFT-USDT-SWAP@BYBIT     |
| 2022-07-01 00:01:11.737 | 6010000    | 180000000000    | 10000NFT-USDT-SWAP@BYBIT     |
| 2022-07-01 00:01:32.209 | 6010000    | 90000000000     | 10000NFT-USDT-SWAP@BYBIT     |
| 2022-07-01 00:01:58.920 | 6020000    | 30000000000     | 10000NFT-USDT-SWAP@BYBIT     |
| 2022-07-01 00:02:34.601 | 6010000    | 100000000000    | 10000NFT-USDT-SWAP@BYBIT     |
| ...                     | ...        | ...             | ...                          |
