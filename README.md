# KNGTOP

Dockerized Polymarket Up/Down runner for BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, and LINK across 5m and 15m windows.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs the `cheap-hit + volume OR move` family.

- Trigger: first side whose Polymarket mid is `<= 0.15`
- Side choice: buy the cheaper of `UP` / `DOWN`
- BTC gate: `volume spike OR move`
- Volume gate: current Binance trade-size volume / previous `10s` average `>= 1.4x`, with cheap-side-favor alignment over `5s` or already beyond window open
- Move gate: cheap-side-favor BTC move over `30s >= $1`, or already beyond window open
- Order: `$1` FAK buy capped at `0.25`
- Delay: none in live bot

Only one buy attempt is made per window.

## Sizing

- Fixed `$1` notional per live order for the current BTC `5m` strategy

## Run

```bash
cp .env.example .env
docker compose --env-file .env up --build -d
```

On deploy, the bot waits `60` seconds before entering the trading loop so the feeds can warm up.

## Local

```bash
pip install -r requirements.txt
set PYTHONPATH=.
python -m kngtop
```

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
```
