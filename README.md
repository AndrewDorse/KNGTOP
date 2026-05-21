# KNGTOP

Dockerized Polymarket Up/Down runner for BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, and LINK across 5m and 15m windows.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs the `KILEMO_2` `H2725` hedge family.

- Seed side: current BTC winner side only
- Seed gate:
  - winner-side Polymarket ask in `0.35-0.50`
  - BTC move over `10s >= $2` in the winner-side direction
- Seed order:
  - FAK buy
  - per-order size = `KNGTOP_NOTIONAL_USD` default `$1`
- Hedge flow:
  - after a seed fill, track `PnL if UP wins` and `PnL if DOWN wins`
  - hedge the weaker outcome side only
  - hedge side ask must be `<= 0.35`
  - size is recalculated from the current deficit, with a live cap of `2x` the base size per hedge buy
  - if the beneficial hedge size is below the exchange minimum, it is rounded up to the minimum instead of being skipped
- Limits:
  - `KNGTOP_HEDGE_MAX_ORDERS_PER_SIDE` default `5`
  - total window budget cap `$30`
  - no artificial delay in live bot

## Sizing

- `KNGTOP_NOTIONAL_USD` default `$1` per live order
- `KNGTOP_HEDGE_MAX_ORDERS_PER_SIDE` default `5`

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
