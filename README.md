# KNGTOP

Dockerized Polymarket Up/Down runner. Current default live entrypoint is the BTC 5m `S0184` bot.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs `S0184`.

- Side: current BTC winner side only
- Entry gate:
  - first `20s` of the 5m window only
  - winner-side ask in `0.46-0.56`
  - BTC absolute move from window open `>= $1`
- Order:
  - FAK buy
  - per-order size = `$1`
  - max price = displayed ask `+ 0.05`, capped at `0.99`
- Limits:
  - one buy per window
  - no hedge leg
  - no artificial delay in live bot

## Sizing

- current live bot uses a single `$1` taker entry
- `KNGTOP_NOTIONAL_USD=1.0` is the intended deploy setting in `.env`

## Run

```bash
cp .env.example .env
docker compose --env-file .env up --build -d
```

Container command is `python -m kngtop`, which now resolves to the current `S0184` live bot in `kngtop.live_kilemo2`.

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
