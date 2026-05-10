# KNGTOP

Dockerized Polymarket Up/Down runner for BTC, ETH, XRP, and SOL across 5m and 15m windows, with one `$1` FAK market buy per slug per window.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Per window, rules are evaluated in list order. The first rule that fires gets the single trade.

| Order | Key | Logic |
|------|-----|--------|
| 1 | `cheap_buy_up` | `mid_up <= 0.30` and `binance_spot > window_open` -> buy `DOWN` |
| 2 | `cheap_buy_down` | `mid_dn <= 0.30` and `binance_spot < window_open` -> buy `UP` |

This logic is used for every configured asset and for both 5m and 15m contracts.

## Run

```bash
cp .env.example .env
docker compose --env-file .env up --build -d
```

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
