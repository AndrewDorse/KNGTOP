# KNGTOP

Dockerized Polymarket Up/Down runner for BTC, ETH, XRP, SOL, DOGE, BNB, HYPE, and LINK across 5m, 15m, 1h, and 4h windows.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Per window, rules are evaluated in list order. The first rule that fires gets the single trade.

| Order | Key | Logic |
|------|-----|--------|
| 1 | `cheap_buy_up` | `mid_up <= 0.15` and `binance_spot > window_open` -> buy `UP` |
| 2 | `cheap_buy_down` | `mid_dn <= 0.15` and `binance_spot < window_open` -> buy `DOWN` |

This logic is used for every configured asset and for 5m, 15m, 1h, and 4h contracts.

## Sizing

- BTC, ETH, XRP, SOL on 5m and 15m: `max($1, 10% of available balance)` computed at window start
- DOGE, BNB, HYPE, LINK on 5m and 15m: `max($1, 5% of available balance)` computed at window start
- All assets on 1h and 4h: fixed `$1`

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
