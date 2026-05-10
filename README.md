# KNGTOP

Dockerized Polymarket **BTC Up/Down** runner: **5m** and **15m** windows **at the same time**, **one** `$1` **FAK market buy** ( [`py_clob_client_v2`](https://github.com/Polymarket/py-clob-client) ) **per slug per window**.

- **Prices:** Polymarket **WebSocket** market channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) for UP/DOWN mids.
- **BTC:** **Binance WebSocket** `btcusdt@trade` for live spot; **Binance REST** kline **open** at the slug epoch for `start_btc` (same idea as `KNG4/prst1/clob_shim.fetch_binance_window_open_btc`).

## Preset strategies (BTC aligned with `kng_bot3` PALADIN KNGTOP presets)

Per window, rules are evaluated **in list order**; the **first** that fires gets the single trade.

### 5m (`RULES_5M`) — gap **$5** for every rule

| Order | Key | Logic |
|------|-----|--------|
| 1 | `u_up_cheap` | `btc > start + 5` and `mid_up ≤ 0.35` → buy **UP** |
| 2 | `u_dn_cheap` | `btc < start − 5` and `mid_dn ≤ 0.35` → buy **DOWN** |
| 3 | `o_fade_up_s` | `btc < start − 5` and `mid_up ≥ 0.68` → buy **DOWN** |
| 4 | `o_fade_dn_s` | `btc > start + 5` and `mid_dn ≥ 0.68` → buy **UP** |

### 15m (`RULES_15M`) — gap **$10** for every rule

| Order | Key | Logic |
|------|-----|--------|
| 1 | `u_up_cheap` | `btc > start + 10` and `mid_up ≤ 0.38` → buy **UP** |
| 2 | `u_dn_cheap` | `btc < start − 10` and `mid_dn ≤ 0.38` → buy **DOWN** |
| 3 | `o_fade_up_s` | `btc < start − 10` and `mid_up ≥ 0.72` → buy **DOWN** |
| 4 | `o_fade_dn_s` | `btc > start + 10` and `mid_dn ≥ 0.68` → buy **UP** |

**Disclaimer:** Live Polymarket resolution may **not** match Binance `start_btc` / last trade. This is execution plumbing + the research thresholds, not guaranteed edge.

## Run (Docker)

```bash
cp .env.example .env
# edit .env — set keys; POLY_DRY_RUN=false only when ready
docker compose --env-file .env up --build -d
```

## Runtime Logging And Retry

- Log stream is intentionally minimal for live ops:
  - `INIT`
  - `DEAL_START`
  - `DEAL_SUCCESS` / `DEAL_FAIL`
  - `ERROR`
- Configure retry-on-error for `$1` market order with `KNGTOP_ORDER_RETRY_ON_ERROR`:
  - `2` means first attempt + 2 retries = **3 total attempts**

## Layout

- **`Dockerfile` / `docker-compose.yml`**: modeled on **`KNG4`** (slim Python, `python -m` entry).
- **`kngtop/ws_market.py`**: from **`KNG3`** `polymarket_ws.py` (market channel).
- **`kngtop/clob_client.py`**: from **`KNG4`** `prst1/clob_shim.py` (v2 `create_and_post_market_order`).

## Local (no Docker)

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

CI (GitHub Actions) runs **pytest** and **`docker build`** on each push/PR to `main`.
