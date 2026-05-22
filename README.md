# KNGTOP

Dockerized Polymarket Up/Down runner. Current default live entrypoint is the BTC 5m `KILEMO_2` bootstrap/active-repair bot.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs `KILEMO_2` variant `bootstrap_active_repair_C + rescue_60_cap080`.

- Bootstrap:
  - before `15s`, buy the cheaper side for `$1` if ask `<= 0.55`
  - at `15s`, if only one side is open, buy the missing side for `$1` if ask `<= 0.70`
- Active repair:
  - every `15s`
  - if share imbalance `> 20%`, buy the smaller-share side
  - if a side ask is at least `0.02` below that side's average entry, buy that side
  - if `avg_up + avg_down <= 0.95`, allow extra averaging buys on asks `<= 0.45`
  - if both sides are already roughly balanced and one ask `<= 0.35`, buy that side
  - last `60s`: only buy the smaller-share side
  - last `30s`: smaller-share side is only allowed up to ask `0.80`
- Rescue:
  - if the window is still one-sided at `60s` remaining, buy the missing side if ask `<= 0.80`
- Order:
  - FAK buys only
  - `$1` default orders
  - `$2` only if ask `<= 0.30` or share imbalance `> 40%`
  - live max price = displayed ask, bounded by `KNGTOP_MARKET_BUY_MAX_PRICE`

## Sizing

- minimum live order is `$1`
- the strategy may use `$2` on deep/imbalanced repair buys
- `KNGTOP_NOTIONAL_USD` currently acts as the upper bound for those larger repair orders

## Run

```bash
cp .env.example .env
docker compose --env-file .env up --build -d
```

Container command is `python -m kngtop`, which resolves to the current `KILEMO_2` live bot in `kngtop.live_kilemo2`.

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
