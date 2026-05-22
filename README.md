# KNGTOP

Dockerized Polymarket Up/Down runner. Current default live entrypoint is the BTC 5m guarded PnL-balance bot.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs guarded PnL-balance candidate `C`.

- Initial buy:
  - every decision slot is `5s`
  - first buy is the lower-ask side for `$2` if ask `<= 0.55`
- Guarded PnL repair:
  - after every fill, compute `pnl_if_up`, `pnl_if_down`, and the weaker outcome side
  - buy only the weaker outcome side
  - cheap weak repair: if weak-side ask `<= 0.45`, buy it
  - high guard C: if weak-side ask `> 0.45`, normal guard cap is `<= 0.60`
  - pre-240s hard high cap is `0.65`
  - final 60s can buy up to `0.80` only when that outcome is dangerously weak
  - if both outcomes are already `>= +0.50`, buy only cheap `<= 0.45` or when share imbalance is `> 25%`
- Order:
  - FAK buys only
  - `$1` default repair orders
  - `$2` initial buy
  - `$2` later only if ask `<= 0.30` or share imbalance `> 40%`
  - max `$20` spent per window
  - max `15` filled buys per window
  - max `8` filled buys per side
  - live max price = displayed ask, bounded by `KNGTOP_MARKET_BUY_MAX_PRICE`

## Sizing

- minimum live order is `$1`
- the strategy may use `$2` on deep/imbalanced repair buys
- `KNGTOP_NOTIONAL_USD` currently acts as the upper bound for `$2` initial/deep/imbalanced buys
- order retry on error defaults to `0`; failed/no-fill FAK attempts do not update position state

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
