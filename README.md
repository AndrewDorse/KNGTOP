# KNGTOP

Dockerized Polymarket Up/Down runner. Current default live entrypoint is the BTC 5m guarded PnL-balance bot.

- Prices: Polymarket WebSocket market channel for UP/DOWN mids.
- Binance: live spot from WebSocket and window-open start price from REST kline open at the slug epoch.

## Current strategy

Current live entrypoint is `BTC` `5m` only and runs guarded PnL-balance candidate `C`.

- Initial buy:
  - every decision slot is `5s`
  - first buy intent is the lower-ask side for `$2` if ask `<= 0.55`
  - if that first FAK no-fills or fails, the bot does not repeat the same `$2`; later zero-position attempts are `$1` and require another side, a better price, or the retry wait
- Guarded PnL repair:
  - after every fill, compute `pnl_if_up`, `pnl_if_down`, and the weaker outcome side
  - if both sides are open and one side is already too large, repair the smaller-share side first
  - otherwise buy only the weaker outcome side
  - cheap weak repair: if weak-side ask `<= 0.45`, buy it
  - high guard C: if weak-side ask `> 0.45`, normal guard cap is `<= 0.60`
  - pre-240s hard high cap is `0.65`
  - final 60s can buy up to `0.80` only when that outcome is dangerously weak
  - post-open repair candidates must keep projected avg sum `<= 0.95`, avoid flipping the bought side past the other side, and keep/improve the share gap
  - stop only when both outcomes have reached the locked-profit ROI target; with `15` max shares and `10%`, that is `$1.50` per outcome
- Order:
  - FAK buys only
  - one order intent at a time; no overlapping sends while a request is in flight
  - live fills are counted only after Polymarket Data API position size confirms the fill
  - `$2` first intent only
  - later order size is projected from `$1.00` to `$2.00` in `$0.20` steps to improve worst-case outcome PnL and reduce outcome gap
  - max `$20` spent per window
  - max `15` PM-confirmed shares per side, configurable with `KNGTOP_MAX_SHARES_PER_SIDE`
  - live max price = displayed ask, bounded by `KNGTOP_MARKET_BUY_MAX_PRICE`

## Sizing

- minimum live order is `$1`
- `KNGTOP_NOTIONAL_USD` currently acts as the upper bound for the first buy and projected repair buys
- `KNGTOP_MAX_SHARES_PER_SIDE` defaults to `15.0`; buys are blocked if the remaining share room cannot fit at least the `$1` minimum order
- `KNGTOP_MAX_SHARE_GAP` defaults to `2.0`; post-open repair buys are blocked when projected shares would remain too unbalanced
- `KNGTOP_REPAIR_AVG_SUM_CAP` defaults to `0.95`; post-open repair buys are blocked when projected avg sum is worse than this cap
- `KNGTOP_LOCKED_PROFIT_ROI` defaults to `0.10`; the bot stops only when both outcome PnLs are at least this value times `KNGTOP_MAX_SHARES_PER_SIDE`
- order retry on error defaults to `0`; failed/no-fill FAK attempts do not update position state
- balance/allowance errors stop the current window instead of retrying the same order

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
