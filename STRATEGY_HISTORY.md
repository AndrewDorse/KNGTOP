# Strategy History

This file keeps a short memory of recent live strategy configurations.

## Current Live Strategy

### 2026-05-22
- Scope: `BTC` only
- Windows: `5m` only
- Family: `KILEMO_2`
- Variant: `guarded_pnl_balance_C`
- Flow:
  - first buy intent is the lower-ask side for `$2` if ask `<= 0.55`
  - if that first FAK no-fills or fails, do not repeat the same `$2`; later zero-position attempts are `$1` and require another side, a better price, or the retry wait
  - reevaluate every `5s`
  - after each fill, compute `pnl_if_up`, `pnl_if_down`, and the weaker outcome side
  - if both sides are open and one side is already too large, repair the smaller-share side first
  - otherwise buy only the weaker outcome side
  - cheap weak repair: ask `<= 0.45`
  - guarded high repair C: high guard `<= 0.60`
  - if ask `> 0.60`, allow only when projected worst PnL reaches `>= -0.25` or projected share gap reaches `<= 10%`
  - pre-240s hard high cap `0.65`
  - final `60s`: up to `0.80` only if the weak outcome is dangerously weak
  - post-open repair candidates must keep projected avg sum `<= 0.95`, avoid flipping the bought side past the other side, and keep/improve the share gap
  - stop only when both outcomes have reached the locked-profit ROI target; with `15` max shares and `10%`, that is `$1.50` per outcome
- Order:
  - FAK buys only
  - `$2` first intent only
  - later buy size is projected from `$1.00` to `$2.00` in `$0.20` steps
  - one order intent at a time
  - live position updates only after Polymarket Data API confirms position-size growth
  - no-fill/error attempts do not update orders, spent, or shares
  - balance/allowance errors stop the current window
  - order retry on error defaults to `0`
  - max `$20` spent per window
  - max `15` PM-confirmed shares per side via `KNGTOP_MAX_SHARES_PER_SIDE`
  - max post-open share gap `2.0` via `KNGTOP_MAX_SHARE_GAP`
  - max post-open repair avg sum `0.95` via `KNGTOP_REPAIR_AVG_SUM_CAP`
  - locked-profit stop ROI `10%` via `KNGTOP_LOCKED_PROFIT_ROI`

### 2026-05-21
- Scope: `BTC` only
- Windows: `5m` only
- Family: `S0184`
- Flow:
  - buy the current winner side only
  - only within the first `25s` of the window
  - winner-side ask must be in `0.45-0.55`
  - BTC absolute move from the window open must be `>= $1`
- Order:
  - FAK buys only
  - no artificial delay in live
  - one buy per window
  - order size = `10%` of available balance, floored at `$1`, checked once at window start
  - max price = displayed ask `+ 0.03`, capped at `0.99`
  - no take-profit exit; hold to resolution

### 2026-05-21 earlier
- Scope: `BTC` only
- Windows: `5m` only
- Family: `KILEMO_2`
- Variant: `H2725`
- Flow:
  - seed the current winner side only
  - seed ask must be in `0.35-0.50`
  - BTC winner-side move over `10s >= $2`
  - after a seed fill, keep recomputing `PnL if UP wins` and `PnL if DOWN wins`
  - hedge only the weaker outcome side
  - hedge side ask must be `<= 0.35`
- Order:
  - FAK buys only
  - no artificial delay in live
  - seed/base notional = `KNGTOP_NOTIONAL_USD` default `$2`
  - hedge buy range = `$1` up to `KNGTOP_NOTIONAL_USD`
  - max `2` buys per side via `KNGTOP_HEDGE_MAX_ORDERS_PER_SIDE`
  - sub-minimum but beneficial hedge sizes are rounded up to the exchange minimum instead of being skipped
  - total live budget cap `$30` per window

### 2026-05-21 earlier still
- Scope: `BTC` only
- Windows: `5m` only
- Family: `cheap_hit_close_volume_and_move`
- Variant: `close<=30 + vol20>=1.4x AND move20>=2usd_or_open`
- Signal:
  - first cheap side mid `<= 0.15`
  - side is whichever of `UP` / `DOWN` is cheaper at the trigger
  - Binance spot must remain within `$30` of the window open
  - current trade-size volume / previous `20s` mean `>= 1.4x`
  - cheap-side BTC alignment over `5s` must be non-negative, or side must already be beyond window open
  - cheap-side BTC move over `20s >= $2`, or side must already be beyond window open
- Order:
  - `$1` FAK buy
  - hard cap `0.25`
  - no artificial `2s` delay in live
  - one buy attempt max per window

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` only
- Family: `serial_hedge`
- Variant: `start 12c / target sum 68c`
- Flow:
  - at window start, place resting buy limits on both `UP` and `DOWN` at `0.12`
  - whichever starter fills first becomes the first leg
  - cancel the opposite starter
  - place the hedge on the opposite side at `0.56`
  - one pair max per window
- Sizing:
  - `5%` of available balance at window start
  - minimum budget `$1.25`
  - maximum budget `$200`
  - minimum order size `5` shares

## Archived Live Strategies

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` only
- Family: `cheap_winner_momentum`
- Variant: `ub0.25_e20_lb5_m0`
- Signal:
  - buy current winner side only
  - price band `0.01-0.25`
  - min elapsed `20s`
  - momentum lookback `5s`
  - momentum threshold `>= 0 bps`
- Order:
  - limit buy
  - limit price = `signal price + 0.03`
- Sizing:
  - `5%` of available balance at window start
  - minimum budget `$1.25`
  - maximum budget `$200`
  - minimum order size `5` shares

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` and `15m`
- Family: `reclaim`
- BTC `5m`:
  - price band `0.01-0.30`
  - min elapsed `30s`
  - lookback `40s`
  - gap `>= 0.05`
- BTC `15m`:
  - price band `0.01-0.25`
  - min elapsed `180s`
  - lookback `40s`
  - gap `>= 0.05`

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` and `15m`
- Families in parallel on `5m`:
  - `reclaim`
  - `CWC`
- Note: one strategy could fire without closing the full window to the other strategy.

### 2026-05-19 and earlier
- Several `BTC` and `ETH` reclaim / CWC / limit-price experiments were tested and replaced.
- See git history and exports in `kng_bot3/exports` for the full research trail.
