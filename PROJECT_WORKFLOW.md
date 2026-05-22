# Project Workflow

## Core Rules

- Keep current live buy rules in `CORE_RULES.md`.
- Before changing strategy logic, check the change against `CORE_RULES.md`.
- If a requested change conflicts with `CORE_RULES.md`, stop and say what conflicts before continuing.

## Engine Strategy Snapshots

- Save strategy snapshots in `engine_strategies/`.
- Use descending engine numbers:
  - `engine_99_live_kilemo2.py` = last pushed strategy before the current update.
  - `engine_98_live_kilemo2.py` = current strategy for this update.
  - Next pushed strategies continue as `97`, `96`, etc.
- After each push, save the pushed `kngtop/live_kilemo2.py` as the next engine snapshot.
- Keep `engine_strategies/README.md` updated when a new engine snapshot is added.

## Comparing Strategies

- Compare the current engine against the previous pushed engine on the same window pool.
- Use the same config for both engines:
  - `MAX_SHARES_PER_SIDE=15`
  - `MAX_SHARE_GAP=2`
  - `REPAIR_AVG_SUM_CAP=0.95`
  - `LOCKED_PROFIT_ROI=0.10`
- Report clear totals only unless detailed trades are requested:
  - realized PnL
  - spent total
  - ROI on spent
  - traded windows
  - wins / losses / flats
  - avg spent
  - avg deals
  - avg avg-sum
  - max share gap
  - one-sided windows
- Do not show "guaranteed worst PnL total" unless explicitly requested.

## Current Engine

- `98` is the current pushed strategy snapshot for the tighter avg-sum repair logic.
- `98` requires post-open repair buys to be below the same side average by the configured buffer.
- `98` rejects Polymarket API `avgPrice=0` for cost basis and falls back to the pending sent order price when needed.
