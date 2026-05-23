# Core Rules

## ONE trading path

1. **Flat window** → bootstrap: send cheaper side, then mandatory opposite hedge in same cycle.
2. **Imbalanced or missing side** → send **only** the required smaller/missing side (`_required_hedge_side`).
3. **Balanced within gap** → **DO NOTHING**. No repair, no winning-side adds, no growth.

While `OrderCycle` is busy → strategy is blocked. No exceptions.

## Order cycle (per pair)

1. Send **one** primary limit → STOP.
2. Poll CLOB until **that order id** is on book (API delay OK, never re-send).
3. Send **one** hedge on opposite side if still required → STOP.
4. Poll CLOB until hedge on book.
5. PM wait: refresh every 1s, need **5 stable reads** AND:
   - PM shows fills from cycle start (both legs if hedged)
   - `abs(up - down) <= max_share_gap`
   - neither side over cap
6. `[CYCLE DONE]` → idle → back to step 1 only if path 2 applies.

Max **2 sends per cycle**, tracked in memory. All live CLOB sends go through `_post_cycle_limit()` only.

## Position truth

- Decisions from PM-confirmed positions.
- Never buy over-cap side; hedge under-cap side when other leg is over cap.
- Adopt orphan CLOB orders (no duplicate send). `order_on_clob` matches **order id only**.

## Logs

- `[CYCLE PRIMARY SENT]` / `[CYCLE PRIMARY ON BOOK]`
- `[CYCLE HEDGE SENT]` / `[CYCLE HEDGE ON BOOK]`
- `[CYCLE PM WAIT] reason=not_ready` — blocked until balanced fills confirmed
- `[CYCLE PM CHECK]` streak toward 5
- `[CYCLE DONE]` — only when balanced + capped + fills reflected
- `[SKIP] reason=balanced_no_trade` — balanced, intentionally idle
