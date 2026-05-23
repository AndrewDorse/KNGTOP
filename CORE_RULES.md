# Core Rules

## ONE trading path

1. **Flat window** → bootstrap: send cheaper side, then mandatory opposite hedge in same cycle (after primary fill confirmed).
2. **Imbalanced or missing side** → send **only** the required smaller/missing side (`_required_hedge_side`).
3. **Balanced within gap** → **DO NOTHING**. No repair, no winning-side adds, no growth.

While `OrderCycle` is busy → strategy is blocked. No exceptions.

## Order cycle (per pair)

1. Send **one** primary limit → STOP.
2. Poll CLOB until **that order id** is on book (API delay OK, never re-send).
3. Poll PM until **primary fill** is confirmed on that side.
4. Send **one** hedge on opposite side if still required → STOP (never re-post if filled or order leaves book).
5. Poll CLOB until hedge order id is on book (or PM already shows hedge fill).
6. Poll PM until **hedge fill** confirmed when needed.
7. PM wait: refresh every 1s, need **5 stable reads** AND:
   - PM shows fills from cycle start (both legs if hedged)
   - `abs(up - down) <= max_share_gap`
   - neither side over `max_shares_per_side`
8. `[CYCLE DONE]` → idle → back to step 1 only if path 2 applies.

Max **2 sends per cycle**, tracked in memory. All live CLOB sends go through `_post_cycle_limit()` only.

**Never cancel** CLOB orders from this bot. **Never re-post** after a leg is filled.

## Position truth

- Decisions from PM-confirmed positions.
- Never buy over `max_shares_per_side`; block new sends when effective shares (filled + reserved) would exceed cap.
- Adopt orphan CLOB orders on the required side only (no duplicate send). `order_on_clob` matches **order id only**.

## Logs

- `[CYCLE PRIMARY SENT]` / `[CYCLE PRIMARY ON BOOK]` / `[CYCLE PRIMARY FILL]`
- `[CYCLE HEDGE SENT]` / `[CYCLE HEDGE ON BOOK]` / `[CYCLE HEDGE FILL]`
- `[CYCLE PM WAIT] reason=not_ready` — blocked until balanced fills confirmed
- `[CYCLE PM CHECK]` streak toward 5
- `[CYCLE DONE]` — only when balanced + capped + fills reflected
- `[SKIP] reason=balanced_no_trade` — balanced, intentionally idle
