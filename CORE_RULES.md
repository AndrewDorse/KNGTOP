# Core Rules

## One Trading Path

1. **Pre-start flat window** -> from 20s before open, rest one 5-share buy on UP and one 5-share buy on DOWN at `0.47`.
2. **Imbalanced or missing side** -> send only the required smaller/missing side (`_required_hedge_side`).
3. **Balanced within gap** -> may add one side only when the limit is at least `0.02` below that side's current average and projected avg-sum stays `<= 0.95`.

While `OrderCycle` is busy, strategy is blocked. The pre-start opening pair is the only two-order exception.

## Order Cycle

1. Send one primary limit -> stop.
2. Poll CLOB until that order id is on book.
3. Poll PM until primary fill is confirmed on that side.
4. Send one hedge on opposite side if still required -> stop.
5. Poll CLOB until hedge order id is on book, or PM already shows hedge fill.
6. Poll PM until hedge fill is confirmed when needed.
7. PM wait: refresh every 1s, need 5 stable reads and:
   - PM shows fills from cycle start
   - `abs(up - down) <= max_share_gap`
   - neither side over `max_shares_per_side`
8. `[CYCLE DONE]` -> idle -> back to path 2 or 3 only if guards allow.

Max 2 sends per cycle. All live CLOB sends go through `_post_cycle_limit()` only.

Never cancel CLOB orders from this bot. Never re-post after a leg is filled.

## Position Truth

- Decisions use PM-confirmed positions.
- Never buy over `max_shares_per_side`; block new sends when effective shares (filled + reserved) would exceed cap.
- Adopt orphan CLOB orders on the required side only. `order_on_clob` matches order id only.

## Logs

- `[OPENING PAIR SENT]`
- `[CYCLE PRIMARY SENT]` / `[CYCLE PRIMARY ON BOOK]` / `[CYCLE PRIMARY FILL]`
- `[CYCLE HEDGE SENT]` / `[CYCLE HEDGE ON BOOK]` / `[CYCLE HEDGE FILL]`
- `[CYCLE PM WAIT] reason=not_ready`
- `[CYCLE PM CHECK]`
- `[CYCLE DONE]`
- `[SKIP] reason=balanced_no_trade`
