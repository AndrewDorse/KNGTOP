# Core Rules

## Order cycle — send once, confirm, hedge once, 5× PM, then next

1. **One cycle at a time** — `OrderCycle` in memory; strategy blocked while `phase != idle`.
2. **Primary send once** — `cycle_begin_primary()` then STOP; poll CLOB until order is on book (API delay OK, no re-send).
3. **Hedge send once** — after primary confirmed on CLOB, send hedge limit immediately if smaller/missing side needs it; tracked in memory, never spammed.
4. **5× PM stable** — after legs placed, refresh PM every 1s; require 5 consecutive stable reads (5s total) before `cycle_reset()` and next pair.
5. **Count sends** — max 2 per cycle (`sends_this_cycle`); `runner.orders_sent` total.

## Position truth

- Cap/gap/hedge from PM-confirmed positions (`_confirmed_position_state`).
- Never buy the over-cap side; still hedge the under-cap side when the other leg is over cap.
- Adopt orphan CLOB open orders into cycle (no duplicate send).

## Strategy (unchanged)

- BTC 5m UP/DOWN only.
- Missing/smaller side priority when share gap > max (default 2).
- Max 15 shares per side.
- Reconcile CLOB + PM every 1s.

## Logs

- `[CYCLE PRIMARY SENT]` — first leg sent, waiting CLOB
- `[CYCLE PRIMARY ON BOOK]` — CLOB confirms; hedge or PM wait next
- `[CYCLE HEDGE SENT]` / `[CYCLE HEDGE ON BOOK]` — second leg (once)
- `[CYCLE PM CHECK]` — stable streak toward 5
- `[CYCLE DONE]` — idle; next pair allowed
