# Core Rules

## Deal gate — ONE order, wait for PM, then next

1. **Send one order** — `register_intent()` refuses if any deal is in flight.
2. **Do nothing until PM confirms** — after post, every tick runs `_wait_active_deal()`: refresh PM API, compare `shares_side` to `order.pre_shares`. No strategy until fill confirmed or order failed/cancelled.
3. **Only then next order** — deal closes with `[ORDER DONE]` when PM shows new shares; next send allowed.
4. **Count sends** — `runner.orders_sent` + log `[ORDER SEND] send_n=N`.

## Position truth

- All cap/gap/hedge decisions from PM-confirmed positions (`_confirmed_position_state`).
- Never buy the over-cap side; still hedge the smaller side when the other leg is over cap.

## Strategy (unchanged)

- BTC 5m UP/DOWN only.
- Missing/smaller side gets priority when share gap > max (default 2).
- Max 15 shares per side; one limit at a time.
- Reconcile CLOB + PM every 1s.

## Logs

- `[ORDER SEND]` — order going out
- `[ORDER ON BOOK]` — posted, waiting PM
- `[DEAL] state=WAIT_PM_FILL` — blocked, waiting fill
- `[ORDER DONE]` — PM confirmed, idle again
- `[OVER_CAP_HEDGE]` — over cap on one side, hedging the other
