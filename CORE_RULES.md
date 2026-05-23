# Core Rules

- Trade BTC 5m UP/DOWN windows only.
- First buy: lower-ask side, max `$2`, only if ask is `<= 0.55`.
- If first buy fails/no-fills, retries are `$1+`, not another blind `$2`.
- Always confirm live fills from Polymarket positions before counting shares/spend.
- Missing side gets priority until both sides are open.
- After both sides are open, keep buying the worse PnL side if that side improves, until both sides are profitable.
- Repair buys after both sides are open should be below that side's current avg, so avg sum improves.
- Prefer the current winning side to be equal or slightly bigger, while keeping sizes close.
- Later buys can be any cent amount from `$1.00` to `$2.00`.
- Max `15` shares per side by default.
- Max share gap `2` shares between UP and DOWN after each fill.
- If share gap exceeds max, only buy the smaller side until balanced.
- One limit order at a time; wait for fill/cancel before sending another.
- Sync open orders from CLOB every tick; cancel duplicates so only one active order exists per window.
- If imbalanced and no active order exists, place a balance limit immediately (do not wait for repair slot).
- Do not buy if it breaks budget/share cap; avg-sum/balance guards apply after both sides are profitable.
- Stop only when both outcomes reach the configured ROI target.
