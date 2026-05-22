# Core Rules

- Trade BTC 5m UP/DOWN windows only.
- First buy: lower-ask side, max `$2`, only if ask is `<= 0.55`.
- If first buy fails/no-fills, retries are `$1+`, not another blind `$2`.
- Always confirm live fills from Polymarket positions before counting shares/spend.
- Missing side gets priority until both sides are open.
- After both sides are open, keep buying the worse PnL side if that side improves, until both sides are profitable.
- Prefer the current winning side to be equal or slightly bigger, while keeping sizes close.
- Later buys can be any cent amount from `$1.00` to `$2.00`.
- Max `15` shares per side by default.
- Do not buy if it breaks budget/share cap; avg-sum/balance guards apply after both sides are profitable.
- Stop only when both outcomes reach the configured ROI target.
