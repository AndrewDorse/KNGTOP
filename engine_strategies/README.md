# Engine Strategy Snapshots

- `engine_86_live_kilemo2.py` + `engine_86_live_orders.py`: strict two-leg `OrderCycle` — one primary, confirm CLOB, one hedge, 5× PM stable, then next (current push).
- `engine_88_live_kilemo2.py` + `engine_88_live_orders.py`: unified `LiveOrder` registry, non-blocking send, reconcile-only lifecycle.
- `engine_99_live_kilemo2.py`: last pushed engine strategy from commit `06dacb6`.
- `engine_98_live_kilemo2.py`: tighter avg-sum repair strategy from commit `2d9a845`.
- `engine_97_live_kilemo2.py`: effective-position risk repair for API lag, hard caps, and one-sided end-window guard.
- `engine_96_live_kilemo2.py`: immediate FAK fill-risk accounting and smaller-side balance sizing fix.
- `engine_95_live_kilemo2.py`: managed limit-order engine with one pending order per window and 5-share/$1 minimum sizing.
- `engine_94_live_kilemo2.py`: passive limit-order fix using one-tick-below-ask prices and $1.05 notional safety buffer.
- `engine_93_live_kilemo2.py`: explicit imbalance limit repair that places smaller-side orders at needed avg-improving prices.
- `engine_92_live_kilemo2.py`: hard share-gap guard on every 5-share limit order; balance-first repair with no avg-price blockers.
- `engine_91_live_kilemo2.py`: CLOB open-order sync each tick, cancel duplicates, max one active limit order, urgent balance when imbalanced.
- `engine_90_live_kilemo2.py`: hedge side from confirmed fills only; block rebootstrap and wrong-side limits after one-sided UP/DOWN open.
- After each new push, save the pushed engine here with the next lower number: `89`, `88`, etc.
