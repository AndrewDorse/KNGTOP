# Engine Strategy Snapshots

- `engine_99_live_kilemo2.py`: last pushed engine strategy from commit `06dacb6`.
- `engine_98_live_kilemo2.py`: tighter avg-sum repair strategy from commit `2d9a845`.
- `engine_97_live_kilemo2.py`: effective-position risk repair for API lag, hard caps, and one-sided end-window guard.
- `engine_96_live_kilemo2.py`: immediate FAK fill-risk accounting and smaller-side balance sizing fix.
- `engine_95_live_kilemo2.py`: managed limit-order engine with one pending order per window and 5-share/$1 minimum sizing.
- `engine_94_live_kilemo2.py`: passive limit-order fix using one-tick-below-ask prices and $1.05 notional safety buffer.
- After each new push, save the pushed engine here with the next lower number: `93`, `92`, etc.
