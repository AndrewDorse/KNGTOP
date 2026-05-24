# KNGTOP

Dockerized Polymarket live runner. Current default entrypoint is the BTC 5m Binance-spike paired resting-limit strategy.

## Live strategy

Current live bot is `disc06_exp30_cap95_b15` with live safeguards:

- market: `BTC` `5m` up/down
- signal: Binance spike only
  - move lookback `5s`
  - move threshold `20 USD`
  - volume lookback `20s`
  - volume ratio min `1.8`
- order staging on signal:
  - trigger-side resting buy at current ask
  - opposite-side resting buy at `ask - 0.06`
  - order size `5` shares
  - if one side is smaller by more than `5` filled shares, smaller side is boosted to `10` shares
- pair guards:
  - projected avg sum after the staged pair must stay `<= 0.95`
  - max `15` filled shares per side
  - max `15 USD` total open+filled exposure per window
  - no new buys after `220s`
  - cancel all buys after `240s`
- lifecycle:
  - one tick execution at a time
  - live positions and open orders are reconciled from Polymarket before decisions
  - if a fresh spike arrives after cooldown and a hanging buy remains, cancel the hanging buy first
  - pair cooldown `10s`
  - resting order expiry `30s`

## Entry point

`python -m kngtop` now launches [kngtop/engine.py](C:/Users/Lenovo/Documents/Git/KNGTOP/kngtop/engine.py:1).

## Run

```bash
cp .env.example .env
docker compose --env-file .env up --build -d
```

## Local

```bash
pip install -r requirements.txt -r requirements-dev.txt
set PYTHONPATH=.
python -m kngtop
```

## Tests

```bash
pytest -q
```
