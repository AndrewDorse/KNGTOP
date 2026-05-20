# Strategy History

This file keeps a short memory of recent live strategy configurations.

## Current Live Strategy

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` only
- Family: `cheap_winner_momentum`
- Variant: `ub0.25_e20_lb5_m0`
- Signal:
  - buy current winner side only
  - price band `0.01-0.25`
  - min elapsed `20s`
  - momentum lookback `5s`
  - momentum threshold `>= 0 bps`
- Order:
  - limit buy
  - limit price = `signal price + 0.03`
- Sizing:
  - `5%` of available balance at window start
  - minimum budget `$1.25`
  - maximum budget `$200`
  - minimum order size `5` shares

## Archived Live Strategies

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` and `15m`
- Family: `reclaim`
- BTC `5m`:
  - price band `0.01-0.30`
  - min elapsed `30s`
  - lookback `40s`
  - gap `>= 0.05`
- BTC `15m`:
  - price band `0.01-0.25`
  - min elapsed `180s`
  - lookback `40s`
  - gap `>= 0.05`

### 2026-05-20
- Scope: `BTC` only
- Windows: `5m` and `15m`
- Families in parallel on `5m`:
  - `reclaim`
  - `CWC`
- Note: one strategy could fire without closing the full window to the other strategy.

### 2026-05-19 and earlier
- Several `BTC` and `ETH` reclaim / CWC / limit-price experiments were tested and replaced.
- See git history and exports in `kng_bot3/exports` for the full research trail.
