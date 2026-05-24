"""Replay adapter for the live limit engine.

This module intentionally does not contain strategy logic. It feeds historical
per-second prices into ``live_limit_engine._tick_runner`` and simulates the PM
APIs the live engine already consumes: positions, open orders, and trade
history.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from unittest.mock import patch

from kngtop.config import KngtopConfig
from kngtop.gamma import ActiveContract, TokenMarket
from kngtop.live_limit_engine import WindowRunner, _tick_runner
from kngtop.simulate_strategies import FEE_BUFFER_PER_TRADE, Tick, WindowData, load_windows

UP_TOKEN_ID = "up-token"
DOWN_TOKEN_ID = "down-token"


def replay_cfg() -> KngtopConfig:
    return KngtopConfig(
        private_key="pk",
        funder="0xabc",
        signature_type=1,
        relayer_api_key="",
        relayer_secret="",
        relayer_passphrase="",
        dry_run=False,
        poll_interval_sec=0.2,
        eval_debounce_sec=0.0,
        request_timeout_sec=5.0,
        notional_usd=2.0,
        trading_pairs=(("BTC", "BTCUSDT"),),
        log_level="WARNING",
        order_cutoff_remaining_sec=20.0,
        order_retry_on_error=0,
        market_buy_max_price=0.90,
        binance_max_age_sec=6.0,
        poly_mid_max_age_sec=5.0,
        ws_rest_poll_enabled=False,
        ws_rest_poll_interval_sec=1.0,
        hedge_max_orders_per_side=2,
        max_shares_per_side=15.0,
        max_share_gap=2.0,
        repair_avg_sum_cap=0.95,
        locked_profit_roi=0.10,
    )


class ReplayPoly:
    def __init__(self, *, up: float = 0.5, down: float = 0.5) -> None:
        self.up = float(up)
        self.down = float(down)

    def set_prices(self, *, up: float, down: float) -> None:
        self.up = float(up)
        self.down = float(down)

    def best_bid_ask_for(self, asset_id: str, max_age_sec: float = 5.0):  # noqa: ANN201
        del max_age_sec
        ask = self.up if asset_id == UP_TOKEN_ID else self.down
        return max(0.01, ask - 0.01), ask


@dataclass(slots=True)
class ReplayFill:
    side: str
    price: float
    shares: float
    elapsed: int
    order_id: str


@dataclass(slots=True)
class ReplayClob:
    open_orders: list[dict[str, object]] = field(default_factory=list)
    recent_trades: dict[str, list[dict[str, object]]] = field(default_factory=lambda: {UP_TOKEN_ID: [], DOWN_TOKEN_ID: []})
    limit_calls: list[tuple[str, float, float]] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    fills: list[ReplayFill] = field(default_factory=list)
    fail_next_place: bool = False
    _next_id: int = 1

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float, post_only: bool = True):  # noqa: ANN201
        assert post_only is True
        self.limit_calls.append((token.token_id, float(price), float(shares)))
        if self.fail_next_place:
            self.fail_next_place = False
            raise TimeoutError("place timeout")
        order_id = f"sim-{self._next_id}"
        self._next_id += 1
        self.open_orders.append(
            {
                "id": order_id,
                "asset_id": token.token_id,
                "side": "BUY",
                "price": float(price),
                "original_size": float(shares),
                "size_left": float(shares),
            }
        )
        return {"ok": True, "orderID": order_id}

    def cancel_order_by_id(self, order_id: str):  # noqa: ANN201
        self.cancelled.append(str(order_id))
        self.open_orders = [row for row in self.open_orders if str(row.get("id")) != str(order_id)]
        return {"ok": True}

    def get_open_orders(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.open_orders]

    def get_recent_trades(self, token: TokenMarket, *, after_ts: int) -> list[dict[str, object]]:
        del after_ts
        return [dict(row) for row in self.recent_trades.get(token.token_id, [])]

    def process_fills(self, *, up_price: float, down_price: float, elapsed: int) -> None:
        kept: list[dict[str, object]] = []
        for row in self.open_orders:
            token_id = str(row["asset_id"])
            side = "UP" if token_id == UP_TOKEN_ID else "DOWN"
            market_price = float(up_price if side == "UP" else down_price)
            limit_price = float(row["price"])
            shares = float(row["size_left"])
            order_id = str(row["id"])
            if market_price <= limit_price + 1e-12:
                self.fills.append(ReplayFill(side=side, price=limit_price, shares=shares, elapsed=int(elapsed), order_id=order_id))
                self.recent_trades[token_id].append(
                    {
                        "id": order_id,
                        "orderId": order_id,
                        "side": "SELL",
                        "price": limit_price,
                        "size": shares,
                    }
                )
            else:
                kept.append(row)
        self.open_orders = kept

    def position_rows(self, slug: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for side, token_id in (("UP", UP_TOKEN_ID), ("DOWN", DOWN_TOKEN_ID)):
            fills = [fill for fill in self.fills if fill.side == side]
            shares = sum(fill.shares for fill in fills)
            if shares <= 0:
                continue
            avg = sum(fill.price * fill.shares for fill in fills) / shares
            rows.append({"slug": slug, "outcome": side, "asset": token_id, "size": shares, "avgPrice": avg})
        return rows

    def runtime_state(self, slug: str) -> dict[str, object]:
        return {
            "reconcile_positions": self.position_rows(slug),
            "reconcile_open_orders": self.get_open_orders(),
            "reconcile_trade_history": {
                "UP": self.get_recent_trades(TokenMarket(UP_TOKEN_ID, "UP", "0.01", False), after_ts=0),
                "DOWN": self.get_recent_trades(TokenMarket(DOWN_TOKEN_ID, "DOWN", "0.01", False), after_ts=0),
            },
        }


def open_order_row(*, order_id: str, token_id: str, price: float, size_left: float = 5.0) -> dict[str, object]:
    return {"id": order_id, "asset_id": token_id, "side": "BUY", "price": price, "original_size": 5.0, "size_left": size_left}


def runner_for_window(window: WindowData) -> WindowRunner:
    start = int(window.window_id.rsplit("-", 1)[-1])
    return WindowRunner(
        pair_key="BTC",
        binance_symbol="BTCUSDT",
        contract=ActiveContract(
            slug=window.window_id,
            question="",
            end_time=datetime.fromtimestamp(start + 300, timezone.utc),
            up=TokenMarket(UP_TOKEN_ID, "UP", "0.01", False),
            down=TokenMarket(DOWN_TOKEN_ID, "DOWN", "0.01", False),
        ),
        window_minutes=5,
        window_open_px=0.0,
    )


def run_live_engine_tick(
    runner: WindowRunner,
    *,
    poly: ReplayPoly,
    clob: ReplayClob,
    cfg: KngtopConfig,
    elapsed: float,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    start = runner.start_sec()
    if start is None:
        raise ValueError("runner slug must include window start timestamp")
    state = runtime_state if runtime_state is not None else clob.runtime_state(runner.contract.slug)
    with patch("kngtop.live_limit_engine.datetime") as fake_dt:
        fake_dt.now.return_value = datetime.fromtimestamp(start + float(elapsed), timezone.utc)
        _tick_runner(runner, poly=poly, clob=clob, cfg=cfg, runtime_state=state)


def _side_for_token(token_id: str) -> str:
    return "UP" if token_id == UP_TOKEN_ID else "DOWN"


def simulate_window_with_live_engine(window: WindowData, cfg: KngtopConfig | None = None) -> dict[str, object]:
    c = cfg or replay_cfg()
    runner = runner_for_window(window)
    poly = ReplayPoly()
    clob = ReplayClob()
    max_effective_gap = 0.0
    danger_orders = 0

    for tick in window.ticks:
        poly.set_prices(up=tick.up_price, down=tick.down_price)
        clob.process_fills(up_price=tick.up_price, down_price=tick.down_price, elapsed=tick.elapsed_sec)
        before_calls = len(clob.limit_calls)
        run_live_engine_tick(runner, poly=poly, clob=clob, cfg=c, elapsed=tick.elapsed_sec)
        pos = runner.positions
        eff_up = pos.shares("UP") + sum(float(row["size_left"]) for row in clob.open_orders if row["asset_id"] == UP_TOKEN_ID)
        eff_down = pos.shares("DOWN") + sum(float(row["size_left"]) for row in clob.open_orders if row["asset_id"] == DOWN_TOKEN_ID)
        max_effective_gap = max(max_effective_gap, abs(eff_up - eff_down))
        for token_id, _price, _shares in clob.limit_calls[before_calls:]:
            side = _side_for_token(token_id)
            other = "DOWN" if side == "UP" else "UP"
            if pos.shares(side) > pos.shares(other) + 1e-12:
                danger_orders += 1

    up_shares = sum(fill.shares for fill in clob.fills if fill.side == "UP")
    down_shares = sum(fill.shares for fill in clob.fills if fill.side == "DOWN")
    cost = sum(fill.price * fill.shares for fill in clob.fills)
    fee = sum(fill.shares * FEE_BUFFER_PER_TRADE for fill in clob.fills)
    payout = up_shares if window.final_result == "UP" else down_shares
    avg_up = (sum(fill.price * fill.shares for fill in clob.fills if fill.side == "UP") / up_shares) if up_shares else None
    avg_down = (sum(fill.price * fill.shares for fill in clob.fills if fill.side == "DOWN") / down_shares) if down_shares else None
    return {
        "window_id": window.window_id,
        "final_result": window.final_result,
        "orders_placed": len(clob.limit_calls),
        "orders_filled": len(clob.fills),
        "orders_cancelled": len(clob.cancelled),
        "up_shares": up_shares,
        "down_shares": down_shares,
        "avg_up": avg_up,
        "avg_down": avg_down,
        "avg_sum": (avg_up + avg_down) if avg_up is not None and avg_down is not None else None,
        "cost": cost,
        "net_pnl": payout - cost - fee,
        "balanced_fills": abs(up_shares - down_shares) < 1e-12,
        "max_effective_gap": max_effective_gap,
        "danger_orders": danger_orders,
        "open_orders_left": len(clob.open_orders),
    }


def summarize(rows: list[dict[str, object]]) -> dict[str, object]:
    traded = [row for row in rows if float(row["orders_filled"]) > 0]
    balanced = [row for row in traded if bool(row["balanced_fills"])]
    net_total = sum(float(row["net_pnl"]) for row in rows)
    return {
        "windows": len(rows),
        "traded": len(traded),
        "trade_rate_pct": 100.0 * len(traded) / len(rows) if rows else 0.0,
        "net_total": net_total,
        "net_avg_window": net_total / len(rows) if rows else 0.0,
        "net_avg_traded": net_total / len(traded) if traded else 0.0,
        "win_rate_pct": 100.0 * sum(1 for row in rows if float(row["net_pnl"]) > 0) / len(rows) if rows else 0.0,
        "orders_placed": sum(int(row["orders_placed"]) for row in rows),
        "orders_filled": sum(int(row["orders_filled"]) for row in rows),
        "orders_cancelled": sum(int(row["orders_cancelled"]) for row in rows),
        "balanced_traded_pct": 100.0 * len(balanced) / len(traded) if traded else 0.0,
        "avg_max_effective_gap": mean(float(row["max_effective_gap"]) for row in rows) if rows else 0.0,
        "max_effective_gap": max((float(row["max_effective_gap"]) for row in rows), default=0.0),
        "danger_orders": sum(int(row["danger_orders"]) for row in rows),
        "open_orders_left": sum(int(row["open_orders_left"]) for row in rows),
        "best": max((float(row["net_pnl"]) for row in rows), default=0.0),
        "worst": min((float(row["net_pnl"]) for row in rows), default=0.0),
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def run_replay(input_dir: Path, out_dir: Path, sample_sizes: tuple[int, ...]) -> list[dict[str, object]]:
    windows = load_windows(str(input_dir))
    summaries: list[dict[str, object]] = []
    for size in sample_sizes:
        rows = [simulate_window_with_live_engine(window) for window in windows[:size]]
        write_csv(out_dir / f"current_balanced_fixed_47c_{size}_details.csv", rows)
        summary = summarize(rows)
        write_csv(out_dir / f"current_balanced_fixed_47c_{size}_summary.csv", [summary])
        summaries.append(summary)
    return summaries


def _parse_sample_sizes(raw: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in raw.split(",") if part.strip())


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    default_input = repo_root.parent / "kng_bot3" / "exports" / "window_price_snapshots_public" / "btc_5m"
    parser = argparse.ArgumentParser(description="Replay current live limit engine on recorded per-second PM windows.")
    parser.add_argument("--input-dir", type=Path, default=default_input)
    parser.add_argument("--out-dir", type=Path, default=repo_root / "reports")
    parser.add_argument("--sample-sizes", default="100,200")
    args = parser.parse_args()

    for summary in run_replay(args.input_dir, args.out_dir, _parse_sample_sizes(args.sample_sizes)):
        print(f"N={summary['windows']}")
        for key, value in summary.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
