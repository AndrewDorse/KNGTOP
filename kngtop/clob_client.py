"""Polymarket CLOB — py_clob_client_v2 market buys (KNG4 ``prst1/clob_shim``)."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from kngtop.gamma import TokenMarket

LOGGER = logging.getLogger("kngtop")

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

from py_clob_client_v2 import (  # noqa: E402
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
    TradeParams,
)
from py_clob_client_v2.clob_types import OrderPayload  # noqa: E402


def _norm_tick(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    return s if s in {"0.1", "0.01", "0.001", "0.0001"} else None


def _maybe_float(raw: object) -> float | None:
    try:
        if raw is None:
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


def _normalize_usdc_balance(raw: object) -> float | None:
    val = _maybe_float(raw)
    if val is None:
        return None
    # Polymarket collateral balances may arrive in 6-decimal base units.
    if float(val).is_integer() and val >= 100_000:
        return max(0.0, val / 1_000_000.0)
    return max(0.0, val)


class KngtopClob:
    def __init__(
        self,
        *,
        private_key: str,
        funder: str,
        signature_type: int,
        relayer_api_key: str,
        relayer_secret: str,
        relayer_passphrase: str,
        market_buy_max_price: float = 0.85,
    ) -> None:
        self._signature_type = int(signature_type)
        self._buy = Side.BUY
        self._sell = Side.SELL
        self._market_buy_max_price = float(market_buy_max_price)
        self._taker_lock = threading.Lock()
        t0 = time.perf_counter()
        self.client = ClobClient(
            HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=signature_type,
            funder=funder,
        )
        if relayer_api_key:
            self.client.set_api_creds(
                ApiCreds(
                    api_key=relayer_api_key,
                    api_secret=relayer_secret or "",
                    api_passphrase=relayer_passphrase,
                )
            )
        else:
            creds = self.client.derive_api_key()
            if creds is None:
                creds = self.client.create_api_key(int(time.time() * 1000))
            self.client.set_api_creds(creds)
        try:
            t1 = time.perf_counter()
            self.client.update_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=signature_type,
                )
            )
            LOGGER.debug("Allowance sync elapsed_ms=%.1f", (time.perf_counter() - t1) * 1000.0)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Collateral allowance sync: %s", exc)
        try:
            resolve_version = getattr(self.client, "_ClobClient__resolve_version", None)
            if callable(resolve_version):
                resolve_version()
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Version prewarm failed: %s", exc)
        LOGGER.debug("clob_client_ready elapsed_ms=%.1f", (time.perf_counter() - t0) * 1000.0)

    def available_balance_usdc(self) -> float | None:
        t0 = time.perf_counter()
        try:
            payload = self.client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=self._signature_type,
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Balance fetch failed: %s", exc)
            return None

        if not isinstance(payload, dict):
            return None

        direct_keys = (
            "available",
            "available_balance",
            "balance",
            "availableBalance",
            "balanceAvailable",
        )
        for key in direct_keys:
            val = _normalize_usdc_balance(payload.get(key))
            if val is not None:
                return val

        nested = payload.get("balanceAllowance")
        if isinstance(nested, dict):
            for key in direct_keys:
                val = _normalize_usdc_balance(nested.get(key))
                if val is not None:
                    return val
        return None

    def _book_opts(self, token: TokenMarket) -> PartialCreateOrderOptions | None:
        tid = token.token_id
        tick = None
        neg = None
        try:
            tick = _norm_tick(self.client.get_tick_size(tid))
        except Exception:
            tick = _norm_tick(token.minimum_tick_size)
        try:
            neg = bool(self.client.get_neg_risk(tid))
        except Exception:
            neg = token.neg_risk
        if tick is None and neg is None:
            return None
        return PartialCreateOrderOptions(
            tick_size=tick,
            neg_risk=bool(neg) if neg is not None else None,
        )

    def prewarm_market_metadata(self, token: TokenMarket) -> None:
        tid = (token.token_id or "").strip()
        if not tid:
            return
        try:
            ensure_market_info = getattr(self.client, "_ClobClient__ensure_market_info_cached", None)
            if callable(ensure_market_info):
                ensure_market_info(tid)
            resolve_version = getattr(self.client, "_ClobClient__resolve_version", None)
            if callable(resolve_version):
                resolve_version()
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Token metadata prewarm failed for %s: %s", tid[:16], exc)

    def _create_and_post_market_order(
        self, margs: MarketOrderArgs, options: PartialCreateOrderOptions | None
    ) -> dict[str, Any]:
        create_and_post = getattr(self.client, "create_and_post_market_order", None)
        if not callable(create_and_post):
            raise RuntimeError("ClobClient.create_and_post_market_order missing (py_clob_client_v2)")
        ot = margs.order_type or OrderType.FAK
        try:
            return create_and_post(margs, options=options, order_type=ot)
        except TypeError:
            return create_and_post(margs, options=options)

    def market_buy_usdc(self, token: TokenMarket, usdc: float, *, max_price: float | None = None) -> dict[str, Any]:
        u = float(usdc)
        if u <= 0:
            raise ValueError("usdc must be > 0")
        opts = self._book_opts(token)
        tick_raw = (
            _norm_tick(self.client.get_tick_size(token.token_id))
            if opts is None or opts.tick_size is None
            else _norm_tick(opts.tick_size)
        )
        tick_f = float(tick_raw or "0.01")
        hi = 1.0 - tick_f
        # Per-share ceiling for FAK; clamp to valid (tick, 1-tick).
        raw_price_cap = self._market_buy_max_price if max_price is None else float(max_price)
        price_cap = min(max(raw_price_cap, tick_f), hi)
        margs = MarketOrderArgs(
            token_id=token.token_id,
            amount=u,
            side=self._buy,
            price=price_cap,
            order_type=OrderType.FAK,
        )
        with self._taker_lock:
            return self._create_and_post_market_order(margs, options=opts)

    def market_buy_shares_fak(
        self, token: TokenMarket, *, shares: float, max_price: float | None = None
    ) -> dict[str, Any]:
        sz = float(shares)
        if sz <= 0:
            raise ValueError("shares must be > 0")
        opts = self._book_opts(token)
        tick_raw = (
            _norm_tick(self.client.get_tick_size(token.token_id))
            if opts is None or opts.tick_size is None
            else _norm_tick(opts.tick_size)
        )
        tick_f = float(tick_raw or "0.01")
        hi = 1.0 - tick_f
        raw_price_cap = self._market_buy_max_price if max_price is None else float(max_price)
        price_cap = min(max(raw_price_cap, tick_f), hi)
        order = OrderArgs(
            token_id=token.token_id,
            price=round(price_cap, 2),
            size=sz,
            side=self._buy,
        )
        create_and_post = getattr(self.client, "create_and_post_order", None)
        with self._taker_lock:
            if callable(create_and_post):
                try:
                    return create_and_post(order_args=order, options=None, order_type=OrderType.FAK, post_only=False)
                except TypeError:
                    return create_and_post(order, None, OrderType.FAK)
            signed = self.client.create_order(order)
            return self.client.post_order(signed, OrderType.FAK)

    def limit_buy(self, token: TokenMarket, *, price: float, usdc: float) -> dict[str, Any]:
        u = float(usdc)
        px = float(price)
        if u <= 0:
            raise ValueError("usdc must be > 0")
        if px <= 0 or px >= 1:
            raise ValueError("price must be between 0 and 1")
        size = u / px
        order = OrderArgs(
            token_id=token.token_id,
            price=round(px, 2),
            size=float(size),
            side=self._buy,
        )
        create_and_post = getattr(self.client, "create_and_post_order", None)
        if callable(create_and_post):
            try:
                return create_and_post(order_args=order, options=None, order_type=OrderType.GTC, post_only=False)
            except TypeError:
                return create_and_post(order, None, OrderType.GTC)
        signed = self.client.create_order(order)
        return self.client.post_order(signed, OrderType.GTC)

    def limit_buy_shares(self, token: TokenMarket, *, price: float, shares: float) -> dict[str, Any]:
        sz = float(shares)
        px = float(price)
        if sz <= 0:
            raise ValueError("shares must be > 0")
        if px <= 0 or px >= 1:
            raise ValueError("price must be between 0 and 1")
        order = OrderArgs(
            token_id=token.token_id,
            price=round(px, 2),
            size=sz,
            side=self._buy,
        )
        create_and_post = getattr(self.client, "create_and_post_order", None)
        if callable(create_and_post):
            try:
                return create_and_post(order_args=order, options=None, order_type=OrderType.GTC, post_only=False)
            except TypeError:
                return create_and_post(order, None, OrderType.GTC)
        signed = self.client.create_order(order)
        return self.client.post_order(signed, OrderType.GTC)

    def cancel_order_by_id(self, order_id: str) -> dict[str, Any]:
        payload = self.client.cancel_order(OrderPayload(orderID=str(order_id)))
        return payload if isinstance(payload, dict) else {}

    def limit_sell_shares(self, token: TokenMarket, *, price: float, shares: float) -> dict[str, Any]:
        sz = float(shares)
        px = float(price)
        if sz <= 0:
            raise ValueError("shares must be > 0")
        if px <= 0 or px >= 1:
            raise ValueError("price must be between 0 and 1")
        order = OrderArgs(
            token_id=token.token_id,
            price=round(px, 2),
            size=sz,
            side=self._sell,
        )
        create_and_post = getattr(self.client, "create_and_post_order", None)
        if callable(create_and_post):
            try:
                return create_and_post(order_args=order, options=None, order_type=OrderType.GTC, post_only=False)
            except TypeError:
                return create_and_post(order, None, OrderType.GTC)
        signed = self.client.create_order(order)
        return self.client.post_order(signed, OrderType.GTC)

    def get_recent_trades(self, token: TokenMarket, *, after_ts: int) -> list[dict[str, Any]]:
        rows = self.client.get_trades(
            TradeParams(asset_id=token.token_id, after=int(after_ts)),
            only_first_page=True,
        )
        return [row for row in rows if isinstance(row, dict)]

    def get_open_orders_for_asset(self, token: TokenMarket) -> list[dict[str, Any]]:
        rows = self.client.get_open_orders(
            OpenOrderParams(asset_id=token.token_id),
            only_first_page=True,
        )
        return [row for row in rows if isinstance(row, dict)]

    def get_order(self, order_id: str) -> dict[str, Any]:
        payload = self.client.get_order(order_id)
        return payload if isinstance(payload, dict) else {}

    def is_order_open_for_asset(self, token: TokenMarket, order_id: str) -> bool:
        oid = str(order_id)
        for row in self.get_open_orders_for_asset(token):
            row_id = str(row.get("id") or row.get("orderID") or row.get("order_id") or "")
            if row_id == oid:
                return True
        return False
