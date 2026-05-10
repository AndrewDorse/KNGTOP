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
    OrderType,
    PartialCreateOrderOptions,
    Side,
)


def _norm_tick(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    return s if s in {"0.1", "0.01", "0.001", "0.0001"} else None


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
        self._market_buy_max_price = float(market_buy_max_price)
        self._taker_lock = threading.Lock()
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
            self.client.update_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=signature_type,
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Collateral allowance sync: %s", exc)

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

    def market_buy_usdc(self, token: TokenMarket, usdc: float) -> dict[str, Any]:
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
        price_cap = min(max(self._market_buy_max_price, tick_f), hi)
        margs = MarketOrderArgs(
            token_id=token.token_id,
            amount=u,
            side=self._buy,
            price=price_cap,
            order_type=OrderType.FAK,
        )
        with self._taker_lock:
            return self._create_and_post_market_order(margs, options=opts)
