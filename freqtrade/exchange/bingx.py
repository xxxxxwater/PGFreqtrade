"""BingX exchange subclass"""

import logging
from datetime import datetime

import ccxt

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, PriceType, TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    ExchangeError,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import FtHas


logger = logging.getLogger(__name__)


class Bingx(Exchange):
    """
    BingX exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "ohlcv_candle_limit": 1000,
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit", "market": "market"},
        "order_time_in_force": ["GTC", "IOC", "PO"],
        "trades_has_history": False,  # Endpoint doesn't seem to support pagination
    }
    _ft_has_futures: FtHas = {
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "stoploss_blocks_assets": False,
        "stop_price_prop": "stopPrice",
        "stop_price_type_field": "workingType",
        "stop_price_type_value_mapping": {
            PriceType.LAST: "CONTRACT_PRICE",
            PriceType.MARK: "MARK_PRICE",
        },
        "funding_fee_candle_limit": 1000,
        "order_time_in_force": ["GTC", "FOK", "IOC", "PO"],
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    @retrier
    def additional_exchange_init(self) -> None:
        """
        Additional exchange initialization logic.
        .api will be available at this point.
        Must be overridden in child methods if required.
        """
        try:
            if not self._config["dry_run"] and self.trading_mode == TradingMode.FUTURES:
                position_mode = self._api.set_position_mode(False)
                self._log_exchange_response("set_position_mode", position_mode)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.FUTURES or self.margin_mode is None:
            return
        self.set_margin_mode(pair, self.margin_mode, accept_fail=True)
        self._set_leverage(leverage, pair, accept_fail=accept_fail)

    @retrier
    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        """
        BingX requires an explicit `side` when setting futures leverage.
        Freqtrade operates in one-way mode, so we always use BOTH.
        """
        if self._config["dry_run"] or not self.exchange_has("setLeverage"):
            return
        try:
            res = self._api.set_leverage(leverage=leverage, symbol=pair, params={"side": "BOTH"})
            self._log_exchange_response("set_leverage", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        BingX funding-fee accounting is derived from funding-rate and mark-price history.
        This keeps dry-run and live behavior aligned even when the exchange does not expose
        settled funding-fee history per position via ccxt.
        """
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}.")
        return 0.0

    def fetch_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ):
        """
        BingX futures stop orders are exposed separately from regular orders.
        Add the stop flag explicitly so ccxt can route this through the algo-order path.
        """
        params = params.copy() if params else {}
        params.setdefault("stop", True)
        return self.fetch_order(order_id, pair, params)

    def cancel_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> dict:
        """
        Cancel stoploss orders through the stop-order path instead of the regular order endpoint.
        """
        params = params.copy() if params else {}
        params.setdefault("stop", True)
        return self.cancel_order(order_id, pair, params)

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,
        open_trades: list,
    ) -> float | None:
        """
        Estimated BingX liquidation price for USDⓢ-M futures.

        Isolated mode follows BingX's published formula:
            long  = entry - (initial_margin - maintenance_margin) / size
            short = entry + (initial_margin - maintenance_margin) / size

        Cross mode solves the account risk equation using wallet balance plus the unrealized PnL
        and maintenance margin of the other cross positions in the same wallet.
        """
        market = self.markets[pair]
        mm_ratio, maintenance_amt = self.get_maintenance_ratio_and_amt(pair, stake_amount)
        maintenance_amt = maintenance_amt or 0.0

        if self.trading_mode != TradingMode.FUTURES:
            raise OperationalException("Freqtrade only supports isolated futures for leverage trading")
        if market["inverse"]:
            raise OperationalException("Freqtrade does not yet support inverse contracts")

        if self.margin_mode == MarginMode.ISOLATED:
            position_value = amount * open_rate
            initial_margin = position_value / leverage
            maintenance_margin = position_value * mm_ratio - maintenance_amt
            margin_diff_per_contract = (initial_margin - maintenance_margin) / amount
            return open_rate + margin_diff_per_contract if is_short else open_rate - margin_diff_per_contract

        if self.margin_mode == MarginMode.CROSS:
            cross_vars = 0.0
            if open_trades:
                pairs = [trade.pair for trade in open_trades]
                if self._config["runmode"] in ("live", "dry_run"):
                    funding_rates = self.fetch_funding_rates(pairs)
                for trade in open_trades:
                    if trade.pair == pair:
                        continue
                    if self._config["runmode"] in ("live", "dry_run"):
                        mark_price = funding_rates[trade.pair]["markPrice"]
                    else:
                        mark_price = trade.open_rate
                    trade_mm_ratio, trade_maint_amt = self.get_maintenance_ratio_and_amt(
                        trade.pair, trade.stake_amount
                    )
                    trade_maint_margin = (
                        trade.amount * mark_price * trade_mm_ratio - (trade_maint_amt or 0.0)
                    )
                    cross_vars += (trade.amount * mark_price - trade.amount * trade.open_rate) - (
                        trade_maint_margin
                    )

            side_1 = -1 if is_short else 1
            return (
                (wallet_balance + cross_vars + maintenance_amt) - (side_1 * amount * open_rate)
            ) / ((amount * mm_ratio) - (side_1 * amount))

        raise OperationalException("Freqtrade only supports cross or isolated margin for BingX")
