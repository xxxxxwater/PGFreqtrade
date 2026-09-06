"""PM order ownership survives fills, REST recovery and process restarts.

Order identity is (canonical contract, exchange order id), never a bare id or
membership in ``open_orders``. Unresolved own events are durable incidents, not
an instruction to import a foreign position or to submit another order.
"""

import json
import logging
from contextlib import nullcontext
from enum import StrEnum
from functools import wraps
from math import isfinite
from typing import Any

from freqtrade.enums import RPCMessageType
from freqtrade.exceptions import OperationalException
from freqtrade.exchange.pm_identity import canonical_pm_pair
from freqtrade.persistence import Order, Trade
from freqtrade.persistence.pm_order_intent import PMOrderIntent
from freqtrade.persistence.pm_outbox import PMOutbox
from freqtrade.persistence.pm_stream_journal import PMStreamJournal


logger = logging.getLogger(__name__)


class PMOwnershipClass(StrEnum):
    KNOWN_LATE = "KNOWN_LATE"
    KNOWN_DUPLICATE = "KNOWN_DUPLICATE"
    RECOVERABLE_UNKNOWN = "RECOVERABLE_UNKNOWN"
    EXTERNAL_ORDER = "EXTERNAL_ORDER"
    FRAMEWORK_ORPHAN = "FRAMEWORK_ORPHAN"
    UNEXPLAINED_EXPOSURE = "UNEXPLAINED_EXPOSURE"


def pm_order_locked(func):
    """Use the bot's lifecycle lock, not a second oppositely ordered lock.

    The live bot uses an RLock so recovery/stream/nested exit operations can
    share one ordering. This is process-local; cross-process exclusion remains
    the deployment/account lock and is not claimed to be solved by this lock.
    """

    @wraps(func)
    def locked(self, *args, **kwargs):
        with getattr(self, "_exit_lock", nullcontext()):
            return func(self, *args, **kwargs)

    return locked


class PMOrderOwnershipMixin:
    def _pm_canonical_pair(self, pair: str, namespace: str = "um") -> str:
        return canonical_pm_pair(self.exchange.markets, pair, namespace=namespace)

    def _pm_owned_order(  # noqa: C901 - ordered, evidence-ranked lookup chain
        self, pair: str, order_id: str, client_id: str, index: dict
    ):
        """Find exactly one owned Order, including terminal orders/closed Trades.

        The outbox retains the client-id link after an intent was reconciled.
        An ACK without a committed Order is NOT sufficient ownership to adopt it.
        """
        cache = getattr(self, "_pm_owned_resolve_cache", None)
        cache_key = (pair, order_id, client_id)
        if cache is not None and cache_key in cache:
            return cache[cache_key]
        matches = {}
        identifiers = {order_id, client_id} - {"", "None"}
        for identifier in identifiers:
            entry = index.get((pair, identifier))
            # Compatibility with callers supplying the old transient index:
            # validate the instrument even for these entries.
            entry = entry or index.get(identifier)
            if entry and self._pm_canonical_pair(entry[0].pair) == pair:
                matches[(entry[0].id, str(entry[1].order_id))] = entry
        if self._pm_db_gate_active():
            alias = PMStreamJournal.get(pair, order_id)
            if alias and alias.linked_order_pk:
                order = Order.session.get(Order, alias.linked_order_pk)
                if order is None or self._pm_canonical_pair(order.ft_pair) != pair:
                    raise OperationalException("Persistent child-order ownership conflict")
                matches[(order.ft_trade_id, str(order.order_id))] = (order._trade_live, order)
            rows = Order.session.query(Order).filter(Order.order_id.in_(identifiers)).all()
            for order in rows:
                if self._pm_canonical_pair(order.ft_pair) != pair:
                    continue
                trade = order._trade_live
                if trade and trade.exchange.lower() == "binance":
                    if self._pm_canonical_pair(trade.pair) != pair:
                        raise OperationalException("Order/Trade instrument ownership conflict")
                    matches[(trade.id, str(order.order_id))] = (trade, order)
            for order in self._pm_client_owned_orders(pair, order_id, client_id):
                matches[(order.ft_trade_id, str(order.order_id))] = (order._trade_live, order)
        if len(matches) > 1:
            raise OperationalException("Ambiguous PM order ownership")
        result = next(iter(matches.values()), None)
        # A redelivery storm (the live DASH incident replayed ~350 events) must
        # not re-run the terminal-order/outbox queries per message. Only proven
        # ownership is cached; misses keep retrying so an order linked later in
        # the same batch is still found.
        if cache is not None and result is not None:
            cache[cache_key] = result
        return result

    def _pm_client_owned_orders(self, pair: str, order_id: str, client_id: str) -> list:
        if not client_id:
            return []
        evidence = PMOutbox.get_by_client_id(client_id) or PMOrderIntent.get_by_client_id(client_id)
        if evidence is None:
            return []
        if isinstance(evidence, PMOutbox):
            payload = json.loads(evidence.payload or "{}")
            # The real PAPI path persists the full exchange request, which
            # carries the raw symbol id ("DASHUSDT") and never a "pair" key.
            # Resolve it in the PM namespace; when it cannot be resolved the
            # linked local Order rows below remain the authoritative ownership
            # proof (client ids are unique) - an unreadable payload must never
            # turn a known order into a FAIL-CLOSED incident.
            evidence_pair = (
                payload.get("pair") or payload.get("symbol") if isinstance(payload, dict) else None
            )
        else:
            evidence_pair = evidence.pair
        if evidence_pair:
            try:
                resolved_pair = self._pm_canonical_pair(evidence_pair)
            except OperationalException:
                resolved_pair = None
            if resolved_pair is not None and resolved_pair != pair:
                raise OperationalException("Client-id journal instrument conflict")
        if not evidence.linked_order_id or not evidence.linked_trade_id:
            return []
        linked = (
            Order.session.query(Order)
            .filter(
                Order.order_id == evidence.linked_order_id,
                Order.ft_trade_id == evidence.linked_trade_id,
            )
            .all()
        )
        operation = evidence.operation if isinstance(evidence, PMOutbox) else evidence.kind
        for order in linked:
            if self._pm_canonical_pair(order.ft_pair) != pair:
                raise OperationalException("Client-id local instrument conflict")
            if operation == "order" and order_id and str(order.order_id) != order_id:
                raise OperationalException("Client-id/exchange order-id conflict")
        return linked

    @staticmethod
    def _pm_event_is_replay(order, data: dict) -> bool:
        """Terminal evidence can absorb older events, never a new cumulative fill.

        Missing/malformed quantities are not evidence of a harmless replay.
        A canceled partial order followed by FILLED must be REST reconciled.
        """
        if order.ft_is_open:
            return False
        try:
            cumulative = float(data["z"])
            local_filled = float(order.filled or 0)
        except (KeyError, TypeError, ValueError):
            return False
        if (
            not isfinite(local_filled)
            or not isfinite(cumulative)
            or cumulative < 0
            or cumulative > local_filled + 1e-12
        ):
            return False
        status = str(order.status).lower()
        incoming = str(data.get("X", "")).upper()
        if incoming in {"NEW", "PARTIALLY_FILLED"}:
            return status in {"closed", "canceled", "cancelled", "expired", "rejected"}
        equivalent = {
            "FILLED": {"closed"},
            "CANCELED": {"canceled", "cancelled"},
            "EXPIRED": {"expired"},
            "REJECTED": {"rejected"},
        }
        return status in equivalent.get(incoming, set())

    def _pm_validate_rest_ownership(self, trade, order, response: dict) -> None:
        """REST is authoritative only after identity and monotonicity validation."""
        pair = self._pm_canonical_pair(trade.pair)
        if not response or self._pm_canonical_pair(response.get("symbol") or trade.pair) != pair:
            raise OperationalException("PM REST response instrument mismatch")
        if str(response.get("id") or "") != str(order.order_id):
            raise OperationalException("PM REST response order-id mismatch")
        if response.get("status") not in {"open", "closed", "canceled", "expired", "rejected"}:
            raise OperationalException("PM REST response has unknown order state")
        if response.get("filled") is None or not isfinite(float(response["filled"])):
            raise OperationalException("PM REST missing finite cumulative fill")
        if not order.ft_is_open:
            if order.status == "closed" and response.get("status") != "closed":
                raise OperationalException("PM REST would regress a filled terminal order")
            if response.get("status") == "open":
                raise OperationalException("PM REST would regress a terminal order to open")
            if float(response.get("filled") or 0) < float(order.filled or 0) - 1e-12:
                raise OperationalException("PM REST would regress cumulative filled quantity")

    def _pm_finish_stream_incident(self, pair: str, order_id: str, order) -> None:
        key = (pair, order_id)
        clean = getattr(self, "_pm_clean_terminal_ids", None)
        if self._pm_db_gate_active() and (clean is None or key not in clean):
            incident = PMStreamJournal.get(pair, order_id)
            if incident and incident.unresolved:
                if float(order.filled or 0) < float(incident.max_cumulative_filled or 0):
                    raise OperationalException("Local order is behind durable stream fill evidence")
                if incident.saw_filled and order.status != "closed":
                    raise OperationalException(
                        "Local order has not confirmed durable terminal fill"
                    )
            # resolve() re-reads the row; skip when no row exists so a
            # redelivery storm does not double the journal queries.
            if incident is not None:
                PMStreamJournal.resolve(pair, order_id, order.id)
        self._pm_unmatched_stream_order_ids.discard(key)
        # Remember clean terminal ownership so a 10k-event redelivery storm
        # does not re-query the journal per message. _pm_note_stream_incident
        # invalidates the entry whenever a new incident is recorded.
        if clean is not None:
            if len(clean) >= 4096:
                clean.clear()
            clean.add(key)

    def _pm_remember_child_ownership(self, pair: str, actual_id: str, strategy_id: str) -> None:
        if not self._pm_db_gate_active():
            return
        rows = Order.session.query(Order).filter(Order.order_id == strategy_id).all()
        rows = [o for o in rows if self._pm_canonical_pair(o.ft_pair) == pair]
        if len(rows) != 1:
            raise OperationalException("Cannot persist ambiguous conditional child ownership")
        existing = PMStreamJournal.get(pair, actual_id)
        if existing and existing.linked_order_pk == rows[0].id and not existing.unresolved:
            return
        # Preserve an existing incident's event evidence; merely remember its
        # ownership here. Only recovery may close an unresolved incident.
        if existing:
            return
        PMStreamJournal.record_unresolved(
            pair, actual_id, "", {"i": actual_id}, "conditional child ownership alias"
        )
        PMStreamJournal.resolve(pair, actual_id, rows[0].id)

    def _pm_unowned_classification(self, client_id: str) -> PMOwnershipClass:
        if self._pm_db_gate_active() and client_id:
            evidence = PMOutbox.get_by_client_id(client_id) or PMOrderIntent.get_by_client_id(
                client_id
            )
            if evidence and evidence.exchange_order_id:
                return PMOwnershipClass.FRAMEWORK_ORPHAN
        return PMOwnershipClass.RECOVERABLE_UNKNOWN

    def _pm_note_stream_incident(self, pair: str, order_id: str, data: dict, reason: str) -> None:
        """Persist and notify once per unresolved instrument/order, not per fill."""
        self._pm_init_user_stream_state()
        key = (pair, order_id)
        first = key not in self._pm_unmatched_stream_order_ids
        self._pm_unmatched_stream_order_ids.add(key)
        # New evidence invalidates any cached "clean terminal" verdict for the
        # key: the incident must be re-verified against the journal.
        getattr(self, "_pm_clean_terminal_ids", set()).discard(key)
        # The ownership cache is keyed (pair, order_id, client_id): a new
        # incident for this instrument/order must invalidate EVERY client-key
        # under it, not just a (pair, order_id) entry that never exists.
        cache = getattr(self, "_pm_owned_resolve_cache", None)
        if cache:
            stale = [k for k in cache if k[0] == pair and k[1] == order_id]
            for stale_key in stale:
                cache.pop(stale_key, None)
        self._pm_block_orders("unmatched_stream_order")
        if self._pm_db_gate_active():
            try:
                _, first = PMStreamJournal.record_unresolved(
                    pair, order_id, str(data.get("c") or ""), data, reason
                )
                Trade.commit()
            except Exception:
                Trade.session.rollback()
                self._pm_block_orders("stream_journal_unavailable")
                logger.exception("Could not persist PM stream ownership incident")
        if first:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": f"PM User Stream FAIL-CLOSED: orderId {order_id} "
                    f"(clientId {data.get('c', '')}) on {pair}: {reason}. "
                    "New exposure BLOCKED until ownership reconciliation succeeds.",
                }
            )
            if (
                self.config.get("exchange", {})
                .get("portfolio_margin_risk", {})
                .get("user_stream_recover_unmatched_orders", True)
            ):
                # Targeted incident recovery first (bounded: REST only when
                # ownership is provable). A burst of DISTINCT unknown orders
                # must not multiply into N full account sweeps, so the full
                # sweep is rate-limited by a configurable cooldown; the
                # scheduled recovery covers it afterwards.
                try:
                    self._pm_recover_stream_incidents()
                except Exception:
                    logger.exception("PM targeted stream incident recovery failed")
                if self._pm_auto_recovery_due():
                    self._pm_order_recovery()

    @pm_order_locked
    def _pm_recover_stream_incidents(self) -> dict[str, Any]:
        """Resolve the exact persisted incident; zero open orders proves nothing."""
        report: dict[str, Any] = {"checked": 0, "resolved": 0, "unresolved": 0, "errors": []}
        if not self._pm_db_gate_active():
            report["unresolved"] = len(getattr(self, "_pm_unmatched_stream_order_ids", []))
            return report
        try:
            incidents = PMStreamJournal.get_unresolved()
            for incident in incidents:
                report["checked"] += 1
                try:
                    data = json.loads(incident.order_data)
                    entry = self._pm_owned_order(
                        incident.pair, incident.exchange_order_id, incident.client_id, {}
                    )
                    if entry is None:
                        entry = self._pm_probe_child_owner(
                            incident.pair, incident.exchange_order_id
                        )
                    if entry is None:
                        report["unresolved"] += 1
                        continue
                    self._pm_reconcile_one_incident(incident, entry, data)
                    Trade.commit()
                    report["resolved"] += 1
                except Exception as exc:
                    Trade.session.rollback()
                    report["unresolved"] += 1
                    report["errors"].append(f"{incident.pair}/{incident.exchange_order_id}: {exc}")
            Trade.commit()
            self._pm_unblock_orders("stream_journal_unavailable")
        except Exception as exc:
            Trade.session.rollback()
            report["errors"].append(f"stream journal unavailable: {exc}")
            self._pm_block_orders("stream_journal_unavailable")
        # Generation-safe release: the decision re-reads the authoritative
        # durable state instead of trusting the snapshot taken before the loop.
        # An incident recorded between the snapshot and this point keeps the
        # gate; resolving the snapshot alone can never clear it.
        pending = PMStreamJournal.get_unresolved() if self._pm_db_gate_active() else []
        memory_pending = getattr(self, "_pm_unmatched_stream_order_ids", set())
        if self._pm_db_gate_active():
            # In-process keys normally mirror the journal; count only keys that
            # have no journal row (recorded while the journal was unreadable).
            pending_keys = {(row.pair, row.exchange_order_id) for row in pending}
            extra_memory = memory_pending - pending_keys
        else:
            extra_memory = memory_pending
        if report["errors"] or pending or extra_memory:
            self._pm_block_orders("unmatched_stream_order")
            report["unresolved"] = len(pending) + len(extra_memory)
        else:
            self._pm_unblock_orders("unmatched_stream_order")
        return report

    def _pm_probe_child_owner(self, pair: str, order_id: str):
        """Bounded scheduled recovery of a child whose first history read failed."""
        matches = []
        for trade in Trade.get_open_trades():
            if self._pm_canonical_pair(trade.pair) != pair:
                continue
            for order in trade.open_sl_orders:
                response = self.exchange.fetch_stoploss_order(order.order_id, trade.pair)
                self._pm_validate_rest_ownership(trade, order, response)
                if str(response.get("id_stop") or "") == order_id:
                    self._pm_record_actual_order(response, order.order_id, pair)
                    matches.append((trade, order))
        if len(matches) > 1:
            raise OperationalException("Ambiguous conditional child ownership")
        return next(iter(matches), None)

    def _pm_reconcile_one_incident(self, incident, entry: tuple, data: dict) -> None:
        trade, order = entry
        # Even an apparently old event requires fresh REST before gate release.
        response = (
            self.exchange.fetch_stoploss_order(order.order_id, trade.pair)
            if order.ft_order_side == "stoploss"
            else self.exchange.fetch_order(order.order_id, trade.pair)
        )
        self._pm_validate_rest_ownership(trade, order, response)
        if float(response.get("filled") or 0) < float(incident.max_cumulative_filled or 0):
            raise OperationalException("REST has not caught up to stream fill")
        if incident.saw_filled and response.get("status") != "closed":
            raise OperationalException("REST has not confirmed observed terminal fill")
        if order.ft_is_open or not self._pm_event_is_replay(order, data):
            self.update_trade_state(
                trade, order.order_id, response, stoploss_order=order.ft_order_side == "stoploss"
            )
        if order.ft_is_open != (response.get("status") == "open"):
            raise OperationalException("Local lifecycle update did not finish")
        if abs(float(order.filled or 0) - float(response.get("filled") or 0)) > 1e-12:
            raise OperationalException("Local filled quantity is not reconciled")
        self._pm_finish_stream_incident(incident.pair, incident.exchange_order_id, order)
