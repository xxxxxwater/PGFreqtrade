"""
PM order intent model.

A PM order intent is durably committed to the main freqtrade database BEFORE the
exchange POST is allowed. This is the single source of truth for order idempotency:
on timeout / disconnect / restart the SAME client id is resolved against the
exchange and a duplicate entry / DCA / reduceOnly / stoploss order can never be
submitted.

Lifecycle (redone for crash consistency):

    PREPARED  ->  ACKED   ->  LINKED   ->  (RECONCILED, then tombstoned)
       |             |          |
       |             |          +-- linked_order_id / linked_trade_id set in the
       |             |              SAME database transaction that commits the
       |             |              local Trade/Order rows. A crash before that
       |             |              commit leaves the intent ACKED, which keeps
       |             |              new exposure blocked and preserves the
       |             |              exchange order id + raw response as evidence.
       |             |
       |             +-- exchange_order_id + raw_response stored on the ACK
       |                 (never deleted before the local commit).
       |
       +-- UNKNOWN when the POST outcome cannot be established (blocks everything
           exposure-increasing for the pair).

    * Deterministic exchange rejections delete the intent; the evidence is kept
      on the corresponding pm_outbox row (state REJECTED).
    * LINKED rows no longer block new orders; the scheduled/startup
      reconciliation verifies the exchange agrees with the local order and then
      tombstones (deletes) the row - that transition IS "RECONCILED".
    * UNKNOWN rows must block all exposure-increasing orders for their pair
      until resolved.

Failing to write / read / commit / parse this table is fail-closed: no
exposure-increasing order may be placed.
"""

from datetime import datetime
from typing import ClassVar

from sqlalchemy import Boolean, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now

# States that are not yet "safe" for exposure-increasing orders.  ``PENDING``
# is retained here only as a fail-closed compatibility guard for a database
# which has not yet completed the startup migration to ``PREPARED``.
UNRESOLVED_STATES = ("PENDING", "PREPARED", "ACKED", "UNKNOWN")
# States kept for audit / reconciliation (including legacy PENDING until it is
# normalized by ``migrate_pm_tables``).
ACTIVE_STATES = ("PENDING", "PREPARED", "ACKED", "LINKED", "UNKNOWN")


class PMOrderIntent(ModelBase):
    """
    A persisted intent to place a PM order.

    ``client_id`` is the Binance ``newClientOrderId`` (kind="order") or
    ``clientAlgoId`` (kind="conditional") and is unique.

    State lifecycle: PREPARED -> ACKED -> LINKED -> (reconciled & deleted).
    Legacy PENDING rows are migrated to PREPARED during database startup; until
    then they remain deliberately unresolved and block new exposure.
    UNKNOWN marks an unresolvable POST outcome and stays until manually or
    programmatically resolved.
    """

    __tablename__ = "pm_order_intents"

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)  # order | conditional
    pair: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8), nullable=True)
    order_type: Mapped[str | None] = mapped_column(String(24), nullable=True)
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    reduce_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PREPARED")
    # Exchange evidence stored on ACK - never lost before the local commit.
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    acked_at: Mapped[datetime | None] = mapped_column(nullable=True)
    # Local linkage, written in the SAME transaction as the Trade/Order commit.
    # Trade ownership known BEFORE POST for DCA/reduce-only orders.  This is
    # immutable evidence used by crash recovery; never infer ownership later by
    # scanning for a same-pair/same-size Trade.  Initial entries legitimately
    # have no origin Trade yet.
    origin_trade_id: Mapped[int | None] = mapped_column(nullable=True)
    linked_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    linked_trade_id: Mapped[int | None] = mapped_column(nullable=True)
    linked_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(255), nullable=True)

    @property
    def is_unresolved(self) -> bool:
        """True when the intent outcome is not yet definitively known."""
        return self.state in UNRESOLVED_STATES

    @property
    def increases_exposure(self) -> bool:
        """True when this intent would increase market exposure if it executed."""
        return not self.reduce_only

    def to_dict(self) -> dict:
        return {
            "client_id": self.client_id,
            "kind": self.kind,
            "pair": self.pair,
            "side": self.side,
            "order_type": self.order_type,
            "amount": self.amount,
            "price": self.price,
            "stop_price": self.stop_price,
            "reduce_only": self.reduce_only,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "state": self.state,
            "exchange_order_id": self.exchange_order_id,
            "raw_response": self.raw_response,
            "acked_at": self.acked_at.isoformat() if self.acked_at else None,
            "origin_trade_id": self.origin_trade_id,
            "linked_order_id": self.linked_order_id,
            "linked_trade_id": self.linked_trade_id,
            "linked_at": self.linked_at.isoformat() if self.linked_at else None,
            "last_error": self.last_error,
        }

    @classmethod
    def get_unresolved(cls) -> list["PMOrderIntent"]:
        """All intents whose outcome is not yet definitively known."""
        return list(
            cls.session.query(cls).filter(cls.state.in_(list(UNRESOLVED_STATES))).all()
        )

    @classmethod
    def get_unresolved_for_pair(cls, pair: str) -> list["PMOrderIntent"]:
        """Unresolved intents for one pair (used by reduce-only stop gating)."""
        return list(
            cls.session.query(cls)
            .filter(cls.state.in_(list(UNRESOLVED_STATES)), cls.pair == pair)
            .all()
        )

    @classmethod
    def get_unresolved_exposure_increasing(cls) -> list["PMOrderIntent"]:
        """Unresolved intents that would increase exposure (reduce_only=False)."""
        return list(
            cls.session.query(cls)
            .filter(cls.state.in_(list(UNRESOLVED_STATES)), cls.reduce_only.is_(False))
            .all()
        )

    @classmethod
    def get_linked(cls) -> list["PMOrderIntent"]:
        """LINKED intents waiting for reconciliation confirmation."""
        return list(cls.session.query(cls).filter(cls.state == "LINKED").all())

    @classmethod
    def has_unresolved(cls) -> bool:
        """Fast check whether any intent is unresolved (authoritative for entry gate)."""
        return (
            cls.session.query(cls)
            .filter(cls.state.in_(list(UNRESOLVED_STATES)))
            .first()
            is not None
        )

    @classmethod
    def has_unresolved_exposure_increasing(cls) -> bool:
        """Any unresolved intent that increases exposure?"""
        return (
            cls.session.query(cls)
            .filter(cls.state.in_(list(UNRESOLVED_STATES)), cls.reduce_only.is_(False))
            .first()
            is not None
        )

    @classmethod
    def has_unresolved_for_pair(cls, pair: str) -> bool:
        """Any unresolved intent for one pair (reduce-only ops may proceed per-pair)."""
        return (
            cls.session.query(cls)
            .filter(cls.state.in_(list(UNRESOLVED_STATES)), cls.pair == pair)
            .first()
            is not None
        )

    @classmethod
    def get_by_client_id(cls, client_id: str) -> "PMOrderIntent | None":
        return cls.session.query(cls).filter(cls.client_id == client_id).first()

    @classmethod
    def get_by_exchange_order_id(cls, exchange_order_id: str) -> "PMOrderIntent | None":
        return (
            cls.session.query(cls)
            .filter(cls.exchange_order_id == exchange_order_id)
            .first()
        )
