"""
PM order outbox model.

The outbox is the durable record of the *delivery* of a PM order operation to the
exchange. Every PM order / algo-stoploss POST goes through it:

    PENDING   ->  ACKED    ->  LINKED   ->  RECONCILED
    (enqueued     (exchange (local        (exchange state confirmed
     before POST)  ACK, id + Trade/Order    against the local order)
                   raw resp  committed in
                   stored)   the same tx)
    PENDING   ->  REJECTED  (deterministic exchange rejection)
    PENDING   ->  DEAD       (unrecoverable after max attempts / manual abort)

Unlike the intent table (short-lived, drives the entry gate), outbox rows are
kept as the permanent audit trail of every order operation: client id, full
request payload, exchange order/algo id, the raw ACK response, and the local
order/trade linkage.
"""

from datetime import datetime
from typing import ClassVar

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now

OUTBOX_ACTIVE_STATES = ("PENDING", "ACKED", "LINKED")
OUTBOX_TERMINAL_STATES = ("RECONCILED", "REJECTED", "DEAD")


class PMOutbox(ModelBase):
    """Durable record of one PM order delivery operation."""

    __tablename__ = "pm_outbox"

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    operation: Mapped[str] = mapped_column(String(16), nullable=False)  # order | conditional
    # JSON text of the full request sent to the exchange (includes the
    # newClientOrderId / clientAlgoId - the idempotency key).
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    linked_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    linked_trade_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dispatch_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)
    processed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    linked_at: Mapped[datetime | None] = mapped_column(nullable=True)
    reconciled_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(255), nullable=True)

    @classmethod
    def get_by_client_id(cls, client_id: str) -> "PMOutbox | None":
        return cls.session.query(cls).filter(cls.client_id == client_id).first()

    @classmethod
    def get_pending(cls, limit: int = 50) -> list["PMOutbox"]:
        """Undispatched (PENDING) rows, oldest first - drained by the relay."""
        return list(
            cls.session.query(cls)
            .filter(cls.state == "PENDING")
            .order_by(cls.id.asc())
            .limit(limit)
            .all()
        )

    @classmethod
    def get_unreconciled(cls, limit: int = 50) -> list["PMOutbox"]:
        """ACKED/LINKED rows that reconciliation should verify, oldest first."""
        return list(
            cls.session.query(cls)
            .filter(cls.state.in_(("ACKED", "LINKED")))
            .order_by(cls.id.asc())
            .limit(limit)
            .all()
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "client_id": self.client_id,
            "operation": self.operation,
            "state": self.state,
            "exchange_order_id": self.exchange_order_id,
            "linked_order_id": self.linked_order_id,
            "linked_trade_id": self.linked_trade_id,
            "dispatch_attempts": self.dispatch_attempts,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "linked_at": self.linked_at.isoformat() if self.linked_at else None,
            "reconciled_at": self.reconciled_at.isoformat() if self.reconciled_at else None,
            "last_error": self.last_error,
        }
