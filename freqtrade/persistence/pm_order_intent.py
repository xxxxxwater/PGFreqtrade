"""
PM order intent model.

A PM order intent is durably committed to the main freqtrade database BEFORE the
exchange POST is allowed. This is the single source of truth for order idempotency:
on timeout / disconnect / restart the SAME client id is resolved against the
exchange and a duplicate entry / DCA / reduceOnly / stoploss order can never be
submitted.

Failing to write / read / commit / parse this table is fail-closed: no
exposure-increasing order may be placed.
"""

from datetime import datetime
from typing import ClassVar

from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


class PMOrderIntent(ModelBase):
    """
    A persisted intent to place a PM order.

    ``client_id`` is the Binance ``newClientOrderId`` (kind="order") or
    ``newClientStrategyId`` (kind="conditional") and is unique.

    ``state`` lifecycle: PENDING -> (POST succeeds -> row deleted) or
    (POST transient failure -> UNKNOWN). UNKNOWN rows must block all
    exposure-increasing orders until resolved.
    """

    __tablename__ = "pm_order_intents"

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)  # order | conditional
    pair: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8), nullable=True)
    order_type: Mapped[str | None] = mapped_column(String(24), nullable=True)
    amount: Mapped[float | None] = mapped_column(nullable=True)
    price: Mapped[float | None] = mapped_column(nullable=True)
    stop_price: Mapped[float | None] = mapped_column(nullable=True)
    reduce_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    last_error: Mapped[str | None] = mapped_column(String(255), nullable=True)

    @property
    def is_unresolved(self) -> bool:
        """True when the intent outcome is not yet definitively known."""
        return self.state in ("PENDING", "UNKNOWN")

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
            "last_error": self.last_error,
        }

    @classmethod
    def get_unresolved(cls) -> list["PMOrderIntent"]:
        """All intents whose outcome is not yet definitively known."""
        return list(
            cls.session.query(cls).filter(cls.state.in_(["PENDING", "UNKNOWN"])).all()
        )

    @classmethod
    def has_unresolved(cls) -> bool:
        """Fast check whether any intent is unresolved (authoritative for entry gate)."""
        return cls.session.query(cls).filter(cls.state.in_(["PENDING", "UNKNOWN"])).first() is not None

    @classmethod
    def get_by_client_id(cls, client_id: str) -> "PMOrderIntent | None":
        return cls.session.query(cls).filter(cls.client_id == client_id).first()
