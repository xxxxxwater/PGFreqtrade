"""
PM signal decision ledger model.

One row per (pair, timeframe, candle_open_time, decision_scope) stores the
immutable factor/signal snapshot.  The legacy ``decision`` columns preserve the
first observed decision for compatibility, but are NOT the execution lifecycle.
Every later blocked/submitted/result transition is appended to
``PMSignalDecisionEvent`` under a stable decision id.

This deliberately allows factor recomputation after restart while keeping
trading side effects independently idempotent through the PM intent/outbox
pipeline.  It does not claim exactly-once factor computation.
"""

from datetime import datetime
from typing import ClassVar

from sqlalchemy import Boolean, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


class PMSignalLedger(ModelBase):
    """Durable signal-decision record for the PM strategy."""

    __tablename__ = "pm_signal_ledger"
    __table_args__ = (
        UniqueConstraint(
            "pair",
            "timeframe",
            "candle_open_time",
            "decision_scope",
            name="uq_pm_signal_candle_scope",
        ),
    )

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    candle_open_time: Mapped[datetime] = mapped_column(nullable=False, index=True)
    decision_scope: Mapped[str] = mapped_column(String(16), nullable=False, default="entry")
    strategy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    factor_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    data_fresh: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    freshness_detail: Mapped[str | None] = mapped_column(String(128), nullable=True)
    signal_tag: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    order_client_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "pair": self.pair,
            "timeframe": self.timeframe,
            "candle_open_time": (
                self.candle_open_time.isoformat() if self.candle_open_time else None
            ),
            "decision_scope": self.decision_scope,
            "strategy_version": self.strategy_version,
            "factor_hash": self.factor_hash,
            "data_fresh": self.data_fresh,
            "freshness_detail": self.freshness_detail,
            "signal_tag": self.signal_tag,
            "decision": self.decision,
            "decision_reason": self.decision_reason,
            "order_client_id": self.order_client_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def record_once(
        cls,
        *,
        pair: str,
        timeframe: str,
        candle_open_time: datetime,
        strategy_version: str,
        factor_hash: str,
        data_fresh: bool,
        decision_scope: str = "entry",
        freshness_detail: str | None = None,
        signal_tag: str | None = None,
        decision: str,
        decision_reason: str | None = None,
        order_client_id: str | None = None,
    ) -> "PMSignalLedger":
        """
        Insert the immutable snapshot row for a candle/scope.  The decision
        fields capture only the first observed lifecycle state for backwards
        compatibility.  Later transitions belong in PMSignalDecisionEvent.
        """
        existing = cls.get_by_candle(pair, timeframe, candle_open_time, decision_scope)
        if existing is not None:
            return existing
        row = cls(
            pair=pair,
            timeframe=timeframe,
            candle_open_time=candle_open_time,
            decision_scope=decision_scope,
            strategy_version=strategy_version,
            factor_hash=factor_hash,
            data_fresh=data_fresh,
            freshness_detail=(freshness_detail or "")[:128] or None,
            signal_tag=signal_tag,
            decision=decision,
            decision_reason=(decision_reason or "")[:255] or None,
            order_client_id=order_client_id,
        )
        cls.session.add(row)
        return row

    @classmethod
    def get_by_candle(
        cls,
        pair: str,
        timeframe: str,
        candle_open_time: datetime,
        decision_scope: str = "entry",
    ) -> "PMSignalLedger | None":
        return (
            cls.session.query(cls)
            .filter(
                cls.pair == pair,
                cls.timeframe == timeframe,
                cls.candle_open_time == candle_open_time,
                cls.decision_scope == decision_scope,
            )
            .first()
        )

    @classmethod
    def set_order_client_id(
        cls,
        pair: str,
        timeframe: str,
        candle_open_time: datetime,
        client_id: str,
        decision_scope: str = "entry",
    ) -> None:
        """Backfill the exchange client id of the submitted order."""
        row = cls.get_by_candle(pair, timeframe, candle_open_time, decision_scope)
        if row is not None and not row.order_client_id:
            row.order_client_id = client_id

    @classmethod
    def recent(
        cls,
        pair: str | None = None,
        limit: int = 100,
        decision_scope: str | None = None,
    ) -> list["PMSignalLedger"]:
        query = cls.session.query(cls)
        if pair is not None:
            query = query.filter(cls.pair == pair)
        if decision_scope is not None:
            query = query.filter(cls.decision_scope == decision_scope)
        return list(query.order_by(cls.candle_open_time.desc()).limit(limit).all())
