"""Append-only PM signal decision lifecycle events.

``PMSignalLedger`` stores the immutable per-candle factor/signal snapshot.  This
model stores every materially different execution decision/result for that
snapshot.  A stable ``decision_id`` joins blocked/submitted/result events across
retries and restarts without pretending factor computation itself is exactly-once.
"""

from datetime import datetime
from hashlib import sha256
from typing import ClassVar

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


class PMSignalDecisionEvent(ModelBase):
    """Append-only execution lifecycle for one durable signal snapshot."""

    __tablename__ = "pm_signal_decision_events"

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    decision_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    signal_ledger_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    pair: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    candle_open_time: Mapped[datetime] = mapped_column(nullable=False, index=True)
    decision_scope: Mapped[str] = mapped_column(String(16), nullable=False, default="entry")
    strategy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    factor_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    order_client_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)

    @staticmethod
    def make_decision_id(
        *,
        pair: str,
        timeframe: str,
        candle_open_time: datetime,
        decision_scope: str,
        strategy_version: str,
        factor_hash: str,
    ) -> str:
        payload = "|".join(
            (
                pair,
                timeframe,
                candle_open_time.isoformat(),
                decision_scope,
                strategy_version,
                factor_hash,
            )
        )
        return sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def append_event(
        cls,
        *,
        signal_ledger_id: int | None,
        pair: str,
        timeframe: str,
        candle_open_time: datetime,
        decision_scope: str,
        strategy_version: str,
        factor_hash: str,
        decision: str,
        decision_reason: str | None = None,
        order_client_id: str | None = None,
    ) -> tuple["PMSignalDecisionEvent", bool]:
        """Append a lifecycle event unless it exactly duplicates the latest one."""
        decision_id = cls.make_decision_id(
            pair=pair,
            timeframe=timeframe,
            candle_open_time=candle_open_time,
            decision_scope=decision_scope,
            strategy_version=strategy_version,
            factor_hash=factor_hash,
        )
        reason = (decision_reason or "")[:255] or None
        client_id = (order_client_id or "")[:40] or None
        latest = (
            cls.session.query(cls)
            .filter(cls.decision_id == decision_id)
            .order_by(cls.id.desc())
            .first()
        )
        if (
            latest is not None
            and latest.decision == decision
            and latest.decision_reason == reason
            and latest.order_client_id == client_id
        ):
            return latest, False
        row = cls(
            decision_id=decision_id,
            signal_ledger_id=signal_ledger_id,
            pair=pair,
            timeframe=timeframe,
            candle_open_time=candle_open_time,
            decision_scope=decision_scope,
            strategy_version=strategy_version,
            factor_hash=factor_hash,
            decision=decision,
            decision_reason=reason,
            order_client_id=client_id,
        )
        cls.session.add(row)
        # Trading sessions use autoflush=False.  Flush without committing so a
        # repeated callback in the same transaction can see and de-duplicate
        # this just-appended lifecycle event.
        cls.session.flush()
        return row, True

    @classmethod
    def recent_for_decision(cls, decision_id: str) -> list["PMSignalDecisionEvent"]:
        return list(
            cls.session.query(cls)
            .filter(cls.decision_id == decision_id)
            .order_by(cls.id.asc())
            .all()
        )
