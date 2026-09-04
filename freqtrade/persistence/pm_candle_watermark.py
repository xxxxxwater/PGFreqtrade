"""Durable PM candle-watermark state.

The signal ledger is an audit trail.  This model is its progress cursor: it
records the most recent *entry-scope* candle for which a decision was safely
written.  Keeping it in the same database transaction as the ledger row makes
restarts idempotent: a process can neither advance the cursor without an audit
row nor submit the same signal candle twice.

When the OHLCV series skips a candle, the cursor deliberately stays behind and
the gap is persisted.  Entry processing remains fail-closed until a later
payload proves that every missing candle has been backfilled.
"""

from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


def _utc_naive(value: datetime | None) -> datetime | None:
    """Normalize timestamps to the legacy PM tables' UTC-naive representation."""
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


class PMCandleWatermark(ModelBase):
    """One durable entry-decision cursor for a ``(pair, timeframe)`` series."""

    __tablename__ = "pm_candle_watermarks"
    __table_args__ = (
        UniqueConstraint("pair", "timeframe", name="uq_pm_candle_watermark"),
    )

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    # This cursor advances only in the transaction that creates an entry-scope
    # ledger row.  It is never used as a substitute for the ledger itself.
    last_decision_candle_open_time: Mapped[datetime | None] = mapped_column(nullable=True)
    # A durable gap latch.  It is cleared only after the bot sees a complete,
    # contiguous recovery sequence ending in a newly decided candle.
    gap_expected_open_time: Mapped[datetime | None] = mapped_column(nullable=True)
    gap_observed_open_time: Mapped[datetime | None] = mapped_column(nullable=True)
    gap_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    gap_detected_at: Mapped[datetime | None] = mapped_column(nullable=True)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now, onupdate=dt_now)

    @property
    def gap_active(self) -> bool:
        return self.gap_expected_open_time is not None

    @classmethod
    def get(
        cls, pair: str, timeframe: str
    ) -> "PMCandleWatermark | None":
        return (
            cls.session.query(cls)
            .filter(cls.pair == pair, cls.timeframe == timeframe)
            .first()
        )

    @classmethod
    def get_or_create(cls, pair: str, timeframe: str) -> "PMCandleWatermark":
        """Return the persistent cursor, adding (but not committing) it when absent."""
        row = cls.get(pair, timeframe)
        if row is None:
            row = cls(pair=pair, timeframe=timeframe)
            cls.session.add(row)
        return row

    def mark_gap(
        self,
        *,
        expected_open_time: datetime,
        observed_open_time: datetime,
        reason: str,
    ) -> None:
        """Latch a discontinuity; callers commit it with the blocked ledger row."""
        self.gap_expected_open_time = _utc_naive(expected_open_time)
        self.gap_observed_open_time = _utc_naive(observed_open_time)
        self.gap_reason = reason[:255]
        self.gap_detected_at = dt_now()

    def mark_entry_decision(
        self,
        candle_open_time: datetime,
        *,
        recovered_gap: bool = False,
    ) -> bool:
        """
        Advance the cursor after a new entry decision is inserted.

        Returns ``False`` for an old/duplicate timestamp.  Callers must then
        roll back their attempted insert rather than silently moving state.
        """
        candle = _utc_naive(candle_open_time)
        if candle is None:
            return False
        if (
            self.last_decision_candle_open_time is not None
            and candle <= _utc_naive(self.last_decision_candle_open_time)
        ):
            return False
        self.last_decision_candle_open_time = candle
        if recovered_gap or self.gap_active:
            self.gap_expected_open_time = None
            self.gap_observed_open_time = None
            self.gap_reason = None
            self.gap_detected_at = None
        return True

    def to_dict(self) -> dict:
        return {
            "pair": self.pair,
            "timeframe": self.timeframe,
            "last_decision_candle_open_time": (
                self.last_decision_candle_open_time.isoformat()
                if self.last_decision_candle_open_time
                else None
            ),
            "gap_expected_open_time": (
                self.gap_expected_open_time.isoformat() if self.gap_expected_open_time else None
            ),
            "gap_observed_open_time": (
                self.gap_observed_open_time.isoformat() if self.gap_observed_open_time else None
            ),
            "gap_reason": self.gap_reason,
            "gap_detected_at": self.gap_detected_at.isoformat() if self.gap_detected_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
