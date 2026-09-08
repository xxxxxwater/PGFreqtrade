"""Durable outbox for critical operator notifications.

This table is intentionally independent from the trading order outbox.  A
notification transport failure must never roll back or replay a trade event.
"""

from datetime import datetime
from typing import ClassVar

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


class PMNotificationOutbox(ModelBase):
    __tablename__ = "pm_notification_outbox"

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    dedupe_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    channel: Mapped[str] = mapped_column(String(16), nullable=False, default="telegram")
    message: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=50, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)
    last_attempt_at: Mapped[datetime | None] = mapped_column(nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(nullable=True, index=True)
    sent_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(255), nullable=True)

    @classmethod
    def get_by_dedupe_key(cls, dedupe_key: str) -> "PMNotificationOutbox | None":
        return cls.session.query(cls).filter(cls.dedupe_key == dedupe_key).first()

    @classmethod
    def due(cls, now: datetime, limit: int = 50) -> list["PMNotificationOutbox"]:
        return list(
            cls.session.query(cls)
            .filter(
                cls.state == "PENDING",
                (cls.next_attempt_at.is_(None) | (cls.next_attempt_at <= now)),
            )
            .order_by(cls.priority.desc(), cls.id.asc())
            .limit(limit)
            .all()
        )
    @classmethod
    def pending_count(cls) -> int:
        return int(cls.session.query(cls).filter(cls.state == "PENDING").count())
