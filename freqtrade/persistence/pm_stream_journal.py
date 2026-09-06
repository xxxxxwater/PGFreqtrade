"""Durable ownership incidents for PM user-stream order events.

The caller must resolve the exchange symbol to the same canonical futures pair
used by Trade/Order before using this journal. Exchange order IDs are scoped by
that instrument: an order ID alone is never an ownership key.

Methods mutate the shared Trade transaction but never commit it. A caller must
commit before claiming that the admission gate survives a process restart.
"""

import json
from datetime import datetime
from math import isfinite
from typing import Any, ClassVar

from sqlalchemy import Boolean, Float, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now


class PMStreamJournal(ModelBase):
    """First event evidence and resolution of an instrument-scoped incident."""

    __tablename__ = "pm_stream_journal"
    __table_args__ = (
        UniqueConstraint("pair", "exchange_order_id", name="uq_pm_stream_instrument_order"),
    )

    session: ClassVar[SessionType]

    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column(String(64), nullable=False)
    exchange_order_id: Mapped[str] = mapped_column(String(255), nullable=False)
    client_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    order_data: Mapped[str] = mapped_column(Text, nullable=False)
    # Keep the first raw event AND monotone evidence from later events. A late
    # NEW event must not erase a FILLED event received while ownership was unknown.
    max_cumulative_filled: Mapped[float | None] = mapped_column(Float, nullable=True)
    saw_filled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    unresolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    linked_order_pk: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)

    @classmethod
    def get(cls, pair: str, order_id: str) -> "PMStreamJournal | None":
        """Read an incident, including a pending insert in this transaction."""
        order_id = str(order_id)
        for row in cls.session.new:
            if isinstance(row, cls) and row.pair == pair and row.exchange_order_id == order_id:
                return row
        return (
            cls.session.query(cls)
            .filter(cls.pair == pair, cls.exchange_order_id == order_id)
            .first()
        )

    @classmethod
    def record_unresolved(
        cls,
        pair: str,
        order_id: str,
        client_id: str | None,
        order_data: dict[str, Any],
        reason: str,
    ) -> tuple["PMStreamJournal", bool]:
        """Return the incident and whether it needs its first notification.

        Duplicate events cannot overwrite the first evidence of an unresolved
        incident. A different nonempty client ID is an identity conflict, not a
        replay. Reopening a resolved incident records fresh evidence for the new
        incident and clears its prior ownership link.
        """
        if not pair or order_id is None or not str(order_id):
            raise ValueError("PM stream journal requires a canonical pair and exchange order ID")
        if not isinstance(order_data, dict):
            raise ValueError("PM stream journal order_data must be an object")
        order_id = str(order_id)
        client_id = str(client_id) if client_id else None
        cumulative = None
        if order_data.get("z") is not None:
            cumulative = float(order_data["z"])
            if not isfinite(cumulative) or cumulative < 0:
                raise ValueError("PM stream cumulative fill must be finite and nonnegative")
        saw_filled = order_data.get("X") == "FILLED"
        row = cls.get(pair, order_id)
        if row is not None:
            if row.client_id and client_id and row.client_id != client_id:
                raise ValueError("PM stream journal client ID conflicts with existing evidence")
            if row.unresolved:
                if cumulative is not None:
                    row.max_cumulative_filled = max(row.max_cumulative_filled or 0, cumulative)
                row.saw_filled = row.saw_filled or saw_filled
                return row, False
            payload = json.dumps(order_data, sort_keys=True, separators=(",", ":"), allow_nan=False)
            row.order_data = payload
            row.max_cumulative_filled = cumulative
            row.saw_filled = saw_filled
            row.client_id = row.client_id or client_id
            row.reason = reason
            row.unresolved = True
            row.linked_order_pk = None
            row.updated_at = dt_now()
            return row, True
        payload = json.dumps(order_data, sort_keys=True, separators=(",", ":"), allow_nan=False)
        now = dt_now()
        row = cls(
            pair=pair,
            exchange_order_id=order_id,
            client_id=client_id,
            order_data=payload,
            max_cumulative_filled=cumulative,
            saw_filled=saw_filled,
            reason=reason,
            unresolved=True,
            created_at=now,
            updated_at=now,
        )
        cls.session.add(row)
        return row, True

    @classmethod
    def get_unresolved(cls) -> list["PMStreamJournal"]:
        """Include pending incidents; exclude resolutions not yet flushed."""
        rows = list(cls.session.query(cls).filter(cls.unresolved.is_(True)).order_by(cls.id).all())
        # Production sessions have autoflush=False. Overlay in-memory changes so
        # callers cannot clear a gate between record/resolve and Trade.commit().
        rows.extend(row for row in cls.session.new if isinstance(row, cls))
        rows.extend(
            row
            for row in cls.session.dirty
            if isinstance(row, cls) and row.unresolved and row not in rows
        )
        return [row for row in rows if row.unresolved and row not in cls.session.deleted]

    @classmethod
    def resolve(cls, pair: str, order_id: str, local_order_pk: int) -> bool:
        """Link evidence to a local Order primary key, without committing."""
        if (
            not isinstance(local_order_pk, int)
            or isinstance(local_order_pk, bool)
            or local_order_pk < 1
        ):
            raise ValueError("PM stream journal resolution requires a local Order primary key")
        row = cls.get(pair, order_id)
        if row is None:
            return False
        if row.linked_order_pk is not None and row.linked_order_pk != local_order_pk:
            raise ValueError("PM stream journal resolution conflicts with its existing Order link")
        if row.unresolved:
            row.linked_order_pk = local_order_pk
            row.unresolved = False
            row.updated_at = dt_now()
        return True

    @classmethod
    def purge_resolved(cls, older_than: datetime, limit: int = 1000) -> int:
        """Delete OLD resolved incident rows (bounded batch), never aliases.

        A resolved row whose exchange_order_id matches a local Order row is
        pure event evidence: the orders table owns that lookup, so the row may
        be purged after the retention window. A row whose exchange_order_id
        does NOT match any local Order row is a durable child/parent ownership
        alias (the parent conditional is stored under its strategy id) and is
        NEVER purged - late child events must still resolve ownership.

        Unresolved rows are never purged. The caller must commit.
        """
        if limit < 1:
            raise ValueError("PM stream journal purge limit must be positive")
        # Lazy import: the persistence package aggregates the models and would
        # otherwise import this module again.
        from sqlalchemy import exists, select

        from freqtrade.persistence.trade_model import Order

        plain_evidence = exists(
            select(1).where(Order.order_id == cls.exchange_order_id)
        )
        rows = (
            cls.session.query(cls)
            .filter(
                cls.unresolved.is_(False),
                cls.updated_at < older_than,
                plain_evidence,
            )
            .limit(limit)
            .all()
        )
        for row in rows:
            cls.session.delete(row)
        return len(rows)
