"""分群快照：读取、参数调整、触发算法 A 重算批次。"""
from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.constants.enums import SEGMENT_LABELS, SEGMENT_ORDER, SEGMENT_RULES
from src.constants.limits import DEFAULT_RFM_WEIGHTS, RFM_WINDOW_DAYS
from src.models.member import Member
from src.models.rfm_batch import RfmBatch
from src.models.segment import Segment
from src.services import rfm_batch_service
from src.utils.errors import not_found
from src.utils.timeutil import iso_utc


def serialize(segment: Segment) -> dict:
    return {
        "id": segment.id,
        "code": segment.code,
        "name": segment.name,
        "rule_text": segment.rule_text,
        "sort_order": segment.sort_order,
        "member_count": segment.member_count,
        "share": round(segment.share or 0.0, 4),
        "avg_r_score": round(segment.avg_r_score or 0.0, 2),
        "avg_f_score": round(segment.avg_f_score or 0.0, 2),
        "avg_m_score": round(segment.avg_m_score or 0.0, 2),
        "avg_r_days": round(segment.avg_r_days or 0.0, 1),
        "avg_amount_window": round(segment.avg_amount_window or 0.0, 2),
        "purchase_rate": round(segment.purchase_rate or 0.0, 6),
        "avg_ticket": round(segment.avg_ticket or 0.0, 2),
        "lift": round(segment.lift or 0.0, 4),
        "coupon_face": round(segment.coupon_face or 0.0, 2),
        "core_share": round(segment.core_share or 0.0, 4),
        "enabled": bool(segment.enabled),
        "as_of": iso_utc(segment.as_of),
        "updated_at": iso_utc(segment.updated_at),
    }


def list_segments(db: Session) -> dict:
    rows = db.scalars(select(Segment).order_by(Segment.sort_order, Segment.id)).all()
    snapshot = {}
    for row in rows:
        if row.snapshot:
            snapshot = row.snapshot
            break
    as_of = next((iso_utc(row.as_of) for row in rows if row.as_of), None)
    members_total = db.scalar(select(func.count()).select_from(Member)) or 0
    active = rfm_batch_service.active_batch(db)
    pending = db.scalar(
        select(func.count()).select_from(RfmBatch).where(RfmBatch.status == "ready")
    ) or 0
    return {
        "items": [serialize(row) for row in rows],
        "members_total": members_total,
        "as_of": as_of,
        "snapshot": snapshot,
        "batch_no": snapshot.get("batch_no") or (active.batch_no if active else None),
        "active_batch_no": active.batch_no if active else None,
        "pending_batches": pending,
        "global_purchase_rate": snapshot.get("global_purchase_rate", 0.0),
        "weights": snapshot.get("weights", list(DEFAULT_RFM_WEIGHTS)),
        "window_days": snapshot.get("window_days", RFM_WINDOW_DAYS),
        "thresholds": snapshot.get("thresholds", {}),
    }


def get_segment(db: Session, segment_id: int) -> Segment:
    segment = db.get(Segment, segment_id)
    if segment is None:
        raise not_found("segment", segment_id)
    return segment


def update_segment(db: Session, segment: Segment, payload: dict) -> Segment:
    if payload.get("coupon_face") is not None:
        segment.coupon_face = round(float(payload["coupon_face"]), 2)
    if payload.get("core_share") is not None:
        segment.core_share = round(float(payload["core_share"]), 4)
    if payload.get("enabled") is not None:
        segment.enabled = bool(payload["enabled"])
    db.flush()
    return segment


def recompute(
    db: Session,
    *,
    weights: tuple[float, float, float] = DEFAULT_RFM_WEIGHTS,
    window_days: int = RFM_WINDOW_DAYS,
    trigger: str = "manual",
    actor: str = "system",
    note: str | None = None,
) -> dict:
    """重算落到独立批次（不覆盖线上口径）；确认后由 `activate` 一次性切换。"""
    return rfm_batch_service.recompute(
        db,
        weights=weights,
        window_days=window_days,
        trigger=trigger,
        actor=actor,
        note=note,
    )


def activate_batch(db: Session, batch_id: int, *, actor: str) -> dict:
    batch = rfm_batch_service.get_batch(db, batch_id)
    return rfm_batch_service.activate(db, batch, actor=actor)


def list_batches(db: Session, limit: int = 30) -> dict:
    return rfm_batch_service.list_batches(db, limit=limit)


def batch_detail(db: Session, batch_id: int) -> dict:
    return rfm_batch_service.batch_detail(db, rfm_batch_service.get_batch(db, batch_id))


def resume_batch(db: Session, batch_id: int) -> dict:
    return rfm_batch_service.resume(db, rfm_batch_service.get_batch(db, batch_id))


def segments_for_allocation(db: Session, batch: RfmBatch | None = None) -> list[dict]:
    """供算法 B 使用：优先读指定批次的八类汇总，券口径仍取线上运营配置。"""
    if batch is not None:
        overrides = {row.code: row for row in db.scalars(select(Segment)).all()}
        rows = rfm_batch_service.batch_segments(db, batch.id)
        result = []
        for row in rows:
            live = overrides.get(row.code)
            if live is not None and not live.enabled:
                continue
            result.append(
                {
                    "code": row.code,
                    "name": row.name,
                    "member_count": row.member_count,
                    "purchase_rate": row.purchase_rate,
                    "coupon_face": (live.coupon_face if live is not None else 0.0),
                    "core_share": (live.core_share if live is not None else 0.6),
                    "avg_ticket": row.avg_ticket,
                }
            )
        return [item for item in result if item["member_count"] > 0]
    rows = db.scalars(select(Segment).where(Segment.enabled.is_(True)).order_by(Segment.sort_order)).all()
    return [
        {
            "code": row.code,
            "name": row.name,
            "member_count": row.member_count,
            "purchase_rate": row.purchase_rate,
            "coupon_face": row.coupon_face,
            "core_share": row.core_share,
            "avg_ticket": row.avg_ticket,
        }
        for row in rows
        if row.member_count > 0
    ]


def ensure_segments_meta(db: Session) -> dict:
    rows = db.scalars(select(Segment).order_by(Segment.sort_order)).all()
    snapshot = next((row.snapshot for row in rows if row.snapshot), {})
    return {
        "codes": [row.code for row in rows] or SEGMENT_ORDER,
        "labels": SEGMENT_LABELS,
        "rules": SEGMENT_RULES,
        "snapshot": snapshot,
    }
