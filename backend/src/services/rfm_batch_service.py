"""RFM 重算批次：独立算、可续跑、核对后一次性切换线上口径。

设计要点（与 README §10「重算批次」一节对应）：

1. **独立重算**：批次从订单事实重算 R/F/M，结果写到 `rfm_batches` /
   `rfm_batch_members` / `rfm_batch_segments`，**不碰**线上 `members` / `segments`。
2. **同水位同权重复用**：批次指纹 = sha256(快照日 + 权重 + 窗口 + 订单水位)。
   同一水位和权重再点一次重算，返回已有批次，不新增行、不重复评分。
3. **失败续跑**：批次分 `collect → score → aggregate → ready` 四段，
   每段写游标（`raw_cursor` / `score_cursor`），失败后重跑从已处理位置继续；
   已写入的会员快照不会重复插入。
4. **一次性切换**：`activate()` 在一个事务里核对人数与购买率，然后整批覆盖
   线上 `members` 评分字段与八类汇总；失败整体回滚，旧版保留（不丢记录）。
5. **名单冻结**：名单生成时把 `batch_no` 和会员级明细写进 `audiences`，
   导出只读这份快照，后续任何重算都不改动已确认名单及其导出明细。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.constants.enums import SEGMENT_LABELS, SEGMENT_ORDER
from src.constants.limits import (
    BATCH_CHUNK_SIZE,
    BATCH_STATUS_LABELS,
    DEFAULT_RFM_WEIGHTS,
    NO_ORDER_RECENCY_DAYS,
    RFM_WINDOW_DAYS,
)
from src.models.member import Member
from src.models.order import Order
from src.models.rfm_batch import RfmBatch, RfmBatchMember, RfmBatchSegment
from src.models.segment import Segment
from src.services import rfm_service
from src.utils.errors import AppError, not_found
from src.utils.timeutil import iso_utc, utcnow

STAGE_ORDER = ("collect", "score", "aggregate")


def _fingerprint(as_of, window_days: int, weights, mark: dict) -> str:
    max_paid = mark["max_paid_at"].isoformat() if mark.get("max_paid_at") else "none"
    raw = "|".join(
        [
            as_of.isoformat(),
            str(window_days),
            "/".join(f"{float(item):.4f}" for item in weights),
            str(mark["count"]),
            str(mark.get("max_id") or 0),
            max_paid,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _batch_no(as_of, fingerprint: str) -> str:
    return f"B{as_of:%Y%m%d}-{fingerprint[:10]}"


def watermark(db: Session) -> dict:
    """订单水位：行数 + 最大 id + 最大支付时间。补录/重试会移动水位。"""
    count, max_id, max_paid = db.execute(
        select(func.count(Order.id), func.max(Order.id), func.max(Order.paid_at))
    ).one()
    return {"count": int(count or 0), "max_id": int(max_id) if max_id else None, "max_paid_at": max_paid}


def _rounded_thresholds(thresholds: dict | None) -> dict:
    """展示用切点：保留 2 位小数（评分仍用原始精度，见 `_score`）。"""
    if not thresholds:
        return {}
    return {key: [round(float(item), 2) for item in values] for key, values in thresholds.items()}


def serialize_batch(batch: RfmBatch, *, is_active: bool = False) -> dict:
    progress = 0.0
    if batch.status in {"ready", "active", "superseded"}:
        progress = 1.0
    elif batch.member_count:
        progress = round(min(1.0, (batch.processed_members or 0) / batch.member_count), 4)
    return {
        "id": batch.id,
        "batch_no": batch.batch_no,
        "status": batch.status,
        "status_label": BATCH_STATUS_LABELS.get(batch.status, batch.status),
        "stage": batch.stage,
        "is_active": is_active,
        "as_of": batch.as_of.isoformat() if batch.as_of else None,
        "window_days": batch.window_days,
        "weights": list(batch.weights or []),
        "watermark": {
            "orders": batch.order_count,
            "max_order_id": batch.order_max_id,
            "max_paid_at": iso_utc(batch.order_max_paid_at),
        },
        "member_count": batch.member_count,
        "orders_total": batch.orders_total,
        "amount_total": round(batch.amount_total or 0.0, 2),
        "global_purchase_rate": round(batch.global_purchase_rate or 0.0, 6),
        "thresholds": _rounded_thresholds(batch.thresholds),
        "processed_members": batch.processed_members,
        "progress": progress,
        "error": batch.error,
        "trigger": batch.trigger,
        "created_by": batch.created_by,
        "created_at": iso_utc(batch.created_at),
        "activated_at": iso_utc(batch.activated_at),
        "activated_by": batch.activated_by,
        "switched_from": batch.switched_from,
        "note": batch.note,
    }


def serialize_batch_segment(row: RfmBatchSegment) -> dict:
    return {
        "code": row.code,
        "name": row.name,
        "rule_text": row.rule_text,
        "sort_order": row.sort_order,
        "member_count": row.member_count,
        "share": round(row.share or 0.0, 4),
        "avg_r_score": round(row.avg_r_score or 0.0, 2),
        "avg_f_score": round(row.avg_f_score or 0.0, 2),
        "avg_m_score": round(row.avg_m_score or 0.0, 2),
        "avg_r_days": round(row.avg_r_days or 0.0, 1),
        "avg_amount_window": round(row.avg_amount_window or 0.0, 2),
        "purchase_rate": round(row.purchase_rate or 0.0, 6),
        "avg_ticket": round(row.avg_ticket or 0.0, 2),
        "lift": round(row.lift or 0.0, 4),
    }


def active_batch(db: Session) -> RfmBatch | None:
    return db.scalar(select(RfmBatch).where(RfmBatch.status == "active").order_by(RfmBatch.activated_at.desc()))


def batch_segments(db: Session, batch_id: int) -> list[RfmBatchSegment]:
    return list(
        db.scalars(
            select(RfmBatchSegment)
            .where(RfmBatchSegment.batch_id == batch_id)
            .order_by(RfmBatchSegment.sort_order, RfmBatchSegment.id)
        ).all()
    )


def list_batches(db: Session, limit: int = 30) -> dict:
    active = active_batch(db)
    active_no = active.batch_no if active else None
    rows = db.scalars(select(RfmBatch).order_by(RfmBatch.id.desc()).limit(limit)).all()
    return {
        "items": [serialize_batch(row, is_active=row.batch_no == active_no) for row in rows],
        "active_batch_no": active_no,
        "total": db.scalar(select(func.count()).select_from(RfmBatch)) or 0,
    }


def get_batch(db: Session, batch_id: int) -> RfmBatch:
    batch = db.get(RfmBatch, batch_id)
    if batch is None:
        raise not_found("rfm_batch", batch_id)
    return batch


def batch_detail(db: Session, batch: RfmBatch) -> dict:
    active = active_batch(db)
    payload = serialize_batch(batch, is_active=bool(active and active.id == batch.id))
    payload["segments"] = [serialize_batch_segment(row) for row in batch_segments(db, batch.id)]
    payload["diff"] = None if (active and active.id == batch.id) else diff_against_active(db, batch)
    return payload


def diff_against_active(db: Session, batch: RfmBatch) -> dict | None:
    """候选批次 vs 线上正式口径：人数 / 购买率 / lift 的逐群差异。"""
    active = active_batch(db)
    if active is not None:
        base_rows = batch_segments(db, active.id)
        base_label = active.batch_no
        base_global = round(float(active.global_purchase_rate or 0.0), 6)
    else:
        segment_rows = db.scalars(select(Segment).order_by(Segment.sort_order, Segment.id)).all()
        base_rows = [
            RfmBatchSegment(
                code=row.code,
                name=row.name,
                member_count=row.member_count,
                purchase_rate=row.purchase_rate,
                lift=row.lift,
            )
            for row in segment_rows
        ]
        base_label = "线上当前口径"
        base_global = None
    if not base_rows:
        return None
    base = {row.code: row for row in base_rows}
    cand = {row.code: row for row in batch_segments(db, batch.id)}
    rows = []
    for code in SEGMENT_ORDER:
        left = base.get(code)
        right = cand.get(code)
        left_count = int(getattr(left, "member_count", 0) or 0)
        right_count = int(getattr(right, "member_count", 0) or 0)
        rows.append(
            {
                "code": code,
                "name": SEGMENT_LABELS[code],
                "current_member_count": left_count,
                "candidate_member_count": right_count,
                "member_count_delta": right_count - left_count,
                "current_purchase_rate": round(float(getattr(left, "purchase_rate", 0.0) or 0.0), 6),
                "candidate_purchase_rate": round(float(getattr(right, "purchase_rate", 0.0) or 0.0), 6),
                "current_lift": round(float(getattr(left, "lift", 0.0) or 0.0), 4),
                "candidate_lift": round(float(getattr(right, "lift", 0.0) or 0.0), 4),
            }
        )
    total_current = sum(item["current_member_count"] for item in rows)
    total_candidate = sum(item["candidate_member_count"] for item in rows)
    return {
        "current_batch_no": base_label,
        "candidate_batch_no": batch.batch_no,
        "current_member_count": total_current,
        "candidate_member_count": total_candidate,
        "member_count_delta": total_candidate - total_current,
        "current_global_purchase_rate": base_global,
        "candidate_global_purchase_rate": round(float(batch.global_purchase_rate or 0.0), 6),
        "segments": rows,
    }


def _start_or_reuse(
    db: Session,
    *,
    as_of,
    weights,
    window_days: int,
    mark: dict,
    trigger: str,
    actor: str,
    note: str | None,
) -> tuple[RfmBatch, bool]:
    fingerprint = _fingerprint(as_of, window_days, weights, mark)
    existing = db.scalar(select(RfmBatch).where(RfmBatch.fingerprint == fingerprint))
    if existing is not None:
        # 同一水位 + 同一权重：复用已有批次；失败/中断则从游标续跑，不新增行。
        if existing.status == "failed":
            existing.status = "running"
            existing.error = None
        return existing, False
    batch = RfmBatch(
        batch_no=_batch_no(as_of, fingerprint),
        fingerprint=fingerprint,
        status="running",
        stage="collect",
        as_of=as_of,
        window_days=window_days,
        weights=list(weights),
        order_count=mark["count"],
        order_max_id=mark["max_id"],
        order_max_paid_at=mark["max_paid_at"],
        trigger=trigger,
        created_by=actor,
        note=note,
    )
    db.add(batch)
    try:
        db.flush()
    except IntegrityError:
        # 并发下另一请求抢先建了同指纹批次：回滚后复用那一个
        db.rollback()
        existing = db.scalar(select(RfmBatch).where(RfmBatch.fingerprint == fingerprint))
        if existing is None:
            raise
        return existing, False
    return batch, True


def _existing_member_ids(db: Session, batch_id: int, member_ids: list[int]) -> set[int]:
    if not member_ids:
        return set()
    return set(
        db.scalars(
            select(RfmBatchMember.member_id).where(
                RfmBatchMember.batch_id == batch_id,
                RfmBatchMember.member_id.in_(member_ids),
            )
        ).all()
    )


def _collect(db: Session, batch: RfmBatch, *, chunk: int | None = None, commit: bool = True) -> None:
    """stage=collect：按 member.id 游标抓窗口内订单聚合，幂等插入会员快照。

    每处理一批就提交一次，崩溃/失败后已提交的游标与快照仍然有效，可续跑。
    """
    chunk = int(chunk or BATCH_CHUNK_SIZE)
    since = datetime.combine(batch.as_of - timedelta(days=batch.window_days), datetime.min.time())
    until = datetime.combine(batch.as_of, datetime.max.time())
    cursor = int(batch.raw_cursor or 0)
    while True:
        ids = db.scalars(
            select(Member.id).where(Member.id > cursor).order_by(Member.id).limit(chunk)
        ).all()
        if not ids:
            break
        rows = db.execute(
            select(
                Order.member_id,
                func.count(Order.id),
                func.sum(Order.amount),
                func.max(Order.paid_at),
            )
            .where(Order.member_id.in_(ids))
            .where(Order.paid_at >= since)
            .where(Order.paid_at <= until)
            .group_by(Order.member_id)
        ).all()
        aggregates = {row[0]: (int(row[1] or 0), float(row[2] or 0.0), row[3]) for row in rows}
        members = db.scalars(select(Member).where(Member.id.in_(ids)).order_by(Member.id)).all()
        done = _existing_member_ids(db, batch.id, [item.id for item in members])
        payload = []
        for member in members:
            if member.id in done:
                continue
            count, amount, latest = aggregates.get(member.id, (0, 0.0, None))
            payload.append(
                {
                    "batch_id": batch.id,
                    "member_id": member.id,
                    "member_no": member.member_no,
                    "name": member.name or "",
                    "phone": member.phone or "",
                    "city": member.city or "",
                    "tier": member.tier or "normal",
                    "r_days": rfm_service.recency_days(
                        latest.date() if isinstance(latest, datetime) else None, batch.as_of
                    ),
                    "orders": count,
                    "amount": round(amount, 2),
                    "purchase_rate": round(member.purchase_rate or 0.0, 6),
                    "avg_ticket": round(member.avg_ticket or 0.0, 2),
                }
            )
        if payload:
            db.bulk_insert_mappings(RfmBatchMember, payload)
        cursor = ids[-1]
        batch.raw_cursor = cursor
        db.flush()
        if commit:
            db.commit()
        if len(ids) < chunk:
            break
    batch.member_count = db.scalar(
        select(func.count()).select_from(RfmBatchMember).where(RfmBatchMember.batch_id == batch.id)
    ) or 0
    batch.stage = "score"
    db.flush()
    if commit:
        db.commit()


def _score(db: Session, batch: RfmBatch, *, chunk: int = BATCH_CHUNK_SIZE, commit: bool = True) -> None:
    """stage=score：先按全量批次会员算分位切点，再按 member_id 游标逐批打分。

    切点保留 `rfm_service.percentiles()` 的原始精度（不提前四舍五入），
    保证与直接调用 `rfm_service.compute()` 的打分逐位一致。
    """
    if not batch.thresholds:
        rows = db.execute(
            select(RfmBatchMember.r_days, RfmBatchMember.orders, RfmBatchMember.amount).where(
                RfmBatchMember.batch_id == batch.id
            )
        ).all()
        if not rows:
            raise AppError("VALIDATION", 409, {"reason": "批次内没有会员快照，无法评分", "batch_no": batch.batch_no})
        batch.thresholds = {
            "r": rfm_service.percentiles([row[0] for row in rows]),
            "f": rfm_service.percentiles([row[1] for row in rows]),
            "m": rfm_service.percentiles([row[2] for row in rows]),
        }
        db.flush()
    thresholds = {key: [float(item) for item in value] for key, value in batch.thresholds.items()}
    weights = tuple(float(item) for item in batch.weights)
    cursor = int(batch.score_cursor or 0)
    while True:
        rows = db.scalars(
            select(RfmBatchMember)
            .where(
                RfmBatchMember.batch_id == batch.id,
                RfmBatchMember.member_id > cursor,
                RfmBatchMember.segment_code.is_(None),
            )
            .order_by(RfmBatchMember.member_id)
            .limit(chunk)
        ).all()
        if not rows:
            break
        for row in rows:
            r_score = rfm_service.score_of(row.r_days, thresholds["r"], reverse=True)
            f_score = rfm_service.score_of(row.orders, thresholds["f"])
            m_score = rfm_service.score_of(row.amount, thresholds["m"])
            row.r_score = r_score
            row.f_score = f_score
            row.m_score = m_score
            row.composite_score = round(weights[0] * r_score + weights[1] * f_score + weights[2] * m_score, 2)
            row.segment_code = rfm_service.classify(r_score, f_score, m_score)
        cursor = rows[-1].member_id
        batch.score_cursor = cursor
        batch.processed_members = db.scalar(
            select(func.count())
            .select_from(RfmBatchMember)
            .where(RfmBatchMember.batch_id == batch.id, RfmBatchMember.segment_code.is_not(None))
        ) or 0
        db.flush()
        if commit:
            db.commit()
        if len(rows) < chunk:
            break
    batch.stage = "aggregate"
    db.flush()
    if commit:
        db.commit()


def _aggregate(db: Session, batch: RfmBatch) -> None:
    """stage=aggregate：直接复用 `rfm_service.aggregate()`，与直接计算逐位一致。"""
    rows = db.scalars(
        select(RfmBatchMember)
        .where(RfmBatchMember.batch_id == batch.id, RfmBatchMember.segment_code.is_not(None))
        .order_by(RfmBatchMember.member_id)
    ).all()
    if not rows:
        raise AppError("VALIDATION", 409, {"reason": "批次没有可用评分结果", "batch_no": batch.batch_no})
    scored = [
        {
            "id": row.member_id,
            "member_no": row.member_no,
            "r_days": row.r_days,
            "orders": row.orders,
            "amount": row.amount,
            "r_score": row.r_score,
            "f_score": row.f_score,
            "m_score": row.m_score,
            "segment_code": row.segment_code,
            "purchase_rate": row.purchase_rate,
            "avg_ticket": row.avg_ticket,
        }
        for row in rows
    ]
    weights = tuple(float(item) for item in batch.weights)
    result = rfm_service.aggregate(scored, weights=weights, window_days=batch.window_days)
    db.execute(delete(RfmBatchSegment).where(RfmBatchSegment.batch_id == batch.id))
    for item in result["segments"]:
        db.add(
            RfmBatchSegment(
                batch_id=batch.id,
                code=item["code"],
                name=item["name"],
                rule_text=item["rule_text"],
                sort_order=item["sort_order"],
                member_count=item["member_count"],
                share=item["share"],
                avg_r_score=item["avg_r_score"],
                avg_f_score=item["avg_f_score"],
                avg_m_score=item["avg_m_score"],
                avg_r_days=item["avg_r_days"],
                avg_amount_window=item["avg_amount_window"],
                purchase_rate=item["purchase_rate"],
                avg_ticket=item["avg_ticket"],
                lift=item["lift"],
            )
        )
    batch.global_purchase_rate = result["global_purchase_rate"]
    batch.orders_total = result["totals"]["orders"]
    batch.amount_total = result["totals"]["amount"]
    batch.member_count = result["totals"]["members"]
    batch.processed_members = result["totals"]["members"]
    batch.stage = "ready"
    batch.status = "ready"
    batch.error = None
    db.flush()


def _verify(batch: RfmBatch, member_count: int, segment_rows: list[RfmBatchSegment]) -> None:
    """切换前核对人数与购买率：批次人数 == 已评分会员数 == 八类人数之和，大盘购买率与加权一致。"""
    if not member_count:
        raise AppError("CONFLICT", 409, {"reason": "批次没有已评分会员，无法切换", "batch_no": batch.batch_no})
    if len(segment_rows) != len(SEGMENT_ORDER):
        raise AppError(
            "CONFLICT",
            409,
            {"reason": "批次八类汇总不完整", "batch_no": batch.batch_no, "segments": len(segment_rows)},
        )
    segment_total = sum(int(row.member_count or 0) for row in segment_rows)
    if segment_total != member_count:
        raise AppError(
            "CONFLICT",
            409,
            {"reason": "八类人数之和与批次会员数不一致", "segments": segment_total, "members": member_count},
        )
    if int(batch.member_count or 0) != member_count:
        raise AppError(
            "CONFLICT",
            409,
            {"reason": "批次记录人数与实际明细不一致", "recorded": batch.member_count, "actual": member_count},
        )
    # 大盘购买率应等于按群人数加权的分群购买率
    weighted = (
        sum(float(row.purchase_rate or 0.0) * int(row.member_count or 0) for row in segment_rows) / member_count
        if member_count
        else 0.0
    )
    recorded = float(batch.global_purchase_rate or 0.0)
    if abs(weighted - recorded) > 1e-4:
        raise AppError(
            "CONFLICT",
            409,
            {
                "reason": "大盘购买率与八类加权购买率不一致",
                "weighted": round(weighted, 6),
                "recorded": round(recorded, 6),
            },
        )
    # 每个分群的 lift 必须与其购买率 / 大盘购买率一致
    for row in segment_rows:
        count = int(row.member_count or 0)
        expected_lift = round(float(row.purchase_rate or 0.0) / recorded, 4) if recorded > 0 and count else 0.0
        if count and abs(float(row.lift or 0.0) - expected_lift) > 1e-4:
            raise AppError(
                "CONFLICT",
                409,
                {
                    "reason": "分群 lift 与购买率不一致，请重跑批次",
                    "code": row.code,
                    "lift": float(row.lift or 0.0),
                    "expected": expected_lift,
                },
            )


def run(db: Session, batch: RfmBatch, *, commit_chunks: bool = True) -> RfmBatch:
    """推进到 ready；失败写 status=failed + error，游标保留可续跑。"""
    if batch.status in {"ready", "active", "superseded"}:
        return batch
    if batch.status == "failed":
        batch.status = "running"
        batch.error = None
        db.flush()
    batch_id = batch.id
    try:
        for stage in STAGE_ORDER:
            if batch.stage == stage:
                if stage == "collect":
                    _collect(db, batch, commit=commit_chunks)
                elif stage == "score":
                    _score(db, batch, commit=commit_chunks)
                else:
                    _aggregate(db, batch)
                    if commit_chunks:
                        db.commit()
        return batch
    except Exception as exc:  # noqa: BLE001 - 失败留痕，供续跑
        db.rollback()
        batch = db.get(RfmBatch, batch_id)
        if batch is not None:
            batch.status = "failed"
            batch.error = f"{type(exc).__name__}: {exc}"[:500]
            db.commit()
        raise


def _payload(db: Session, batch: RfmBatch, *, created: bool, reused: bool) -> dict:
    payload = serialize_batch(batch, is_active=batch.status == "active")
    payload["segments"] = [serialize_batch_segment(row) for row in batch_segments(db, batch.id)]
    payload["created"] = created
    payload["reused"] = reused
    return payload


def recompute(
    db: Session,
    *,
    weights=DEFAULT_RFM_WEIGHTS,
    window_days: int = RFM_WINDOW_DAYS,
    trigger: str = "manual",
    actor: str = "system",
    note: str | None = None,
) -> dict:
    """建批次并跑到 ready。同一水位 + 权重默认复用已有批次（不新增、不重复）。"""
    weights = rfm_service.validate_weights(tuple(weights))
    if window_days < 30 or window_days > 730:
        raise AppError("VALIDATION", 422, {"reason": "统计窗口必须在 30~730 天之间"})
    if db.scalar(select(func.count()).select_from(Member)) == 0:
        raise AppError("VALIDATION", 409, {"reason": "会员表为空，无法重算分群"})
    as_of = rfm_service.as_of_from_orders(db)
    mark = watermark(db)
    batch, created = _start_or_reuse(
        db,
        as_of=as_of,
        weights=weights,
        window_days=window_days,
        mark=mark,
        trigger=trigger,
        actor=actor,
        note=note,
    )
    if batch.status in {"ready", "active", "superseded"}:
        return _payload(db, batch, created=False, reused=True)
    run(db, batch, commit_chunks=True)
    return _payload(db, batch, created=created, reused=not created)


def resume(db: Session, batch: RfmBatch) -> dict:
    if batch.status in {"ready", "active", "superseded"}:
        return _payload(db, batch, created=False, reused=True)
    run(db, batch, commit_chunks=True)
    return _payload(db, batch, created=False, reused=True)


def activate(db: Session, batch: RfmBatch, *, actor: str) -> dict:
    """核对人数与购买率后，一次性把候选批次切换为线上正式口径。

    整个写入在调用方事务内：任何一步失败都 rollback，线上旧版仍可用。
    """
    if batch.status != "ready":
        raise AppError("CONFLICT", 409, {"reason": "只有 ready 状态的候选批次可以切换", "status": batch.status})
    segment_rows = batch_segments(db, batch.id)
    member_count = db.scalar(
        select(func.count())
        .select_from(RfmBatchMember)
        .where(RfmBatchMember.batch_id == batch.id, RfmBatchMember.segment_code.is_not(None))
    ) or 0
    _verify(batch, member_count, segment_rows)

    previous = active_batch(db)
    previous_no = previous.batch_no if previous else None
    existing_codes = {row.code: row for row in db.scalars(select(Segment)).all()}

    now = utcnow()
    payload = []
    offset = 0
    chunk = BATCH_CHUNK_SIZE
    while True:
        rows = db.scalars(
            select(RfmBatchMember)
            .where(RfmBatchMember.batch_id == batch.id, RfmBatchMember.segment_code.is_not(None))
            .order_by(RfmBatchMember.member_id)
            .limit(chunk)
            .offset(offset)
        ).all()
        if not rows:
            break
        for row in rows:
            last_order = None
            if row.r_days < NO_ORDER_RECENCY_DAYS:
                last_order = datetime.combine(batch.as_of - timedelta(days=int(row.r_days)), datetime.min.time())
            payload.append(
                {
                    "id": row.member_id,
                    "r_days": row.r_days,
                    "orders_window": row.orders,
                    "amount_window": row.amount,
                    "r_score": row.r_score,
                    "f_score": row.f_score,
                    "m_score": row.m_score,
                    "composite_score": row.composite_score,
                    "segment_code": row.segment_code,
                    "scored_at": now,
                    "last_order_at": last_order,
                }
            )
        offset += chunk
        if len(rows) < chunk:
            break
    if not payload:
        raise AppError("CONFLICT", 409, {"reason": "没有可回写的会员评分", "batch_no": batch.batch_no})
    for start in range(0, len(payload), chunk):
        db.execute(update(Member), payload[start : start + chunk])

    as_of_dt = datetime.combine(batch.as_of, datetime.min.time())
    snapshot = {
        "batch_no": batch.batch_no,
        "weights": list(batch.weights or []),
        "window_days": batch.window_days,
        "thresholds": batch.thresholds or {},
        "global_purchase_rate": round(batch.global_purchase_rate or 0.0, 6),
        "as_of": batch.as_of.isoformat() if batch.as_of else None,
        "members": batch.member_count,
        "orders": batch.orders_total,
        "thresholds_basis": "batch",
        "watermark": {
            "orders": batch.order_count,
            "max_order_id": batch.order_max_id,
            "max_paid_at": iso_utc(batch.order_max_paid_at),
        },
    }
    for item in segment_rows:
        segment = existing_codes.get(item.code)
        if segment is None:
            segment = Segment(code=item.code, name=item.name, sort_order=item.sort_order)
            db.add(segment)
            existing_codes[item.code] = segment
        segment.name = item.name
        segment.rule_text = item.rule_text
        segment.sort_order = item.sort_order
        segment.member_count = item.member_count
        segment.share = item.share
        segment.avg_r_score = item.avg_r_score
        segment.avg_f_score = item.avg_f_score
        segment.avg_m_score = item.avg_m_score
        segment.avg_r_days = item.avg_r_days
        segment.avg_amount_window = item.avg_amount_window
        segment.purchase_rate = item.purchase_rate
        segment.avg_ticket = item.avg_ticket
        segment.lift = item.lift
        segment.as_of = as_of_dt
        segment.snapshot = snapshot

    if previous is not None and previous.id != batch.id:
        previous.status = "superseded"
    batch.status = "active"
    batch.activated_at = now
    batch.activated_by = actor
    batch.switched_from = previous_no
    db.flush()
    return {
        "batch": serialize_batch(batch, is_active=True),
        "switched_from": previous_no,
        "member_count": len(payload),
        "segments": [serialize_batch_segment(row) for row in segment_rows],
    }


def available_members(db: Session, batch: RfmBatch, segment_code: str, limit: int) -> list[RfmBatchMember]:
    """名单生成从「生成时的正式批次」里取会员，保证导出与批次口径一致。"""
    return list(
        db.scalars(
            select(RfmBatchMember)
            .where(RfmBatchMember.batch_id == batch.id, RfmBatchMember.segment_code == segment_code)
            .order_by(RfmBatchMember.composite_score.desc().nullslast(), RfmBatchMember.member_id)
            .limit(limit)
        ).all()
    )


def ensure_initial(db: Session, *, actor: str = "system", trigger: str = "seed") -> dict | None:
    """线上还没有正式批次时，用当前线上参数建第一个批次并直接切换（幂等）。"""
    if active_batch(db) is not None:
        return None
    snapshot_rows = db.scalars(select(Segment).order_by(Segment.sort_order)).all()
    if not snapshot_rows:
        return None
    snapshot = next((row.snapshot for row in snapshot_rows if row.snapshot), {}) or {}
    weights = tuple(float(item) for item in snapshot.get("weights") or DEFAULT_RFM_WEIGHTS)
    window_days = int(snapshot.get("window_days") or RFM_WINDOW_DAYS)
    result = recompute(
        db, weights=weights, window_days=window_days, trigger=trigger, actor=actor, note="初始化正式口径批次"
    )
    batch = get_batch(db, result["id"])
    return activate(db, batch, actor=actor)
