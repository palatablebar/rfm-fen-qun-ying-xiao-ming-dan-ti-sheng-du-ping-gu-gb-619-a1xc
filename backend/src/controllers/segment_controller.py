"""分群控制器：重算批次、候选核对、一键切换正式口径，均落审计。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.services import audit_service, segment_service


def list_segments(db: Session) -> dict:
    return segment_service.list_segments(db)


def recompute(db: Session, payload: dict, principal: dict, request: Request) -> dict:
    weights = (
        float(payload.get("weight_r", 0.3)),
        float(payload.get("weight_f", 0.3)),
        float(payload.get("weight_m", 0.4)),
    )
    result = segment_service.recompute(
        db,
        weights=weights,
        window_days=int(payload.get("window_days", 365)),
        trigger=payload.get("trigger") or "manual",
        actor=principal["email"],
    )
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="segment.recompute",
        entity="rfm_batch",
        entity_id=result["id"],
        detail={
            "batch_no": result["batch_no"],
            "weights": list(weights),
            "window_days": result["window_days"],
            "as_of": result["as_of"],
            "members": result["member_count"],
            "thresholds": result["thresholds"],
            "reused": result.get("reused", False),
            "watermark": result.get("watermark"),
        },
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return result


def list_batches(db: Session, limit: int = 30) -> dict:
    return segment_service.list_batches(db, limit=limit)


def get_batch(db: Session, batch_id: int) -> dict:
    return segment_service.batch_detail(db, batch_id)


def resume_batch(db: Session, batch_id: int, principal: dict, request: Request) -> dict:
    result = segment_service.resume_batch(db, batch_id)
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="segment.batch.resume",
        entity="rfm_batch",
        entity_id=batch_id,
        detail={"batch_no": result["batch_no"], "stage": result.get("stage"), "status": result.get("status")},
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return result


def activate_batch(db: Session, batch_id: int, principal: dict, request: Request) -> dict:
    # 切换的所有写入都在本事务里；失败由调用方 rollback，旧版本保留
    result = segment_service.activate_batch(db, batch_id, actor=principal["email"])
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="segment.activate",
        entity="rfm_batch",
        entity_id=batch_id,
        detail={
            "batch_no": result["batch"]["batch_no"],
            "switched_from": result.get("switched_from"),
            "member_count": result.get("member_count"),
            "segments": [
                {"code": item["code"], "members": item["member_count"], "purchase_rate": item["purchase_rate"]}
                for item in result.get("segments", [])
            ],
        },
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return result


def update_segment(db: Session, segment_id: int, payload: dict, principal: dict, request: Request) -> dict:
    segment = segment_service.get_segment(db, segment_id)
    segment_service.update_segment(db, segment, payload)
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="segment.update",
        entity="segment",
        entity_id=segment.id,
        detail={"fields": list(payload.keys()), "code": segment.code},
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return segment_service.serialize(segment)
