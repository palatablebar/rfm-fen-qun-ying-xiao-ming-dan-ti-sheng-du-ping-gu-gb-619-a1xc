from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from src.config.database import get_db
from src.constants.roles import SEGMENT_ADMIN_ROLES
from src.controllers import segment_controller
from src.middlewares.rbac import READ_ROLES, require_roles
from src.schemas.segment import RecomputeIn, SegmentPatch

router = APIRouter(prefix="/segments", tags=["segments"])


@router.get("")
def list_segments(
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*READ_ROLES)),
) -> dict:
    return segment_controller.list_segments(db)


@router.post("/recompute")
def recompute(
    payload: RecomputeIn,
    request: Request,
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*SEGMENT_ADMIN_ROLES)),
) -> dict:
    """重算到独立候选批次；线上口径在切换前保持不变。"""
    return segment_controller.recompute(db, payload.model_dump(), principal, request)


@router.get("/batches")
def list_batches(
    limit: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*READ_ROLES)),
) -> dict:
    return segment_controller.list_batches(db, limit=limit)


@router.get("/batches/{batch_id}")
def get_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*READ_ROLES)),
) -> dict:
    return segment_controller.get_batch(db, batch_id)


@router.post("/batches/{batch_id}/resume")
def resume_batch(
    batch_id: int,
    request: Request,
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*SEGMENT_ADMIN_ROLES)),
) -> dict:
    """失败批次从已处理位置续跑。"""
    return segment_controller.resume_batch(db, batch_id, principal, request)


@router.post("/batches/{batch_id}/activate")
def activate_batch(
    batch_id: int,
    request: Request,
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*SEGMENT_ADMIN_ROLES)),
) -> dict:
    """核对人数与购买率后一次性切换线上口径；失败保留旧版。"""
    return segment_controller.activate_batch(db, batch_id, principal, request)


@router.patch("/{segment_id}")
def update_segment(
    segment_id: int,
    payload: SegmentPatch,
    request: Request,
    db: Session = Depends(get_db),
    principal: dict = Depends(require_roles(*SEGMENT_ADMIN_ROLES)),
) -> dict:
    return segment_controller.update_segment(db, segment_id, payload.model_dump(exclude_unset=True), principal, request)
