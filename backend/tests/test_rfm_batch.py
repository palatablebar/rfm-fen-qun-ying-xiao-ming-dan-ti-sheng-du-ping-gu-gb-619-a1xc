"""RFM 重算批次集成测试（SQLite 内存库，覆盖续跑 / 复用 / 切换 / 名单冻结）。

这些用例直接跑在 ORM 层，不依赖 Postgres / Redis；`Bucket` 用真实订单事实
驱动 R/F/M，口径与线上 SQL 版本一致（窗口 [as_of-window, as_of]）。
"""
from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from src.models import Base
from src.models.audience import Audience
from src.models.campaign import Campaign
from src.models.member import Member
from src.models.order import Order
from src.models.rfm_batch import RfmBatch, RfmBatchMember, RfmBatchSegment
from src.models.segment import Segment
from src.services import audience_service, rfm_batch_service
from src.utils.errors import AppError

AS_OF = date(2026, 11, 14)


@pytest.fixture()
def db():
    engine = create_engine("sqlite://", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    _seed_members(session)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _seed_members(db, count: int = 60) -> None:
    rng = random.Random(7)
    for index in range(1, count + 1):
        member = Member(
            member_no=f"MY{index:05d}",
            name=f"会员{index}",
            phone=f"1380000{index:04d}",
            city="上海",
            tier="gold" if index % 5 == 0 else "normal",
            joined_on=date(2025, 1, 1),
            purchase_rate=round(rng.uniform(0.01, 0.08), 6),
            avg_ticket=round(rng.uniform(18, 40), 2),
        )
        db.add(member)
        db.flush()
        # 每人 0~8 单，最近 400 天内
        for order_index in range(rng.randint(0, 8)):
            paid_at = datetime.combine(AS_OF - timedelta(days=rng.randint(0, 400)), time(12, 0))
            db.add(
                Order(
                    order_no=f"MYO{index:04d}{order_index:02d}",
                    member_id=member.id,
                    store_code="MY-S01",
                    channel="store",
                    amount=round(rng.uniform(12, 60), 2),
                    items=1,
                    paid_at=paid_at,
                )
            )
    db.commit()


def _active_snapshot(db) -> dict:
    active = rfm_batch_service.active_batch(db)
    assert active is not None
    return rfm_batch_service.serialize_batch(active, is_active=True)


def test_recompute_creates_candidate_without_touching_live(db):
    batch = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    assert batch["status"] == "ready"
    assert batch["created"] is True
    assert batch["member_count"] == 60
    # 候选批次不写线上会员与分群
    assert db.scalar(select(func.count()).select_from(Segment)) == 0
    assert db.scalar(select(func.count()).select_from(Member).where(Member.segment_code.is_not(None))) == 0
    segments = rfm_batch_service.batch_segments(db, batch["id"])
    assert len(segments) == 8
    assert sum(item.member_count for item in segments) == 60


def test_same_watermark_and_weights_reuses_batch(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    second = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    assert second["batch_no"] == first["batch_no"]
    assert second["reused"] is True
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 1
    assert db.scalar(select(func.count()).select_from(RfmBatchMember)) == 60


def test_different_weights_create_distinct_batch(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    assert second["batch_no"] != first["batch_no"]
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 2


def test_order_backfill_moves_watermark_and_creates_new_batch(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    member_id = db.scalar(select(Member.id).order_by(Member.id))
    db.add(
        Order(
            order_no="MYO-BACKFILL-01",
            member_id=member_id,
            store_code="MY-S01",
            channel="store",
            amount=88.0,
            items=2,
            paid_at=datetime.combine(AS_OF - timedelta(days=1), time(9, 0)),
        )
    )
    db.commit()
    second = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    assert second["batch_no"] != first["batch_no"]
    assert second["created"] is True
    assert second["watermark"]["orders"] == first["watermark"]["orders"] + 1


def test_resume_from_processed_position(db, monkeypatch):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    batch = rfm_batch_service.get_batch(db, first["id"])
    # 模拟在 score 阶段中途失败：回退 stage，保留已处理游标与已写入明细
    batch.status = "failed"
    batch.stage = "score"
    batch.score_cursor = 20
    batch.thresholds = {}
    batch.error = "simulated failure"
    # 清掉游标之后（尚未处理）的打分，游标之前保留，模拟中断在半途
    for row in db.scalars(
        select(RfmBatchMember).where(
            RfmBatchMember.batch_id == batch.id,
            RfmBatchMember.member_id > batch.score_cursor,
        )
    ).all():
        row.r_score = None
        row.f_score = None
        row.m_score = None
        row.composite_score = None
        row.segment_code = None
    batch.processed_members = 0
    db.commit()

    resumed = rfm_batch_service.resume(db, batch)
    assert resumed["status"] == "ready"
    assert resumed["member_count"] == 60
    # 续跑没有重复插入会员快照
    assert db.scalar(select(func.count()).select_from(RfmBatchMember).where(RfmBatchMember.batch_id == batch.id)) == 60
    again = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    assert again["batch_no"] == resumed["batch_no"]
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 1


def test_resume_after_failure_keeps_cursor_and_does_not_duplicate(db):
    batch = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    record = rfm_batch_service.get_batch(db, batch["id"])
    # 模拟 collect 处理完前 25 人后进程崩溃：游标与已写入明细都在，阶段仍停在 collect
    kept = db.scalars(
        select(RfmBatchMember)
        .where(RfmBatchMember.batch_id == record.id)
        .order_by(RfmBatchMember.member_id)
        .limit(25)
    ).all()
    kept_ids = {row.member_id for row in kept}
    db.execute(
        RfmBatchMember.__table__.delete().where(
            RfmBatchMember.batch_id == record.id,
            RfmBatchMember.member_id.notin_(kept_ids),
        )
    )
    record.status = "failed"
    record.stage = "collect"
    record.raw_cursor = max(kept_ids)
    record.score_cursor = 0
    record.thresholds = {}
    record.error = "worker died mid-collect"
    for row in kept:
        # 崩溃发生在 collect 阶段，尚未进入打分
        row.r_score = row.f_score = row.m_score = None
        row.composite_score = None
        row.segment_code = None
    db.commit()
    assert db.scalar(
        select(func.count()).select_from(RfmBatchMember).where(RfmBatchMember.batch_id == record.id)
    ) == 25

    resumed = rfm_batch_service.resume(db, db.get(RfmBatch, record.id))
    assert resumed["status"] == "ready"
    assert resumed["member_count"] == 60
    total = db.scalar(
        select(func.count()).select_from(RfmBatchMember).where(RfmBatchMember.batch_id == record.id)
    )
    assert total == 60
    # 续跑后同一水位 + 权重仍然复用同一批次，不新增
    again = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    assert again["batch_no"] == resumed["batch_no"]
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 1


def test_activate_switches_live_and_keeps_old_version(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    result = rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    db.commit()
    assert result["switched_from"] is None
    assert db.scalar(select(func.count()).select_from(Segment)) == 8
    scored = db.scalar(select(func.count()).select_from(Member).where(Member.segment_code.is_not(None)))
    assert scored == 60
    snapshot = _active_snapshot(db)
    assert snapshot["batch_no"] == first["batch_no"]
    # 线上人数与批次一致
    live_total = db.scalar(select(func.sum(Segment.member_count)))
    assert live_total == first["member_count"]

    # 新权重重算 + 切换：旧批次保留为 superseded，不丢记录
    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    switched = rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, second["id"]), actor="admin@rfmseg.io")
    db.commit()
    assert switched["switched_from"] == first["batch_no"]
    old = db.scalar(select(RfmBatch).where(RfmBatch.batch_no == first["batch_no"]))
    assert old.status == "superseded"
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 2


def test_activate_failure_rolls_back_and_keeps_old_version(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    db.commit()
    live_before = db.scalar(select(func.sum(Segment.member_count)))

    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    candidate = rfm_batch_service.get_batch(db, second["id"])
    # 损坏候选批次：八类人数之和与会员数对不上，切换必须被拒绝
    segment_row = db.scalar(select(RfmBatchSegment).where(RfmBatchSegment.batch_id == candidate.id))
    segment_row.member_count = int(segment_row.member_count or 0) + 1
    db.commit()
    with pytest.raises(AppError) as exc:
        rfm_batch_service.activate(db, candidate, actor="admin@rfmseg.io")
    db.rollback()
    assert exc.value.status_code == 409
    # 线上口径完全没变，旧批次仍是 active
    assert db.scalar(select(func.sum(Segment.member_count))) == live_before
    assert _active_snapshot(db)["batch_no"] == first["batch_no"]
    assert db.get(RfmBatch, candidate.id).status == "ready"


def test_audience_freezes_batch_and_export_ignores_later_recompute(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    campaign = Campaign(code="autumn", name="秋日会员日券", budget=500.0, status="active", reach_cost=0.15)
    db.add(campaign)
    db.commit()
    audience = audience_service.generate(db, campaign=campaign, budget=500.0, operator_email="seed@rfmseg.io")
    db.commit()
    assert audience.batch_no == first["batch_no"]
    assert audience.members
    frozen_count = len(audience.members)
    assert frozen_count == audience.member_count
    frozen_nos = {item["member_no"] for item in audience.members}

    from src.services import export_service

    csv_before = export_service.audience_csv(db, audience.id).decode("utf-8")

    # 后续重算并切换：已确认名单及其导出明细不得改动
    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, second["id"]), actor="admin@rfmseg.io")
    db.commit()
    refreshed = db.get(Audience, audience.id)
    assert refreshed.batch_no == first["batch_no"]
    assert {item["member_no"] for item in refreshed.members} == frozen_nos
    csv_after = export_service.audience_csv(db, audience.id).decode("utf-8")
    assert csv_before == csv_after
    assert first["batch_no"] in csv_after


def test_batch_diff_against_active(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    db.commit()
    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    detail = rfm_batch_service.batch_detail(db, rfm_batch_service.get_batch(db, second["id"]))
    diff = detail["diff"]
    assert diff["current_batch_no"] == first["batch_no"]
    assert diff["candidate_batch_no"] == second["batch_no"]
    assert sum(item["candidate_member_count"] for item in diff["segments"]) == second["member_count"]
    assert sum(item["current_member_count"] for item in diff["segments"]) == first["member_count"]


def test_batch_matches_direct_compute_exactly(db):
    """批次结果必须与直接调用 rfm_service.compute() 逐位一致。"""
    from src.services import rfm_service

    batch = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    as_of = date.fromisoformat(batch["as_of"])
    members = db.scalars(select(Member).order_by(Member.id)).all()
    since = datetime.combine(as_of - timedelta(days=365), time.min)
    until = datetime.combine(as_of, time.max)
    payload = []
    for member in members:
        rows = db.execute(
            select(func.count(Order.id), func.sum(Order.amount), func.max(Order.paid_at))
            .where(Order.member_id == member.id)
            .where(Order.paid_at >= since)
            .where(Order.paid_at <= until)
        ).one()
        latest = rows[2]
        payload.append(
            {
                "id": member.id,
                "member_no": member.member_no,
                "last_order_on": latest.date() if isinstance(latest, datetime) else None,
                "orders": int(rows[0] or 0),
                "amount": round(float(rows[1] or 0.0), 2),
                "purchase_rate": member.purchase_rate,
                "avg_ticket": member.avg_ticket,
            }
        )
    direct = rfm_service.compute(payload, as_of=as_of, weights=(0.3, 0.3, 0.4), window_days=365)
    batch_segments = {row.code: row for row in rfm_batch_service.batch_segments(db, batch["id"])}
    batch_members = db.scalars(
        select(RfmBatchMember).where(RfmBatchMember.batch_id == batch["id"]).order_by(RfmBatchMember.member_id)
    ).all()
    direct_by_id = {row["id"]: row for row in direct["members"]}
    for row in batch_members:
        target = direct_by_id[row.member_id]
        for key in ("r_days", "orders", "amount", "r_score", "f_score", "m_score", "segment_code", "composite_score"):
            assert getattr(row, key) == target[key], (row.member_id, key, getattr(row, key), target[key])
    for item in direct["segments"]:
        row = batch_segments[item["code"]]
        assert row.member_count == item["member_count"]
        assert row.purchase_rate == item["purchase_rate"]
        assert row.lift == item["lift"]
        assert row.avg_r_score == item["avg_r_score"]
        assert row.avg_amount_window == item["avg_amount_window"]
    assert batch["global_purchase_rate"] == direct["global_purchase_rate"]
    assert batch["member_count"] == direct["totals"]["members"]
    assert batch["orders_total"] == direct["totals"]["orders"]
    assert batch["amount_total"] == direct["totals"]["amount"]


def test_activate_write_failure_rolls_back_to_old_version(db, monkeypatch):
    """切换写入中途异常时必须整体回滚，旧批次与线上口径保持不变，且不丢记录。"""
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    db.commit()
    live_before = {
        row.code: row.member_count for row in db.scalars(select(Segment)).all()
    }
    member_before = {
        row.id: row.segment_code for row in db.scalars(select(Member)).all()
    }

    second = rfm_batch_service.recompute(db, weights=(0.6, 0.2, 0.2), window_days=365)
    candidate = rfm_batch_service.get_batch(db, second["id"])
    calls = {"n": 0}
    original = db.execute

    def flaky(statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if table is not None and getattr(table, "name", None) == "members" and args:
            calls["n"] += 1
            if calls["n"] >= 1:
                raise RuntimeError("disk full while writing member scores")
        return original(statement, *args, **kwargs)

    monkeypatch.setattr(db, "execute", flaky)
    with pytest.raises(RuntimeError):
        rfm_batch_service.activate(db, candidate, actor="admin@rfmseg.io")
    db.rollback()
    monkeypatch.setattr(db, "execute", original)

    # 线上口径与会员分群保持旧版
    assert {row.code: row.member_count for row in db.scalars(select(Segment)).all()} == live_before
    assert {row.id: row.segment_code for row in db.scalars(select(Member)).all()} == member_before
    assert _active_snapshot(db)["batch_no"] == first["batch_no"]
    # 候选批次记录仍在，可再次尝试切换
    assert db.get(RfmBatch, candidate.id).status == "ready"
    assert db.get(RfmBatch, candidate.id).error is None


def test_different_window_same_watermark_creates_new_batch(db):
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    second = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=180)
    assert second["batch_no"] != first["batch_no"]
    assert db.scalar(select(func.count()).select_from(RfmBatch)) == 2


def test_generate_fails_closed_when_batch_members_missing(db):
    """正式批次会员明细与配额不符时拒绝出名单，避免导出与批次口径对不上。"""
    first = rfm_batch_service.recompute(db, weights=(0.3, 0.3, 0.4), window_days=365)
    rfm_batch_service.activate(db, rfm_batch_service.get_batch(db, first["id"]), actor="admin@rfmseg.io")
    # 从可投分群（lift > 1 且有人）删掉一条会员快照，配额仍按批次汇总算
    eligible = next(
        row.code
        for row in rfm_batch_service.batch_segments(db, first["id"])
        if row.member_count > 0 and row.lift > 1
    )
    victim = db.scalars(
        select(RfmBatchMember).where(
            RfmBatchMember.batch_id == first["id"], RfmBatchMember.segment_code == eligible
        ).limit(1)
    ).one()
    db.delete(victim)
    campaign = Campaign(code="guard", name="守护活动", budget=100000.0, status="active", reach_cost=0.15)
    db.add(campaign)
    db.commit()
    with pytest.raises(AppError) as exc:
        audience_service.generate(db, campaign=campaign, budget=100000.0, operator_email="admin@rfmseg.io")
    assert exc.value.status_code == 409
    assert db.scalar(select(func.count()).select_from(Audience)) == 0
