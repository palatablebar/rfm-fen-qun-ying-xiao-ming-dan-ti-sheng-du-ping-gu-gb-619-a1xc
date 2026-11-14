"""RFM 重算批次：独立于线上口径的候选结果与可追溯的正式版本。

一次重算 = 一个批次。批次用「订单水位 + 权重 + 窗口」指纹做自然键：
同一水位和权重重跑只会复用已有批次（不新增、不重复），失败批次从游标续跑。

批次落库的是**会员级评分快照 + 八类汇总**，线上 `members`/`segments`
只会在管理员确认「切换正式口径」时被一次性覆盖。名单生成时固定引用
当时的正式批次号（`audiences.batch_no`），所以后续重算不会改动
已确认名单及其导出明细。
"""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import JSON, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models import Base
from src.utils.timeutil import utcnow


class RfmBatch(Base):
    __tablename__ = "rfm_batches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # B<快照日>-<指纹前 10 位>，由水位与参数确定性生成，重跑不新增
    batch_no: Mapped[str] = mapped_column(String(40), unique=True, index=True)
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="running", index=True)
    stage: Mapped[str] = mapped_column(String(16), default="collect")

    as_of: Mapped[date] = mapped_column(Date, index=True)
    window_days: Mapped[int] = mapped_column(Integer, default=365)
    weights: Mapped[list] = mapped_column(JSON, default=list)

    # 订单水位（水位变了就是新批次；同一水位重跑复用本批次）
    order_count: Mapped[int] = mapped_column(Integer, default=0)
    order_max_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    order_max_paid_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    global_purchase_rate: Mapped[float] = mapped_column(Float, default=0.0)
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    orders_total: Mapped[int] = mapped_column(Integer, default=0)
    amount_total: Mapped[float] = mapped_column(Float, default=0.0)

    # 续跑游标：collect 阶段按 member.id 递增，score 阶段按 member_id 递增
    raw_cursor: Mapped[int] = mapped_column(Integer, default=0)
    score_cursor: Mapped[int] = mapped_column(Integer, default=0)
    processed_members: Mapped[int] = mapped_column(Integer, default=0)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trigger: Mapped[str] = mapped_column(String(16), default="manual")
    created_by: Mapped[str] = mapped_column(String(160), default="system")
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    # 切换审计：本批次替换掉哪一版正式口径
    switched_from: Mapped[str | None] = mapped_column(String(40), nullable=True)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    activated_by: Mapped[str | None] = mapped_column(String(160), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class RfmBatchSegment(Base):
    """批次内的八类分群汇总，切换正式口径时整批搬运到线上 `segments`。"""

    __tablename__ = "rfm_batch_segments"
    __table_args__ = (UniqueConstraint("batch_id", "code", name="uq_rfm_batch_segment"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("rfm_batches.id", ondelete="CASCADE"), index=True)
    code: Mapped[str] = mapped_column(String(20), index=True)
    name: Mapped[str] = mapped_column(String(40))
    rule_text: Mapped[str] = mapped_column(String(120), default="")
    sort_order: Mapped[int] = mapped_column(Integer, default=0)

    member_count: Mapped[int] = mapped_column(Integer, default=0)
    share: Mapped[float] = mapped_column(Float, default=0.0)
    avg_r_score: Mapped[float] = mapped_column(Float, default=0.0)
    avg_f_score: Mapped[float] = mapped_column(Float, default=0.0)
    avg_m_score: Mapped[float] = mapped_column(Float, default=0.0)
    avg_r_days: Mapped[float] = mapped_column(Float, default=0.0)
    avg_amount_window: Mapped[float] = mapped_column(Float, default=0.0)
    purchase_rate: Mapped[float] = mapped_column(Float, default=0.0)
    avg_ticket: Mapped[float] = mapped_column(Float, default=0.0)
    lift: Mapped[float] = mapped_column(Float, default=0.0)


class RfmBatchMember(Base):
    """批次内的会员级评分快照：候选人核对、正式切换、名单导出都以它为准。"""

    __tablename__ = "rfm_batch_members"
    __table_args__ = (
        UniqueConstraint("batch_id", "member_id", name="uq_rfm_batch_member"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("rfm_batches.id", ondelete="CASCADE"), index=True)
    member_id: Mapped[int] = mapped_column(Integer, index=True)
    member_no: Mapped[str] = mapped_column(String(24), default="")
    # 会员展示字段也随批次冻结，名单导出不受后续主档修改影响
    name: Mapped[str] = mapped_column(String(40), default="")
    phone: Mapped[str] = mapped_column(String(20), default="")
    city: Mapped[str] = mapped_column(String(24), default="")
    tier: Mapped[str] = mapped_column(String(12), default="normal")

    r_days: Mapped[int] = mapped_column(Integer, default=0)
    orders: Mapped[int] = mapped_column(Integer, default=0)
    amount: Mapped[float] = mapped_column(Float, default=0.0)
    r_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    f_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    m_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    composite_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    segment_code: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    purchase_rate: Mapped[float] = mapped_column(Float, default=0.0)
    avg_ticket: Mapped[float] = mapped_column(Float, default=0.0)
