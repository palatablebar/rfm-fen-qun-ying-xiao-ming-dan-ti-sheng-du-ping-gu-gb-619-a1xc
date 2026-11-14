"""算法 A｜RFM 评分与八类规则分群（纯 Python，确定性）。

口径（README §10 与页面「RFM 分群」同步展示）：

1. 单人指标（以快照日 as_of 为基准）：
   - R = as_of − 最近一次下单日期（天）；从未下单记 9999。
   - F = 窗口内（默认近 180 天）订单数。
   - M = 窗口内订单金额合计。
2. 分位切档：对全量会员的 R / F / M 各算 20/40/60/80 分位（线性插值），
   按值落在第几档给 1-5 分；**同值必然同档**（只与分位阈值比较，不做名次排序）。
   R 越小越好 → 档位反向（最低一档给 5 分）。
3. 综合分 = w_r·R + w_f·F + w_m·M（默认 0.3/0.3/0.4，三项之和必须为 1）。
4. 八类规则分群：按 SEGMENT_ORDER 顺序首次命中（规则文本见 constants/enums.py）。
5. 分群购买率 = 群内会员历史券响应率均值；大盘购买率 = 全量均值；
   lift = 分群购买率 / 大盘购买率（保留 2 位）。
"""
from __future__ import annotations

import math
from bisect import bisect_left
from datetime import date, datetime

from sqlalchemy.orm import Session

from src.constants.enums import (
    SEGMENT_LABELS,
    SEGMENT_ORDER,
    SEGMENT_RULES,
    SegmentCode,
)
from src.constants.limits import (
    DEFAULT_RFM_WEIGHTS,
    MAX_RFM_WEIGHTS,
    MIN_RFM_WEIGHTS,
    NO_ORDER_RECENCY_DAYS,
    RFM_BUCKETS,
    RFM_WINDOW_DAYS,
    SCORE_SPLIT,
    WEIGHT_SUM_TOLERANCE,
)
from src.utils.errors import AppError


def validate_weights(weights: tuple[float, float, float]) -> tuple[float, float, float]:
    cleaned = tuple(float(item) for item in weights)
    if len(cleaned) != 3:
        raise AppError("VALIDATION", 422, {"reason": "需要三个权重"})
    for item in cleaned:
        if not math.isfinite(item) or item < MIN_RFM_WEIGHTS or item > MAX_RFM_WEIGHTS:
            raise AppError("VALIDATION", 422, {"reason": "权重必须在 0~1 之间", "weights": cleaned})
    if abs(sum(cleaned) - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise AppError("VALIDATION", 422, {"reason": "三项权重之和必须等于 1.0", "weights": cleaned})
    return cleaned  # type: ignore[return-value]


def percentiles(values: list[float], buckets: int = RFM_BUCKETS) -> list[float]:
    """线性插值分位阈值（共 buckets-1 个切点），空输入返回全 0。"""
    ordered = sorted(float(item) for item in values)
    if not ordered:
        return [0.0] * (buckets - 1)
    last = len(ordered) - 1
    thresholds: list[float] = []
    for index in range(1, buckets):
        position = last * index / buckets
        low = int(math.floor(position))
        high = min(low + 1, last)
        frac = position - low
        thresholds.append(round(ordered[low] + (ordered[high] - ordered[low]) * frac, 6))
    return thresholds


def bucket_index(value: float, thresholds: list[float]) -> int:
    """返回 0..buckets-1 的档位（同值同档：只与阈值比较）。"""
    return bisect_left(thresholds, float(value))


def score_of(value: float, thresholds: list[float], reverse: bool = False, buckets: int = RFM_BUCKETS) -> int:
    index = bucket_index(value, thresholds)
    return (buckets - index) if reverse else (index + 1)


def classify(r_score: int, f_score: int, m_score: int) -> str:
    """八类规则分群，顺序即优先级（首次命中）。规则文本见 constants/enums.py。"""
    split = SCORE_SPLIT
    if r_score >= 4 and f_score >= 4 and m_score >= 4:
        return SegmentCode.CHAMPION.value
    if r_score >= split and f_score >= 4 and m_score >= split:
        return SegmentCode.LOYAL.value
    if r_score >= 4 and f_score >= split and m_score >= 4:
        return SegmentCode.POTENTIAL.value
    if r_score <= 2 and f_score >= 4 and m_score >= 4:
        return SegmentCode.AT_RISK.value
    if r_score >= 4 and f_score <= 2:
        return SegmentCode.NEW.value
    if r_score <= 2 and f_score >= 2 and m_score >= 2:
        return SegmentCode.CHURN_WARN.value
    if r_score >= split and m_score <= split:
        return SegmentCode.ORDINARY.value
    return SegmentCode.HIBERNATING.value


def _recency_days(last_order_on: date | None, as_of: date) -> int:
    if last_order_on is None:
        return NO_ORDER_RECENCY_DAYS
    return max(0, (as_of - last_order_on).days)


def recency_days(last_order_on: date | None, as_of: date) -> int:
    """公开别名：批次重算与直接计算共用同一 R 口径。"""
    return _recency_days(last_order_on, as_of)


def compute(
    members: list[dict],
    *,
    as_of: date,
    weights: tuple[float, float, float] = DEFAULT_RFM_WEIGHTS,
    window_days: int = RFM_WINDOW_DAYS,
) -> dict:
    """输入会员明细，返回打分 + 分群聚合（不碰数据库，便于单测与确定性校验）。"""
    weights = validate_weights(weights)
    if not members:
        return {
            "as_of": as_of.isoformat(),
            "window_days": window_days,
            "weights": list(weights),
            "buckets": RFM_BUCKETS,
            "thresholds": {"r": [], "f": [], "m": []},
            "global_purchase_rate": 0.0,
            "members": [],
            "segments": _empty_segments(),
            "totals": {"members": 0, "orders": 0, "amount": 0.0},
            "status": "partial",
            "log": "没有可评分的会员，未生成分群",
        }

    recency = [_recency_days(item.get("last_order_on"), as_of) for item in members]
    freq = [int(item.get("orders") or 0) for item in members]
    money = [round(float(item.get("amount") or 0.0), 2) for item in members]

    thresholds_r = percentiles(recency)
    thresholds_f = percentiles(freq)
    thresholds_m = percentiles(money)

    scored: list[dict] = []
    for index, item in enumerate(members):
        r_score = score_of(recency[index], thresholds_r, reverse=True)
        f_score = score_of(freq[index], thresholds_f)
        m_score = score_of(money[index], thresholds_m)
        composite = round(weights[0] * r_score + weights[1] * f_score + weights[2] * m_score, 2)
        code = classify(r_score, f_score, m_score)
        row = {
            "id": item.get("id"),
            "member_no": item.get("member_no"),
            "r_days": recency[index],
            "orders": freq[index],
            "amount": money[index],
            "r_score": r_score,
            "f_score": f_score,
            "m_score": m_score,
            "composite_score": composite,
            "segment_code": code,
            "purchase_rate": round(float(item.get("purchase_rate") or 0.0), 6),
            "avg_ticket": round(float(item.get("avg_ticket") or 0.0), 2),
        }
        scored.append(row)

    aggregate_result = aggregate(scored, weights=weights, window_days=window_days)
    return {
        "as_of": as_of.isoformat(),
        "window_days": window_days,
        "weights": list(weights),
        "buckets": RFM_BUCKETS,
        "thresholds": {
            "r": [round(item, 2) for item in thresholds_r],
            "f": [round(item, 2) for item in thresholds_f],
            "m": [round(item, 2) for item in thresholds_m],
        },
        "members": scored,
        **aggregate_result,
        "status": "success",
    }


def aggregate(scored: list[dict], *, weights: tuple[float, float, float], window_days: int) -> dict:
    """由已打分的会员明细汇总八类分群、大盘购买率与口径说明。

    `compute()` 与批次重算（`rfm_batch_service`）共用这一段，
    保证候选批次与直接计算得到的人数 / 购买率逐位一致。
    """
    total_members = len(scored)
    if total_members == 0:
        return {
            "global_purchase_rate": 0.0,
            "segments": _empty_segments(),
            "totals": {"members": 0, "orders": 0, "amount": 0.0},
            "log": "没有可评分的会员，未生成分群",
        }
    global_rate = round(sum(row["purchase_rate"] for row in scored) / total_members, 6)
    groups: dict[str, list[dict]] = {code: [] for code in SEGMENT_ORDER}
    for row in scored:
        groups[row["segment_code"]].append(row)
    segments = []
    for code in SEGMENT_ORDER:
        bucket = groups[code]
        count = len(bucket)
        if count:
            rate = sum(row["purchase_rate"] for row in bucket) / count
            avg_ticket = sum(row["avg_ticket"] for row in bucket) / count
            avg_r = sum(row["r_score"] for row in bucket) / count
            avg_f = sum(row["f_score"] for row in bucket) / count
            avg_m = sum(row["m_score"] for row in bucket) / count
            avg_days = sum(row["r_days"] for row in bucket) / count
            avg_amount = sum(row["amount"] for row in bucket) / count
        else:
            rate = avg_ticket = avg_r = avg_f = avg_m = avg_days = avg_amount = 0.0
        lift = round(rate / global_rate, 4) if global_rate > 0 else 0.0
        segments.append(
            {
                "code": code,
                "name": SEGMENT_LABELS[code],
                "rule_text": SEGMENT_RULES[code],
                "sort_order": SEGMENT_ORDER.index(code),
                "member_count": count,
                "share": round(count / total_members, 4) if total_members else 0.0,
                "avg_r_score": round(avg_r, 2),
                "avg_f_score": round(avg_f, 2),
                "avg_m_score": round(avg_m, 2),
                "avg_r_days": round(avg_days, 1),
                "avg_amount_window": round(avg_amount, 2),
                "purchase_rate": round(rate, 6),
                "avg_ticket": round(avg_ticket, 2),
                "lift": lift,
            }
        )
    totals = {
        "members": total_members,
        "orders": sum(int(row["orders"] or 0) for row in scored),
        "amount": round(sum(float(row["amount"] or 0.0) for row in scored), 2),
    }
    top = max(segments, key=lambda item: (item["lift"], item["member_count"]))
    log = (
        f"评分 {total_members} 人（窗口 {window_days} 天，权重 {weights[0]:.2f}/{weights[1]:.2f}/{weights[2]:.2f}）；"
        f"最高提升度 {top['name']} lift {top['lift']:.2f}（{top['member_count']} 人）"
    )
    return {
        "global_purchase_rate": global_rate,
        "segments": segments,
        "totals": totals,
        "log": log,
    }


def _empty_segments() -> list[dict]:
    return [
        {
            "code": code,
            "name": SEGMENT_LABELS[code],
            "rule_text": SEGMENT_RULES[code],
            "sort_order": SEGMENT_ORDER.index(code),
            "member_count": 0,
            "share": 0.0,
            "avg_r_score": 0.0,
            "avg_f_score": 0.0,
            "avg_m_score": 0.0,
            "avg_r_days": 0.0,
            "avg_amount_window": 0.0,
            "purchase_rate": 0.0,
            "avg_ticket": 0.0,
            "lift": 0.0,
        }
        for code in SEGMENT_ORDER
    ]


def as_of_from_orders(db: Session) -> date:
    """快照基准日 = 库内最新订单日期（没有订单则用业务当天）。"""
    from sqlalchemy import func, select

    from src.models.order import Order
    from src.utils.timeutil import local_now

    latest = db.scalar(select(func.max(Order.paid_at)))
    if isinstance(latest, datetime):
        return latest.date()
    return local_now().date()
