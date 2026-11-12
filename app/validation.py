"""results.csv 校验。

约定 CSV 表头（首行）：sample_no,test_item,result_value
业务规则：
- 三列齐全、表头顺序固定；
- 每行三列，列数不符即坏行；
- 样品号 / 项目 / 结果均为非空文本；
- 批内 (样品号, 项目) 组合唯一。
任何不满足都会收集为明确的中文原因，整批拒收，不入库。
"""
from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass

REQUIRED_HEADER = ("sample_no", "test_item", "result_value")


@dataclass
class ValidationResult:
    ok: bool
    records: list[dict]
    reasons: list[str]
    checksum: str


def validate_csv(raw: bytes) -> ValidationResult:
    reasons: list[str] = []
    checksum = hashlib.sha256(raw).hexdigest()

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return ValidationResult(
            False, [], [f"CSV 编码不是 UTF-8：{exc}"], checksum
        )

    if not text.strip():
        return ValidationResult(False, [], ["CSV 文件为空"], checksum)

    reader = csv.reader(io.StringIO(text))
    try:
        rows = list(reader)
    except csv.Error as exc:
        return ValidationResult(False, [], [f"CSV 解析失败：{exc}"], checksum)

    if not rows:
        return ValidationResult(False, [], ["CSV 没有任何行"], checksum)

    header = tuple(h.strip() for h in rows[0])
    if header != REQUIRED_HEADER:
        reasons.append(
            f"表头必须为 {','.join(REQUIRED_HEADER)}，实际为 {','.join(header) or '(空)'}"
        )
        # 表头错误时后续行无法按约定解析，直接判坏批
        return ValidationResult(False, [], reasons, checksum)

    if len(rows) == 1:
        reasons.append("CSV 只有表头，没有检测记录")
        return ValidationResult(False, [], reasons, checksum)

    records: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for idx, row in enumerate(rows[1:], start=2):  # 行号含表头，从第 2 行起
        if len(row) != 3:
            reasons.append(f"第 {idx} 行列数为 {len(row)}，应为 3 列")
            continue
        sample_no, test_item, result_value = (cell.strip() for cell in row)
        row_errors = []
        if not sample_no:
            row_errors.append("样品号为空")
        if not test_item:
            row_errors.append("项目为空")
        if not result_value:
            row_errors.append("结果为空")
        if row_errors:
            reasons.append(f"第 {idx} 行：{'；'.join(row_errors)}")
            continue
        key = (sample_no, test_item)
        if key in seen:
            reasons.append(
                f"第 {idx} 行：样品号 {sample_no} 与项目 {test_item} 的组合在批内重复"
            )
            continue
        seen.add(key)
        records.append(
            {
                "sample_no": sample_no,
                "test_item": test_item,
                "result_value": result_value,
                "row_no": idx,
            }
        )

    if reasons:
        return ValidationResult(False, [], reasons, checksum)
    return ValidationResult(True, records, [], checksum)
