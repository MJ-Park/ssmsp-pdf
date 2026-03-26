# 웹을 통해 PDF에서 위험성평가표를 추출해 AI 입력용 JSONL과 검수용 CSV로 변환하는 프로그램
# uv run streamlit run app.py 를 통해 실행

import hashlib
import html
import io
import json
import re
from collections import defaultdict
from datetime import datetime

import pandas as pd
import pdfplumber
import streamlit as st
from validation import ISSUE_REASONS, validate_extraction_rows

# --- [Core Logic] 파싱 설정 ---
# 2024년 이후 양식 헤더
HEADERS_2024 = [
    "세부작업",
    "위험분류",
    "위험상황결과",
    "현재 안전보건조치",
    "재해사례",
    "현재 위험성(가능성)",
    "현재 위험성(중대성)",
    "현재 위험성",
    "NO",
    "감소대책",
    "개선 후 위험성",
    "개선 예정일",
    "완료일",
    "담당자",
]

# 2022~2023년 양식 헤더
HEADERS_2022 = [
    "소공종",
    "세부작업",
    "기인물",
    "위험분류",
    "위험 세부분류",
    "위험발생 상황 및 결과",
    "관련근거(법적기준)",
    "현재의 안전보건조치",
    "가능성(빈도)",
    "중대성(강도)",
    "위험성",
    "위험성 감소대책",
    "개선 후 위험성",
    "개선예정일",
    "완료일",
    "담당자",
]

# 양식 타입 정의
TYPE_2024 = "2024_STYLE"
TYPE_2022 = "2022_STYLE"

FORM_CONFIGS = {
    TYPE_2024: {
        "label": "2024년 이후 양식",
        "headers": HEADERS_2024,
        "skip_keywords": ["위험분류", "세부작업", "공사명", "작업공정명", "위험성평가결과서"],
        "page_keywords": ["공사명", "작업공정명", "위험성평가 결과서"],
        "source_positions": {
            "세부작업": 1,
            "위험분류": 2,
            "위험상황결과": 4,
            "현재 안전보건조치": 6,
            "재해사례": 7,
            "현재 위험성(가능성)": 8,
            "현재 위험성(중대성)": 9,
            "현재 위험성": 10,
            "NO": 11,
            "감소대책": 12,
            "개선 후 위험성": 13,
            "개선 예정일": 14,
            "완료일": 16,
            "담당자": 17,
        },
    },
    TYPE_2022: {
        "label": "2022~2023년 양식",
        "headers": HEADERS_2022,
        "skip_keywords": ["소공종", "기인물", "위험분류", "위험성평가표", "공사명", "작업공정명", "유해위험요인"],
        "page_keywords": ["위험성평가표", "유해위험요인 파악"],
        "source_positions": None,
    },
}

TABLE_SETTINGS = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 5,
}

REVIEW_IMAGE_RESOLUTION = 144

REVIEW_ACTION_OPTIONS = [
    "승인",
    "수정 후 승인",
    "보류",
    "오탐",
    "페이지 일괄 보정 필요",
]

EXPORT_EXCLUDED_PREFIXES = ("review_",)

TAG_RULES = {
    "야간작업": ["야간", "야간작업", "야간 작업"],
    "차로통제": ["차로통제", "차로 통제", "차단", "lane closure"],
    "교통통제": ["교통통제", "교통 통제", "교통처리", "차량통제", "차량 통제"],
    "중장비": ["중장비", "건설기계", "장비 작업", "장비운행", "장비 운행"],
    "굴삭기": ["굴삭기", "백호", "포크레인"],
    "덤프": ["덤프", "덤프트럭", "덤프 트럭"],
    "절삭": ["절삭", "컷팅", "커팅", "절단"],
    "포장": ["포장", "아스팔트", "노면포장", "재포장"],
    "고소작업": ["고소작업", "고소 작업", "작업대", "비계", "스카이", "점검차"],
    "협소공간": ["협소공간", "밀폐공간", "좁은 공간", "맨홀"],
    "보행자": ["보행자", "행인", "통행인"],
    "신호수": ["신호수"],
    "유도원": ["유도원", "유도자"],
}

CORE_OUTPUT_COLUMNS = [
    "document_id",
    "row_id",
    "source_file",
    "Source_File",
    "form_version",
    "page",
    "table_index",
    "row_index",
    "major_process",
    "sub_process",
    "hazard_factor",
    "accident_scenario",
    "current_controls",
    "tags",
    "search_text",
    "raw_row_text",
    "needs_review",
    "needs_review_original",
    "validation_status",
    "validation_score",
    "issue_codes",
    "short_reason",
    "parse_warning",
    "merged_from_cells",
    "review_page",
    "review_bbox",
    "review_highlight_available",
]


def clean_text(text):
    return re.sub(r"\s+", " ", str(text)).strip() if text else ""


def clean_multiline_text(text):
    if not text:
        return ""

    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = [clean_text(line) for line in normalized.split("\n")]
    return "\n".join([line for line in lines if line])


def unique_ordered(items):
    seen = set()
    ordered = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def safe_positive_int(value, default=0):
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def normalize_bbox(bbox):
    if not bbox or len(bbox) != 4:
        return []

    try:
        normalized = [round(float(value), 2) for value in bbox]
    except (TypeError, ValueError):
        return []

    if normalized[0] >= normalized[2] or normalized[1] >= normalized[3]:
        return []

    return normalized


def format_bbox_label(bbox):
    normalized = normalize_bbox(bbox)
    if not normalized:
        return ""
    return ", ".join(str(value) for value in normalized)


def is_export_excluded(column_name):
    return any(column_name.startswith(prefix) for prefix in EXPORT_EXCLUDED_PREFIXES)


def serialize_preview_value(value):
    if isinstance(value, list):
        if value and all(not isinstance(item, dict) for item in value):
            return " | ".join(str(item) for item in value)
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def build_export_row(row):
    return {key: value for key, value in row.items() if not is_export_excluded(key)}


def build_raw_cell_records(raw_cells, cleaned_cells, cell_bboxes):
    raw_cell_records = []
    total_cells = max(len(raw_cells), len(cleaned_cells), len(cell_bboxes))

    for idx in range(total_cells):
        raw_text = raw_cells[idx] if idx < len(raw_cells) else ""
        cleaned_text = cleaned_cells[idx] if idx < len(cleaned_cells) else clean_text(raw_text)
        bbox = normalize_bbox(cell_bboxes[idx]) if idx < len(cell_bboxes) else []
        raw_cell_records.append(
            {
                "cell_index": idx + 1,
                "raw_cell_text": raw_text,
                "cleaned_cell_text": cleaned_text,
                "cell_bbox": bbox,
                "cell_bbox_label": format_bbox_label(bbox),
                "is_empty": not bool(cleaned_text),
            }
        )

    return raw_cell_records


def can_use_positional_mapping(raw_cell_records, source_positions):
    if not source_positions:
        return False

    max_position = max(source_positions.values())
    if len(raw_cell_records) < max_position:
        return False

    anchor_positions = unique_ordered(
        [
            source_positions.get("세부작업"),
            source_positions.get("위험분류"),
            source_positions.get("위험상황결과"),
            source_positions.get("재해사례"),
        ]
    )
    populated_anchor_count = sum(
        1
        for position in anchor_positions
        if position
        and position <= len(raw_cell_records)
        and clean_text(raw_cell_records[position - 1].get("cleaned_cell_text"))
    )
    return populated_anchor_count >= 2


def build_positional_mapped_cell_records(headers, raw_cell_records, source_positions):
    mapped_cell_records = []
    used_indices = set()

    for idx, header in enumerate(headers, start=1):
        source_index = source_positions.get(header)
        source_record = None
        if source_index and source_index <= len(raw_cell_records):
            source_record = raw_cell_records[source_index - 1]
            used_indices.add(source_index)

        mapped_cell_records.append(
            {
                "column_index": f"C{idx}",
                "header_name": header,
                "source_cell_index": source_record.get("cell_index", "") if source_record else "",
                "raw_cell_text": source_record.get("raw_cell_text", "") if source_record else "",
                "cleaned_cell_text": source_record.get("cleaned_cell_text", "") if source_record else "",
                "cell_bbox": source_record.get("cell_bbox", []) if source_record else [],
                "cell_bbox_label": source_record.get("cell_bbox_label", "") if source_record else "",
                "mapping_note": (
                    f"위치 고정 매핑 (원본 cell {source_index})"
                    if source_record
                    else "비어 있음"
                ),
            }
        )

    unused_cells = [
        record
        for record in raw_cell_records
        if record.get("cell_index") not in used_indices and clean_text(record.get("cleaned_cell_text"))
    ]
    for overflow_index, source_record in enumerate(unused_cells, start=1):
        mapped_cell_records.append(
            {
                "column_index": f"EX{overflow_index}",
                "header_name": "(unmapped)",
                "source_cell_index": source_record.get("cell_index", ""),
                "raw_cell_text": source_record.get("raw_cell_text", ""),
                "cleaned_cell_text": source_record.get("cleaned_cell_text", ""),
                "cell_bbox": source_record.get("cell_bbox", []),
                "cell_bbox_label": source_record.get("cell_bbox_label", ""),
                "mapping_note": "위치 고정 매핑에서 사용되지 않은 원시 셀",
            }
        )

    return mapped_cell_records


def build_mapped_cell_records(headers, raw_cell_records, source_positions=None):
    if can_use_positional_mapping(raw_cell_records, source_positions):
        return build_positional_mapped_cell_records(headers, raw_cell_records, source_positions)

    mapped_cell_records = []
    non_empty_cells = [record for record in raw_cell_records if record.get("cleaned_cell_text")]

    for idx, header in enumerate(headers, start=1):
        source_record = non_empty_cells[idx - 1] if idx - 1 < len(non_empty_cells) else None
        mapped_cell_records.append(
            {
                "column_index": f"C{idx}",
                "header_name": header,
                "source_cell_index": source_record.get("cell_index", "") if source_record else "",
                "raw_cell_text": source_record.get("raw_cell_text", "") if source_record else "",
                "cleaned_cell_text": source_record.get("cleaned_cell_text", "") if source_record else "",
                "cell_bbox": source_record.get("cell_bbox", []) if source_record else [],
                "cell_bbox_label": source_record.get("cell_bbox_label", "") if source_record else "",
                "mapping_note": "비어 있음" if not source_record else "압축 후 매핑",
            }
        )

    overflow_cells = non_empty_cells[len(headers) :]
    for overflow_index, source_record in enumerate(overflow_cells, start=1):
        mapped_cell_records.append(
            {
                "column_index": f"EX{overflow_index}",
                "header_name": "(extra)",
                "source_cell_index": source_record.get("cell_index", ""),
                "raw_cell_text": source_record.get("raw_cell_text", ""),
                "cleaned_cell_text": source_record.get("cleaned_cell_text", ""),
                "cell_bbox": source_record.get("cell_bbox", []),
                "cell_bbox_label": source_record.get("cell_bbox_label", ""),
                "mapping_note": "헤더 수 초과 셀",
            }
        )

    return mapped_cell_records


def get_mapped_cell_by_header(mapped_cell_records, header_name):
    for record in mapped_cell_records:
        if record.get("header_name") == header_name:
            return record
    return None


def collect_source_details(mapped_cell_records, headers):
    source_columns = []
    source_cell_indices = []
    source_texts = []

    for header in headers:
        cell_record = get_mapped_cell_by_header(mapped_cell_records, header)
        if not cell_record:
            continue

        cleaned_text = clean_text(cell_record.get("cleaned_cell_text"))
        if not cleaned_text:
            continue

        source_columns.append(cell_record.get("column_index", ""))
        source_cell_indices.append(str(cell_record.get("source_cell_index", "")))
        source_texts.append(f"{header}: {cleaned_text}")

    return {
        "source_column": ", ".join([value for value in source_columns if value]),
        "source_cell_index": ", ".join([value for value in source_cell_indices if value]),
        "source_text": " | ".join(source_texts),
    }


def build_field_provenance_records(
    row_dict,
    mapped_cell_records,
    page_context,
    major_process,
    sub_process,
    hazard_factor,
    accident_scenario,
    current_controls,
    tags,
    search_text,
    raw_row_text,
):
    provenance_rows = []

    def append_row(field_name, value, source_column, source_cell_index, source_text, mapping_rule):
        provenance_rows.append(
            {
                "field_name": field_name,
                "value": value,
                "source_column": source_column,
                "source_cell_index": source_cell_index,
                "source_text": source_text,
                "mapping_rule": mapping_rule,
            }
        )

    major_source = collect_source_details(mapped_cell_records, ["소공종"])
    if not major_source["source_text"] and clean_text(page_context.get("작업공정명")):
        major_source = {
            "source_column": "page_context.작업공정명",
            "source_cell_index": "",
            "source_text": page_context.get("작업공정명", ""),
        }
    append_row(
        "major_process",
        major_process,
        major_source["source_column"],
        major_source["source_cell_index"],
        major_source["source_text"],
        "소공종이 있으면 우선 사용하고, 없으면 페이지 컨텍스트의 작업공정명을 사용",
    )

    sub_process_source = collect_source_details(mapped_cell_records, ["세부작업"])
    append_row(
        "sub_process",
        sub_process,
        sub_process_source["source_column"],
        sub_process_source["source_cell_index"],
        sub_process_source["source_text"],
        "세부작업 열을 직접 사용",
    )

    hazard_source = collect_source_details(mapped_cell_records, ["기인물", "위험분류", "위험 세부분류"])
    append_row(
        "hazard_factor",
        hazard_factor,
        hazard_source["source_column"],
        hazard_source["source_cell_index"],
        hazard_source["source_text"],
        "기인물, 위험분류, 위험 세부분류를 비어 있지 않은 값만 결합",
    )

    scenario_headers = ["위험발생 상황 및 결과", "위험상황결과"]
    scenario_source = collect_source_details(mapped_cell_records, scenario_headers)
    append_row(
        "accident_scenario",
        accident_scenario,
        scenario_source["source_column"],
        scenario_source["source_cell_index"],
        scenario_source["source_text"],
        "양식별 사고상황 열을 직접 사용",
    )

    controls_headers = ["현재의 안전보건조치", "현재 안전보건조치"]
    controls_source = collect_source_details(mapped_cell_records, controls_headers)
    append_row(
        "current_controls",
        " | ".join(current_controls),
        controls_source["source_column"],
        controls_source["source_cell_index"],
        controls_source["source_text"],
        "현재 안전보건조치 열을 분리/정규화하여 목록으로 변환",
    )

    raw_row_source = collect_source_details(mapped_cell_records, [key for key, value in row_dict.items() if clean_text(value)])
    append_row(
        "raw_row_text",
        raw_row_text,
        raw_row_source["source_column"],
        raw_row_source["source_cell_index"],
        raw_row_source["source_text"],
        "행 전체의 매핑된 헤더/값을 사람이 읽을 수 있는 문자열로 재구성",
    )

    append_row(
        "tags",
        ", ".join(tags),
        "derived",
        "",
        "sub_process / hazard_factor / accident_scenario / current_controls / raw_row_text",
        "규칙 기반 TAG_RULES 키워드 매칭",
    )
    append_row(
        "search_text",
        search_text,
        "derived",
        "",
        "major_process / sub_process / hazard_factor / accident_scenario / current_controls / tags",
        "핵심 필드를 deterministic 포맷으로 결합",
    )

    return provenance_rows


def build_issue_detail_rows(row):
    detail_rows = []
    for issue_code in row.get("issue_codes", []):
        detail_rows.append(
            {
                "issue_code": issue_code,
                "description": ISSUE_REASONS.get(issue_code, issue_code),
            }
        )
    return detail_rows


def build_page_summary_map(page_summary_rows):
    return {
        (summary.get("document_id"), summary.get("source_file"), summary.get("page")): summary
        for summary in page_summary_rows
    }


def build_page_warning_message(page_summary):
    if not page_summary:
        return ""

    warnings = []
    if page_summary.get("validation_grade") == "critical":
        warnings.append("이 페이지는 행 단위 오류보다 페이지 전체 열 밀림 가능성이 더 큽니다.")
    elif page_summary.get("validation_grade") == "warning":
        warnings.append("이 페이지에는 사람 검토가 필요한 의심 행이 포함되어 있습니다.")

    if page_summary.get("strong_shift_suspicions", 0) >= 2:
        warnings.append("같은 페이지에서 current_controls 열 밀림 의심 행이 반복됩니다.")
    if page_summary.get("numeric_controls_rows", 0) >= 1 or page_summary.get("case_number_in_controls_rows", 0) >= 1:
        warnings.append("현재 안전보건조치 칸에 숫자/사례번호 오염이 관찰됩니다.")
    if page_summary.get("short_current_controls_rows", 0) >= 2:
        warnings.append("조치 필드가 지나치게 짧은 행이 여러 개 있습니다.")

    dominant_issue_codes = clean_text(page_summary.get("dominant_issue_codes"))
    if dominant_issue_codes:
        warnings.append(f"주요 이슈: {dominant_issue_codes}")

    return " ".join(unique_ordered(warnings))


def initialize_review_mode():
    if "review_mode" not in st.session_state:
        st.session_state["review_mode"] = "행 단위 리뷰"


def initialize_review_decisions():
    if "review_decisions" not in st.session_state:
        st.session_state["review_decisions"] = {}


def build_review_target_key(review_scope, document_id, page, row_id=""):
    return f"{review_scope}:{document_id}:p{page}:{row_id}"


def get_review_decision(review_scope, document_id, page, row_id=""):
    initialize_review_decisions()
    target_key = build_review_target_key(review_scope, document_id, page, row_id=row_id)
    return st.session_state["review_decisions"].get(target_key, {})


def save_review_decision(review_scope, row, reviewer_action, reviewer_note):
    initialize_review_decisions()
    document_id = row.get("document_id", "")
    page = row.get("page", "")
    row_id = row.get("row_id", "") if review_scope == "row" else ""
    target_key = build_review_target_key(review_scope, document_id, page, row_id=row_id)

    st.session_state["review_decisions"][target_key] = {
        "review_scope": review_scope,
        "document_id": document_id,
        "row_id": row_id,
        "source_file": row.get("source_file", ""),
        "page": page,
        "row_index": row.get("row_index", "") if review_scope == "row" else "",
        "reviewer_action": reviewer_action,
        "reviewer_note": reviewer_note,
        "reviewed_at": datetime.now().isoformat(timespec="seconds"),
        "validation_status": row.get("validation_status", ""),
        "validation_score": row.get("validation_score", 0),
        "issue_codes": format_issue_codes(row.get("issue_codes", [])),
    }


def build_review_decisions_df():
    initialize_review_decisions()
    decision_rows = list(st.session_state["review_decisions"].values())
    if not decision_rows:
        return pd.DataFrame()
    return pd.DataFrame(decision_rows).sort_values(by=["source_file", "page", "row_index", "review_scope"])


def build_header_order_caption(mapped_cell_records):
    ordered_headers = [
        f"{record.get('column_index')} {record.get('header_name')}"
        for record in mapped_cell_records
        if clean_text(record.get("header_name")) and not str(record.get("header_name")).startswith("(")
    ]
    return " | ".join(ordered_headers)


def build_document_id(source_file, file_bytes):
    digest = hashlib.sha1(file_bytes).hexdigest()[:12]
    return f"doc_{digest}"


def extract_page_context(page_text):
    context = {}

    for line in page_text.splitlines():
        normalized = clean_text(line)
        if not normalized:
            continue

        if "공사명" in normalized and "공사명" not in context:
            match = re.search(r"공사명\s*[:：]\s*(.+)", normalized)
            if match:
                context["공사명"] = clean_text(match.group(1))

        if "작업공정명" in normalized and "작업공정명" not in context:
            match = re.search(r"작업공정명\s*[:：]\s*(.+)", normalized)
            if match:
                context["작업공정명"] = clean_text(match.group(1))

    return context


def get_current_controls_text(row_dict):
    return row_dict.get("현재의 안전보건조치", "") or row_dict.get("현재 안전보건조치", "")


def split_control_items(text):
    if not text:
        return []

    normalized = clean_multiline_text(text)
    normalized = re.sub(r"\s+(?=\d+\s*[.)])", "\n", normalized)
    normalized = re.sub(r"\s+(?=[•·\-])", "\n", normalized)
    normalized = normalized.replace(";", "\n")

    items = []
    for chunk in normalized.split("\n"):
        chunk = re.sub(r"^\d+\s*[.)]\s*", "", chunk)
        chunk = re.sub(r"^[•·\-]\s*", "", chunk)
        chunk = clean_text(chunk)
        if chunk:
            items.append(chunk)

    return unique_ordered(items)


def build_hazard_factor(row_dict):
    parts = [
        row_dict.get("기인물", ""),
        row_dict.get("위험분류", ""),
        row_dict.get("위험 세부분류", ""),
    ]
    return " / ".join(unique_ordered([clean_text(part) for part in parts if clean_text(part)]))


def build_accident_scenario(row_dict):
    return clean_text(row_dict.get("위험발생 상황 및 결과", "") or row_dict.get("위험상황결과", ""))


def build_major_process(row_dict, page_context):
    return clean_text(row_dict.get("소공종", "") or page_context.get("작업공정명", ""))


def build_raw_row_text(row_dict):
    parts = []
    for header, value in row_dict.items():
        cleaned = clean_text(value)
        if cleaned:
            parts.append(f"{header}: {cleaned}")
    return " | ".join(parts)


def build_search_text(major_process, sub_process, hazard_factor, accident_scenario, current_controls, tags):
    sections = [
        ("대공종", major_process),
        ("세부작업", sub_process),
        ("위험요인", hazard_factor),
        ("사고상황", accident_scenario),
        ("현재대책", " ; ".join(current_controls)),
        ("태그", ", ".join(tags)),
    ]
    return " | ".join([f"[{label}] {clean_text(value)}" for label, value in sections if clean_text(value)])


def generate_tags(sub_process, hazard_factor, accident_scenario, current_controls, raw_row_text):
    text_parts = [sub_process, hazard_factor, accident_scenario, raw_row_text]
    text_parts.extend(current_controls)
    haystack = " ".join([clean_text(part) for part in text_parts if clean_text(part)])

    tags = []
    for tag, keywords in TAG_RULES.items():
        if any(keyword in haystack for keyword in keywords):
            tags.append(tag)

    return tags


def build_review_flags(row_dict, cleaned_cells, raw_cells, sub_process, hazard_factor, accident_scenario, current_controls, raw_row_text):
    warnings = []

    non_empty_indices = [idx for idx, cell in enumerate(cleaned_cells) if cell]
    interior_blank_cells = False
    if non_empty_indices:
        first_index = non_empty_indices[0]
        last_index = non_empty_indices[-1]
        interior_blank_cells = any(not cleaned_cells[idx] for idx in range(first_index, last_index + 1))

    has_multiline_cells = any("\n" in cell for cell in raw_cells if cell)
    merged_from_cells = interior_blank_cells or has_multiline_cells

    missing_major_fields = sum(
        1
        for value in [sub_process, hazard_factor, accident_scenario, " ; ".join(current_controls)]
        if not clean_text(value)
    )

    if missing_major_fields >= 2:
        warnings.append("주요 필드 누락")
    if merged_from_cells:
        warnings.append("셀 병합 또는 줄바꿈 의심")
    if raw_row_text and missing_major_fields >= 3:
        warnings.append("원문 대비 구조화 부족")

    return {
        "needs_review": bool(warnings),
        "parse_warning": " | ".join(unique_ordered(warnings)),
        "merged_from_cells": merged_from_cells,
    }


def build_output_row(
    row_dict,
    raw_cells,
    cleaned_cells,
    raw_cell_records,
    mapped_cell_records,
    document_id,
    source_file,
    form_version,
    page_number,
    table_index,
    row_index,
    page_context,
    row_bbox=None,
):
    major_process = build_major_process(row_dict, page_context)
    sub_process = clean_text(row_dict.get("세부작업", ""))
    hazard_factor = build_hazard_factor(row_dict)
    accident_scenario = build_accident_scenario(row_dict)
    current_controls = split_control_items(get_current_controls_text(row_dict))
    raw_row_text = build_raw_row_text(row_dict)
    tags = generate_tags(sub_process, hazard_factor, accident_scenario, current_controls, raw_row_text)
    search_text = build_search_text(major_process, sub_process, hazard_factor, accident_scenario, current_controls, tags)
    field_provenance = build_field_provenance_records(
        row_dict=row_dict,
        mapped_cell_records=mapped_cell_records,
        page_context=page_context,
        major_process=major_process,
        sub_process=sub_process,
        hazard_factor=hazard_factor,
        accident_scenario=accident_scenario,
        current_controls=current_controls,
        tags=tags,
        search_text=search_text,
        raw_row_text=raw_row_text,
    )
    review_info = build_review_flags(
        row_dict=row_dict,
        cleaned_cells=cleaned_cells,
        raw_cells=raw_cells,
        sub_process=sub_process,
        hazard_factor=hazard_factor,
        accident_scenario=accident_scenario,
        current_controls=current_controls,
        raw_row_text=raw_row_text,
    )

    normalized_row_bbox = normalize_bbox(row_bbox)

    output_row = {
        "document_id": document_id,
        "row_id": f"{document_id}_p{page_number}_t{table_index}_r{row_index}",
        "source_file": source_file,
        "Source_File": source_file,
        "form_version": form_version,
        "page": page_number,
        "table_index": table_index,
        "row_index": row_index,
        "major_process": major_process,
        "sub_process": sub_process,
        "hazard_factor": hazard_factor,
        "accident_scenario": accident_scenario,
        "current_controls": current_controls,
        "tags": tags,
        "search_text": search_text,
        "raw_row_text": raw_row_text,
        "review_page": page_number,
        "review_bbox": normalized_row_bbox,
        "review_highlight_available": bool(normalized_row_bbox),
        "review_raw_cells": raw_cells,
        "review_cleaned_cells": cleaned_cells,
        "review_raw_cell_records": raw_cell_records,
        "review_mapped_cells": mapped_cell_records,
        "review_field_provenance": field_provenance,
        "review_page_context": page_context,
    }
    output_row.update(review_info)

    for header, value in row_dict.items():
        output_row[header] = value

    return output_row


def _parse_table_rows(
    table_rows,
    row_bboxes,
    row_cell_bboxes,
    headers,
    source_positions,
    skip_keywords,
    document_id,
    source_file,
    form_version,
    page_number,
    table_index,
    page_context,
):
    """공통 테이블 파싱 로직"""
    extracted_rows = []

    for raw_row_index, row in enumerate(table_rows, start=1):
        raw_cells = [clean_multiline_text(cell) for cell in row]
        cleaned_cells = [clean_text(cell) for cell in row]
        row_bbox = row_bboxes[raw_row_index - 1] if raw_row_index - 1 < len(row_bboxes) else []
        cell_bboxes = row_cell_bboxes[raw_row_index - 1] if raw_row_index - 1 < len(row_cell_bboxes) else []
        raw_cell_records = build_raw_cell_records(raw_cells, cleaned_cells, cell_bboxes)
        mapped_cell_records = build_mapped_cell_records(headers, raw_cell_records, source_positions=source_positions)

        # 빈 칸 압축 (기존 데이터 추출 방식 유지)
        compressed = [cell for cell in cleaned_cells if cell]

        if len(compressed) < 2:
            continue

        row_text_nospace = "".join(compressed).replace(" ", "")

        if any(keyword in row_text_nospace for keyword in skip_keywords):
            print(f"[Debug] Skipping header/title row: {compressed}")
            continue

        row_dict = {}
        for record in mapped_cell_records:
            header_name = record.get("header_name", "")
            if not clean_text(header_name) or str(header_name).startswith("("):
                continue
            row_dict[header_name] = record.get("cleaned_cell_text", "")

        check_val = (
            row_dict.get("세부작업", "")
            + row_dict.get("위험상황결과", "")
            + row_dict.get("위험발생 상황 및 결과", "")
        )
        if not check_val.strip():
            continue

        extracted_rows.append(
            build_output_row(
                row_dict=row_dict,
                raw_cells=raw_cells,
                cleaned_cells=cleaned_cells,
                raw_cell_records=raw_cell_records,
                mapped_cell_records=mapped_cell_records,
                document_id=document_id,
                source_file=source_file,
                form_version=form_version,
                page_number=page_number,
                table_index=table_index,
                row_index=raw_row_index,
                page_context=page_context,
                row_bbox=row_bbox,
            )
        )

    return extracted_rows


def parse_pdf_bytes(file_bytes, style_type, source_file):
    """업로드된 PDF 바이너리 데이터를 받아 AI 입력용 데이터 리스트 반환"""
    extracted_data = []
    config = FORM_CONFIGS[style_type]
    document_id = build_document_id(source_file, file_bytes)

    print(f"[Debug] Parsing PDF with style: {style_type}")

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        print(f"[Debug] Total pages: {len(pdf.pages)}")
        for page_idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            if not text:
                continue

            if not all(keyword in text for keyword in config["page_keywords"]):
                continue

            print(f"[Debug] Page {page_idx} matched keywords.")

            tables = page.find_tables(table_settings=TABLE_SETTINGS)

            print(f"[Debug] Page {page_idx} tables found: {len(tables)}")
            page_context = extract_page_context(text)

            for table_index, table in enumerate(tables, start=1):
                table_rows = table.extract()
                row_bboxes = [normalize_bbox(getattr(table_row, "bbox", [])) for table_row in getattr(table, "rows", [])]
                row_cell_bboxes = [
                    [normalize_bbox(cell_bbox) for cell_bbox in getattr(table_row, "cells", [])]
                    for table_row in getattr(table, "rows", [])
                ]
                rows = _parse_table_rows(
                    table_rows=table_rows,
                row_bboxes=row_bboxes,
                row_cell_bboxes=row_cell_bboxes,
                headers=config["headers"],
                source_positions=config.get("source_positions"),
                skip_keywords=config["skip_keywords"],
                document_id=document_id,
                    source_file=source_file,
                    form_version=config["label"],
                    page_number=page_idx,
                    table_index=table_index,
                    page_context=page_context,
                )
                if rows:
                    print(f"[Debug] Extracted {len(rows)} rows from table {table_index}.")
                    extracted_data.extend(rows)

    return extracted_data


def build_preview_df(rows):
    if not rows:
        return pd.DataFrame()

    preview_rows = []
    for row in rows:
        preview_row = {}
        for key, value in build_export_row(row).items():
            preview_row[key] = serialize_preview_value(value)
        preview_rows.append(preview_row)

    df = pd.DataFrame(preview_rows)
    ordered_columns = [column for column in CORE_OUTPUT_COLUMNS if column in df.columns]
    remaining_columns = [column for column in df.columns if column not in ordered_columns]
    return df[ordered_columns + remaining_columns]


def build_extraction_report(source_file, style_type, rows, error_message="", document_id=""):
    form_version = FORM_CONFIGS[style_type]["label"]
    document_id = rows[0]["document_id"] if rows else document_id
    needs_review_rows = sum(1 for row in rows if row.get("needs_review"))
    pages_used = unique_ordered([str(row.get("page")) for row in rows if row.get("page")])
    row_warnings = unique_ordered([row.get("parse_warning", "") for row in rows if row.get("parse_warning")])
    tags_preview = unique_ordered([tag for row in rows for tag in row.get("tags", [])])[:5]

    warnings = row_warnings
    if error_message:
        warnings = [error_message]
    elif not rows:
        warnings = ["추출된 데이터 없음"]

    return {
        "source_file": source_file,
        "document_id": document_id,
        "form_version": form_version,
        "total_rows": len(rows),
        "needs_review_rows": needs_review_rows,
        "pages_used": ", ".join(pages_used),
        "warnings": " | ".join(warnings),
        "tags_preview": ", ".join(tags_preview),
        "status": "error" if error_message else ("ok" if rows else "empty"),
    }


def build_jsonl(rows):
    return "\n".join(json.dumps(build_export_row(row), ensure_ascii=False) for row in rows)


def build_rows_by_document(rows):
    rows_by_document = {}
    for row in rows:
        rows_by_document.setdefault(row.get("document_id"), []).append(row)
    return rows_by_document


def build_document_summary_map(document_summary_rows):
    return {summary["document_id"]: summary for summary in document_summary_rows}


def build_review_rows(rows):
    review_rows = [row for row in rows if row.get("validation_status") != "normal"]
    return sorted(
        review_rows,
        key=lambda row: (
            clean_text(row.get("source_file")),
            safe_positive_int(row.get("page"), default=999999),
            safe_positive_int(row.get("row_index"), default=999999),
            clean_text(row.get("row_id")),
        ),
    )


def build_review_index(rows):
    review_index = {}
    for row in build_review_rows(rows):
        source_file = clean_text(row.get("source_file"))
        page = safe_positive_int(row.get("page"))
        if not source_file or not page:
            continue
        file_bucket = review_index.setdefault(
            source_file,
            {
                "document_id": row.get("document_id", ""),
                "source_file": source_file,
                "pages": defaultdict(list),
            },
        )
        file_bucket["pages"][page].append(row)
    return review_index


def is_reviewed_row(row):
    decision = get_review_decision("row", row.get("document_id", ""), row.get("page", ""), row_id=row.get("row_id", ""))
    return bool(clean_text(decision.get("reviewer_action", "")))


def set_selected_review_row(row):
    st.session_state["review_selected_source_file"] = row.get("source_file", "")
    st.session_state["review_selected_page"] = safe_positive_int(row.get("page"))
    st.session_state["review_selected_row_id"] = row.get("row_id", "")


def queue_selected_review_row(row):
    st.session_state["review_selection_target"] = {
        "source_file": row.get("source_file", ""),
        "page": safe_positive_int(row.get("page")),
        "row_id": row.get("row_id", ""),
    }


def apply_pending_review_selection():
    target = st.session_state.get("review_selection_target")
    if not target:
        return

    st.session_state["review_selected_source_file"] = target.get("source_file", "")
    st.session_state["review_selected_page"] = target.get("page", 0)
    st.session_state["review_selected_row_id"] = target.get("row_id", "")
    st.session_state["review_selection_target"] = None


def find_next_unresolved_review_row(rows, current_row):
    review_queue = build_review_rows(rows)
    current_row_id = clean_text(current_row.get("row_id"))
    current_index = next(
        (index for index, row in enumerate(review_queue) if clean_text(row.get("row_id")) == current_row_id),
        -1,
    )

    for row in review_queue[current_index + 1 :]:
        if not is_reviewed_row(row):
            return row

    for row in review_queue:
        if not is_reviewed_row(row):
            return row

    return None


def build_review_progress(rows):
    review_queue = build_review_rows(rows)
    total_count = len(review_queue)
    reviewed_count = sum(1 for row in review_queue if is_reviewed_row(row))
    unresolved_count = total_count - reviewed_count
    return {
        "total": total_count,
        "reviewed": reviewed_count,
        "unresolved": unresolved_count,
    }


def format_issue_codes(issue_codes):
    if isinstance(issue_codes, list):
        return " | ".join(issue_codes)
    return clean_text(issue_codes)


def format_review_row_label(row):
    status = row.get("validation_status", "normal")
    status_label = "강한 검토" if status == "strong_review_needed" else "주의"
    row_index = row.get("row_index", "")
    summary = clean_text(row.get("short_reason")) or format_issue_codes(row.get("issue_codes", []))
    summary = summary[:60] + ("..." if len(summary) > 60 else "")
    return f"행 {row_index} | {status_label} | {summary}"


def build_page_review_table(page_rows):
    table_rows = []
    for row in page_rows:
        table_rows.append(
            {
                "row_id": row.get("row_id", ""),
                "row_index": row.get("row_index", ""),
                "validation_status": row.get("validation_status", "normal"),
                "validation_score": row.get("validation_score", 0),
                "issue_codes": format_issue_codes(row.get("issue_codes", [])),
                "short_reason": row.get("short_reason", ""),
                "tags": ", ".join(row.get("tags", [])) if isinstance(row.get("tags"), list) else clean_text(row.get("tags")),
                "highlight": "yes" if row.get("review_highlight_available") else "no",
            }
        )
    return pd.DataFrame(table_rows)


def build_modal_comparison_records(selected_row):
    mapped_cell_records = selected_row.get("review_mapped_cells", [])

    def column_sort_key(record):
        column_index = clean_text(record.get("column_index"))
        if column_index.startswith("C"):
            try:
                return int(column_index[1:])
            except ValueError:
                return 999
        return 999

    ordered_records = sorted(mapped_cell_records, key=column_sort_key)
    return [
        record
        for record in ordered_records
        if clean_text(record.get("header_name")) and not str(record.get("header_name")).startswith("(")
    ]


def build_modal_comparison_html(selected_row):
    comparison_records = build_modal_comparison_records(selected_row)
    if not comparison_records:
        return ""

    wide_headers = {"위험상황결과", "현재 안전보건조치", "감소대책"}
    header_cells = []
    value_cells = []

    for record in comparison_records:
        header_name = clean_text(record.get("header_name")) or "-"
        value_text = clean_text(record.get("cleaned_cell_text"))
        min_width = 220 if header_name in wide_headers else 120

        header_cells.append(
            f'<th style="min-width:{min_width}px;padding:8px 10px;border:1px solid #dcdcdc;'
            f'background:#f5f7fb;font-size:12px;font-weight:600;text-align:left;vertical-align:top;">'
            f"{html.escape(header_name)}</th>"
        )
        value_cells.append(
            f'<td style="min-width:{min_width}px;padding:10px;border:1px solid #e6e6e6;'
            f'font-size:12px;line-height:1.5;vertical-align:top;white-space:normal;word-break:break-word;">'
            f"{html.escape(value_text) if value_text else '&nbsp;'}</td>"
        )

    return (
        '<div style="overflow-x:auto;">'
        '<table style="border-collapse:collapse;table-layout:fixed;width:max-content;min-width:100%;">'
        f"<thead><tr>{''.join(header_cells)}</tr></thead>"
        f"<tbody><tr>{''.join(value_cells)}</tr></tbody>"
        "</table></div>"
    )


def render_row_crop_only_panel(selected_row, document_payloads):
    st.markdown("**행 단위 확대 보기**")
    header_caption = build_header_order_caption(selected_row.get("review_mapped_cells", []))
    if header_caption:
        st.caption(f"열 순서: {header_caption}")

    payload = document_payloads.get(selected_row.get("document_id"), {})
    pdf_bytes = payload.get("file_bytes")

    if not pdf_bytes:
        st.info("현재 세션에는 원본 PDF 바이트가 없어 행 crop을 표시할 수 없습니다.")
        return

    if not selected_row.get("review_highlight_available"):
        st.info("이 행은 bbox가 안정적으로 계산되지 않아 확대 보기를 표시할 수 없습니다.")
        return

    try:
        crop_image = render_review_row_crop(
            pdf_bytes,
            safe_positive_int(selected_row.get("page")),
            json.dumps(selected_row.get("review_bbox", []), ensure_ascii=False),
        )
        if crop_image:
            st.image(crop_image, caption="선택한 행 확대 보기", use_container_width=True)
        else:
            st.info("행 crop을 생성하지 못했습니다.")
    except Exception as exc:
        st.warning(f"행 crop 이미지를 렌더링하지 못했습니다: {exc}")


def render_modal_issue_summary(selected_row):
    st.markdown("**의심 사유**")
    if selected_row.get("short_reason"):
        st.caption(selected_row.get("short_reason"))

    issue_rows = build_issue_detail_rows(selected_row)
    if issue_rows:
        for issue_row in issue_rows[:3]:
            st.write(f"- `{issue_row['issue_code']}`: {issue_row['description']}")


def save_and_advance_review(all_results, selected_row, reviewer_action, reviewer_note):
    save_review_decision("row", selected_row, reviewer_action, reviewer_note)
    st.session_state["review_pending_issue_row_id"] = ""
    next_row = find_next_unresolved_review_row(all_results, selected_row)

    if next_row:
        st.session_state["review_queue_completed"] = False
        queue_selected_review_row(next_row)
    else:
        st.session_state["review_queue_completed"] = True
        st.session_state["review_modal_open"] = False


@st.cache_data(show_spinner=False)
def render_review_page_image(pdf_bytes, page_number, all_bboxes_json="[]", selected_bbox_json="[]"):
    all_bboxes = json.loads(all_bboxes_json)
    selected_bbox = json.loads(selected_bbox_json)

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[page_number - 1]
        page_image = page.to_image(resolution=REVIEW_IMAGE_RESOLUTION)

        for bbox in all_bboxes:
            normalized_bbox = normalize_bbox(bbox)
            if normalized_bbox:
                page_image.draw_rect(
                    normalized_bbox,
                    fill=(255, 183, 77, 48),
                    stroke=(255, 152, 0),
                    stroke_width=2,
                )

        normalized_selected_bbox = normalize_bbox(selected_bbox)
        if normalized_selected_bbox:
            page_image.draw_rect(
                normalized_selected_bbox,
                fill=(229, 57, 53, 52),
                stroke=(198, 40, 40),
                stroke_width=4,
            )

        image_buffer = io.BytesIO()
        page_image.save(image_buffer, format="PNG", quantize=False)
        return image_buffer.getvalue()


def expand_bbox_for_crop(bbox, page_width, page_height, x_pad=20, y_pad=12):
    normalized_bbox = normalize_bbox(bbox)
    if not normalized_bbox:
        return []

    return [
        max(0, normalized_bbox[0] - x_pad),
        max(0, normalized_bbox[1] - y_pad),
        min(page_width, normalized_bbox[2] + x_pad),
        min(page_height, normalized_bbox[3] + y_pad),
    ]


@st.cache_data(show_spinner=False)
def render_review_row_crop(pdf_bytes, page_number, row_bbox_json="[]"):
    row_bbox = normalize_bbox(json.loads(row_bbox_json))
    if not row_bbox:
        return b""

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[page_number - 1]
        crop_bbox = expand_bbox_for_crop(row_bbox, page.width, page.height)
        cropped_page = page.crop(tuple(crop_bbox), strict=False)
        page_image = cropped_page.to_image(resolution=max(REVIEW_IMAGE_RESOLUTION, 220))
        page_image.draw_rect(
            row_bbox,
            fill=(229, 57, 53, 38),
            stroke=(198, 40, 40),
            stroke_width=3,
        )

        image_buffer = io.BytesIO()
        page_image.save(image_buffer, format="PNG", quantize=False)
        return image_buffer.getvalue()


def build_raw_cell_review_df(selected_row):
    mapped_cell_records = selected_row.get("review_mapped_cells", [])
    if not mapped_cell_records:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "PDF 열": record.get("column_index", ""),
                "헤더": record.get("header_name", ""),
                "원시 셀 위치": record.get("source_cell_index", ""),
                "원시 추출값": record.get("raw_cell_text", ""),
                "bbox": record.get("cell_bbox_label", ""),
                "매핑 메모": record.get("mapping_note", ""),
            }
            for record in mapped_cell_records
        ]
    )


def build_original_cell_review_df(selected_row):
    raw_cell_records = selected_row.get("review_raw_cell_records", [])
    if not raw_cell_records:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "cell_index": record.get("cell_index", ""),
                "raw_cell_text": record.get("raw_cell_text", ""),
                "cleaned_cell_text": record.get("cleaned_cell_text", ""),
                "bbox": record.get("cell_bbox_label", ""),
                "is_empty": record.get("is_empty", False),
            }
            for record in raw_cell_records
        ]
    )


def build_field_provenance_df(selected_row):
    provenance_rows = selected_row.get("review_field_provenance", [])
    if not provenance_rows:
        return pd.DataFrame()
    return pd.DataFrame(provenance_rows)


def render_review_header(selected_row, page_summary):
    st.markdown("**검토 대상 요약**")
    st.caption(
        " | ".join(
            [
                f"source_file: {selected_row.get('source_file', '-')}",
                f"document_id: {selected_row.get('document_id', '-')}",
                f"page: {selected_row.get('page', '-')}",
                f"row_id: {selected_row.get('row_id', '-')}",
                f"row_index: {selected_row.get('row_index', '-')}",
            ]
        )
    )
    st.caption(
        " | ".join(
            [
                f"validation_status: {selected_row.get('validation_status', 'normal')}",
                f"validation_score: {selected_row.get('validation_score', 0)}",
                f"issue_codes: {format_issue_codes(selected_row.get('issue_codes', [])) or '-'}",
                f"tags: {', '.join(selected_row.get('tags', [])) if isinstance(selected_row.get('tags'), list) else clean_text(selected_row.get('tags')) or '-'}",
            ]
        )
    )
    if page_summary:
        st.caption(
            f"페이지 등급: {page_summary.get('validation_grade', 'normal')} | "
            f"페이지 suspicious_rows: {page_summary.get('suspicious_rows', 0)} | "
            f"strong_shift_suspicions: {page_summary.get('strong_shift_suspicions', 0)}"
        )


def render_pdf_evidence_panel(selected_row, page_rows, document_payloads):
    st.markdown("**원문 PDF 근거**")
    st.caption(
        f"{selected_row.get('source_file', '')} | page {selected_row.get('page', '')} | "
        f"row_id {selected_row.get('row_id', '')}"
    )

    payload = document_payloads.get(selected_row.get("document_id"), {})
    pdf_bytes = payload.get("file_bytes")
    all_bboxes = [row.get("review_bbox", []) for row in page_rows if row.get("review_highlight_available")]
    selected_bbox = selected_row.get("review_bbox", []) if selected_row.get("review_highlight_available") else []

    if not pdf_bytes:
        st.info("현재 세션에는 원본 PDF 바이트가 없어 원문 이미지를 표시할 수 없습니다. 다시 변환하면 검토할 수 있습니다.")
        return

    try:
        page_image = render_review_page_image(
            pdf_bytes,
            safe_positive_int(selected_row.get("page")),
            json.dumps(all_bboxes, ensure_ascii=False),
            json.dumps(selected_bbox, ensure_ascii=False),
        )
        st.image(page_image, caption="전체 페이지 + 행 하이라이트", use_container_width=True)
        st.caption("주황색은 같은 페이지의 의심 행, 빨간색은 현재 선택한 행입니다.")
    except Exception as exc:
        st.warning(f"전체 페이지 이미지를 렌더링하지 못했습니다: {exc}")

    st.markdown("**행 단위 확대 보기**")
    header_caption = build_header_order_caption(selected_row.get("review_mapped_cells", []))
    if header_caption:
        st.caption(f"열 순서: {header_caption}")

    if selected_row.get("review_highlight_available"):
        try:
            crop_image = render_review_row_crop(
                pdf_bytes,
                safe_positive_int(selected_row.get("page")),
                json.dumps(selected_row.get("review_bbox", []), ensure_ascii=False),
            )
            if crop_image:
                st.image(crop_image, caption="선택한 행 crop", use_container_width=True)
            else:
                st.info("행 crop을 생성하지 못했습니다.")
        except Exception as exc:
            st.warning(f"행 crop 이미지를 렌더링하지 못했습니다: {exc}")
    else:
        st.info("이 행은 bbox가 안정적으로 계산되지 않아 crop 없이 전체 페이지만 확인할 수 있습니다.")


def render_raw_cell_panel(selected_row):
    st.markdown("**원시 셀 재구성**")
    st.caption("PDF에서 읽어낸 셀을 헤더 순서로 다시 맞춘 테이블입니다. 셀 추출 자체가 맞는지 먼저 확인할 수 있습니다.")

    mapped_df = build_raw_cell_review_df(selected_row)
    if mapped_df.empty:
        st.info("표시할 원시 셀 재구성 데이터가 없습니다.")
    else:
        st.dataframe(mapped_df, use_container_width=True, hide_index=True)

    with st.expander("원본 셀 배열 보기", expanded=False):
        raw_df = build_original_cell_review_df(selected_row)
        if raw_df.empty:
            st.caption("원본 셀 배열 정보가 없습니다.")
        else:
            st.dataframe(raw_df, use_container_width=True, hide_index=True)


def render_structured_result_panel(selected_row):
    st.markdown("**최종 구조화 결과 및 출처**")
    st.caption("최종 필드가 어느 열/셀에서 왔는지 provenance를 같이 보여줍니다.")

    provenance_df = build_field_provenance_df(selected_row)
    if provenance_df.empty:
        st.info("필드 provenance 정보가 없습니다.")
    else:
        st.dataframe(provenance_df, use_container_width=True, hide_index=True)

    st.markdown("**의심 사유 설명**")
    issue_rows = build_issue_detail_rows(selected_row)
    if not issue_rows:
        st.caption("표시할 issue가 없습니다.")
    else:
        for issue_row in issue_rows:
            st.write(f"- `{issue_row['issue_code']}`: {issue_row['description']}")
    if selected_row.get("short_reason"):
        st.caption(f"short_reason: {selected_row.get('short_reason')}")

    existing_decision = get_review_decision(
        "row",
        selected_row.get("document_id", ""),
        selected_row.get("page", ""),
        row_id=selected_row.get("row_id", ""),
    )
    default_action = existing_decision.get("reviewer_action", "보류")
    default_note = existing_decision.get("reviewer_note", "")

    form_key = f"row_review_form_{selected_row.get('row_id', '')}"
    action_key = f"row_review_action_{selected_row.get('row_id', '')}"
    note_key = f"row_review_note_{selected_row.get('row_id', '')}"
    if action_key not in st.session_state:
        st.session_state[action_key] = default_action
    if note_key not in st.session_state:
        st.session_state[note_key] = default_note

    with st.form(form_key):
        st.markdown("**리뷰 액션**")
        reviewer_action = st.selectbox("행 리뷰 상태", REVIEW_ACTION_OPTIONS, key=action_key)
        reviewer_note = st.text_area("행 리뷰 메모", key=note_key, height=100)
        submitted = st.form_submit_button("행 리뷰 저장", use_container_width=True)

    if submitted:
        save_review_decision("row", selected_row, reviewer_action, reviewer_note)
        st.success("행 리뷰 결정을 저장했습니다.")


def render_page_review_mode(selected_source_file, selected_page, page_rows, selected_row, page_summary, document_payloads):
    st.markdown("**페이지 단위 검토**")
    if page_summary:
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric("total_rows", page_summary.get("total_rows", 0))
        metric_col2.metric("suspicious_rows", page_summary.get("suspicious_rows", 0))
        metric_col3.metric("strong_shift_suspicions", page_summary.get("strong_shift_suspicions", 0))
        metric_col4.metric("validation_grade", page_summary.get("validation_grade", "normal"))
        page_warning_message = build_page_warning_message(page_summary)
        if page_warning_message:
            st.warning(page_warning_message)
        dominant_issue_codes = [
            clean_text(code)
            for code in clean_text(page_summary.get("dominant_issue_codes", "")).split(",")
            if clean_text(code)
        ]
        if dominant_issue_codes:
            st.caption("페이지 주요 issue 설명")
            for issue_code in dominant_issue_codes:
                st.write(f"- `{issue_code}`: {ISSUE_REASONS.get(issue_code, issue_code)}")

    left_col, right_col = st.columns([0.95, 1.05], gap="large")

    with left_col:
        st.markdown("**페이지 suspicious row 목록**")
        st.dataframe(build_page_review_table(page_rows), use_container_width=True, hide_index=True)

        existing_decision = get_review_decision(
            "page",
            selected_row.get("document_id", ""),
            selected_page,
        )
        page_action_key = f"page_review_action_{selected_row.get('document_id', '')}_{selected_page}"
        page_note_key = f"page_review_note_{selected_row.get('document_id', '')}_{selected_page}"
        if page_action_key not in st.session_state:
            st.session_state[page_action_key] = existing_decision.get("reviewer_action", "페이지 일괄 보정 필요")
        if page_note_key not in st.session_state:
            st.session_state[page_note_key] = existing_decision.get("reviewer_note", "")

        with st.form(f"page_review_form_{selected_row.get('document_id', '')}_{selected_page}"):
            st.markdown("**페이지 리뷰 액션**")
            reviewer_action = st.selectbox("페이지 리뷰 상태", REVIEW_ACTION_OPTIONS, key=page_action_key)
            reviewer_note = st.text_area("페이지 리뷰 메모", key=page_note_key, height=100)
            submitted = st.form_submit_button("페이지 리뷰 저장", use_container_width=True)

        if submitted:
            save_review_decision("page", selected_row, reviewer_action, reviewer_note)
            st.success("페이지 리뷰 결정을 저장했습니다.")

    with right_col:
        st.markdown("**의심 행 crop 썸네일**")
        payload = document_payloads.get(selected_row.get("document_id"), {})
        pdf_bytes = payload.get("file_bytes")
        thumb_cols = st.columns(2)

        for index, row in enumerate(page_rows):
            target_col = thumb_cols[index % 2]
            with target_col:
                with st.container(border=True):
                    st.caption(
                        f"행 {row.get('row_index', '')} | {row.get('validation_status', 'normal')} | score {row.get('validation_score', 0)}"
                    )
                    if pdf_bytes and row.get("review_highlight_available"):
                        try:
                            crop_image = render_review_row_crop(
                                pdf_bytes,
                                safe_positive_int(row.get("page")),
                                json.dumps(row.get("review_bbox", []), ensure_ascii=False),
                            )
                            if crop_image:
                                st.image(crop_image, use_container_width=True)
                        except Exception:
                            st.caption("crop 렌더링 실패")
                    else:
                        st.caption("crop unavailable")

                    st.write(row.get("short_reason", "") or "-")
                    if st.button("이 행 상세 보기", key=f"page_to_row_{row.get('row_id', '')}", use_container_width=True):
                        st.session_state["review_selected_row_id"] = row.get("row_id")
                        st.session_state["review_mode"] = "행 단위 리뷰"
                        st.rerun()


def initialize_review_selection(review_index):
    source_files = list(review_index.keys())
    if not source_files:
        return None, None, None

    selected_source_file = st.session_state.get("review_selected_source_file")
    if selected_source_file not in source_files:
        selected_source_file = source_files[0]
        st.session_state["review_selected_source_file"] = selected_source_file

    pages = sorted(review_index[selected_source_file]["pages"].keys())
    selected_page = st.session_state.get("review_selected_page")
    if selected_page not in pages:
        selected_page = pages[0]
        st.session_state["review_selected_page"] = selected_page

    page_rows = review_index[selected_source_file]["pages"][selected_page]
    row_ids = [row.get("row_id") for row in page_rows]
    selected_row_id = st.session_state.get("review_selected_row_id")
    if selected_row_id not in row_ids:
        selected_row_id = row_ids[0]
        st.session_state["review_selected_row_id"] = selected_row_id

    return selected_source_file, selected_page, selected_row_id


@st.dialog("빠른 검토", width="large", dismissible=False)
def render_quick_review_dialog(result_state):
    apply_pending_review_selection()

    all_results = result_state.get("all_results", [])
    document_payloads = result_state.get("document_payloads", {})
    review_index = build_review_index(all_results)
    page_summary_map = build_page_summary_map(result_state.get("validation", {}).get("page_summary_rows", []))

    if not review_index:
        st.info("검토할 의심 행이 없습니다.")
        if st.button("검토 닫기", use_container_width=True):
            st.session_state["review_modal_open"] = False
            st.rerun()
        return

    selected_source_file, selected_page, selected_row_id = initialize_review_selection(review_index)
    if not selected_source_file or not selected_page or not selected_row_id:
        st.info("검토 가능한 행이 없습니다.")
        if st.button("검토 닫기", use_container_width=True):
            st.session_state["review_modal_open"] = False
            st.rerun()
        return

    top_col1, top_col2, top_col3, top_col4 = st.columns([1.1, 0.8, 1.7, 0.5], gap="small")
    source_files = list(review_index.keys())
    selected_source_file = top_col1.selectbox(
        "원본 PDF",
        options=source_files,
        index=source_files.index(selected_source_file),
        key="review_selected_source_file",
    )

    pages = sorted(review_index[selected_source_file]["pages"].keys())
    if st.session_state.get("review_selected_page") not in pages:
        st.session_state["review_selected_page"] = pages[0]
    selected_page = top_col2.selectbox(
        "의심 페이지",
        options=pages,
        format_func=lambda value: f"{value} 페이지",
        index=pages.index(st.session_state["review_selected_page"]),
        key="review_selected_page",
    )

    page_rows = review_index[selected_source_file]["pages"][selected_page]
    row_map = {row.get("row_id"): row for row in page_rows}
    row_ids = list(row_map.keys())
    if st.session_state.get("review_selected_row_id") not in row_ids:
        st.session_state["review_selected_row_id"] = row_ids[0]
    selected_row_id = top_col3.selectbox(
        "의심 행",
        options=row_ids,
        index=row_ids.index(st.session_state["review_selected_row_id"]),
        format_func=lambda row_id: format_review_row_label(row_map[row_id]),
        key="review_selected_row_id",
    )

    top_col4.write("")
    top_col4.write("")
    if top_col4.button("닫기", use_container_width=True):
        st.session_state["review_modal_open"] = False
        st.session_state["review_pending_issue_row_id"] = ""
        st.rerun()

    selected_row = row_map[selected_row_id]
    page_rows = review_index[selected_source_file]["pages"][selected_page]
    page_summary = page_summary_map.get((selected_row.get("document_id"), selected_source_file, selected_page), {})
    progress = build_review_progress(all_results)

    if progress["unresolved"] > 0:
        st.session_state["review_queue_completed"] = False
        st.caption(
            f"남은 의심 행 {progress['unresolved']}건 / 전체 {progress['total']}건 | "
            f"현재 페이지 의심 행 {len(page_rows)}건"
        )
    elif st.session_state.get("review_queue_completed"):
        st.success("모든 의심 행 검토를 완료했습니다.")

    render_review_header(selected_row, page_summary)

    st.divider()

    with st.container(border=True, height=360):
        render_row_crop_only_panel(selected_row, document_payloads)

    st.write("")

    with st.container(border=True, height=360):
        st.markdown("**추출 비교**")
        comparison_html = build_modal_comparison_html(selected_row)
        if comparison_html:
            st.markdown(comparison_html, unsafe_allow_html=True)
        else:
            st.info("표시할 추출 비교 데이터가 없습니다.")

        st.write("")
        render_modal_issue_summary(selected_row)

    pending_issue_row_id = st.session_state.get("review_pending_issue_row_id", "")
    note_key = f"review_issue_note_{selected_row.get('row_id', '')}"
    existing_decision = get_review_decision(
        "row",
        selected_row.get("document_id", ""),
        selected_row.get("page", ""),
        row_id=selected_row.get("row_id", ""),
    )

    if pending_issue_row_id == selected_row.get("row_id"):
        if note_key not in st.session_state:
            st.session_state[note_key] = existing_decision.get("reviewer_note", "")

        st.write("")
        with st.container(border=True):
            st.markdown("**문제 메모**")
            st.text_area(
                "문제 메모",
                key=note_key,
                height=90,
                placeholder="문제가 있다고 판단한 이유를 짧게 적어주세요.",
                label_visibility="collapsed",
            )
            confirm_col, cancel_col = st.columns(2)
            if confirm_col.button("문제 있음 저장 후 다음", use_container_width=True, type="primary"):
                reviewer_note = clean_text(st.session_state.get(note_key, ""))
                if not reviewer_note:
                    st.warning("문제 메모를 입력해 주세요.")
                else:
                    save_and_advance_review(all_results, selected_row, "문제 있음", reviewer_note)
                    st.rerun()
            if cancel_col.button("취소", use_container_width=True):
                st.session_state["review_pending_issue_row_id"] = ""
                st.rerun()

    st.write("")
    action_col1, action_col2 = st.columns(2, gap="large")
    if action_col1.button("문제 없음", use_container_width=True, type="primary"):
        st.session_state[note_key] = ""
        save_and_advance_review(all_results, selected_row, "문제 없음", "")
        st.rerun()

    if action_col2.button("문제 있음", use_container_width=True):
        if note_key not in st.session_state:
            st.session_state[note_key] = existing_decision.get("reviewer_note", "")
        st.session_state["review_pending_issue_row_id"] = selected_row.get("row_id", "")
        st.rerun()


def render_conversion_results(result_state):
    all_results = result_state.get("all_results", [])
    file_reports = result_state.get("file_reports", [])
    validation_state = result_state.get("validation", {})

    if not (all_results or file_reports):
        return

    st.subheader("📊 파일별 추출 현황")
    report_df = pd.DataFrame(file_reports)
    st.dataframe(report_df, use_container_width=True, hide_index=True)

    report_csv_data = report_df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="📥 extraction_report.csv 다운로드",
        data=report_csv_data,
        file_name="extraction_report.csv",
        mime="text/csv",
    )

    st.divider()

    if all_results:
        preview_df = build_preview_df(all_results)
        needs_review_count = sum(1 for row in all_results if row.get("needs_review"))

        metric_col1, metric_col2, metric_col3 = st.columns(3)
        metric_col1.metric("전체 행 수", len(all_results))
        metric_col2.metric("검토 필요 행", needs_review_count)
        metric_col3.metric("문서 수", len({row["document_id"] for row in all_results}))

        st.subheader("📋 AI 입력용 행 미리보기")
        st.caption("page / row_id / source_file / tags / needs_review 기준으로 원문 PDF를 역추적할 수 있습니다.")
        st.dataframe(preview_df, use_container_width=True)

        preview_csv_data = preview_df.to_csv(index=False, encoding="utf-8-sig")
        jsonl_data = build_jsonl(all_results)

        col1, col2 = st.columns(2)
        col1.download_button(
            label="📥 preview.csv 다운로드",
            data=preview_csv_data,
            file_name="preview.csv",
            mime="text/csv",
        )
        col2.download_button(
            label="📥 risk_rows.jsonl 다운로드",
            data=jsonl_data,
            file_name="risk_rows.jsonl",
            mime="application/json",
        )

        validation_report_rows = validation_state.get("validation_report_rows", [])
        page_summary_rows = validation_state.get("page_summary_rows", [])
        document_summary_rows = validation_state.get("document_summary_rows", [])
        issue_count_rows = validation_state.get("issue_count_rows", [])
        suspicious_rows = validation_state.get("suspicious_rows", [])
        review_rows = build_review_rows(all_results)

        if validation_report_rows:
            validation_report_df = pd.DataFrame(validation_report_rows)
            page_summary_df = pd.DataFrame(page_summary_rows)
            document_summary_df = pd.DataFrame(document_summary_rows)
            issue_count_df = pd.DataFrame(issue_count_rows)

            suspicious_count = sum(1 for row in all_results if row.get("validation_status") != "normal")
            strong_review_count = sum(1 for row in all_results if row.get("validation_status") == "strong_review_needed")
            warning_or_critical_pages = sum(
                1 for summary in page_summary_rows if summary.get("validation_grade") in {"warning", "critical"}
            )
            critical_pages = sum(1 for summary in page_summary_rows if summary.get("validation_grade") == "critical")

            st.divider()
            st.subheader("🛡️ 추출 검증 결과")

            metric_col4, metric_col5, metric_col6, metric_col7 = st.columns(4)
            metric_col4.metric("Suspicious 행", suspicious_count)
            metric_col5.metric("강한 검토 필요 행", strong_review_count)
            metric_col6.metric("Warning/Critical 페이지", warning_or_critical_pages)
            metric_col7.metric("Critical 페이지", critical_pages)

            if st.session_state.get("review_queue_completed"):
                st.success("검토를 완료했습니다.")

            if review_rows:
                review_button_col, review_info_col = st.columns([0.3, 0.7])
                if review_button_col.button("🔎 검토하기", use_container_width=True):
                    initialize_review_selection(build_review_index(all_results))
                    st.session_state["review_modal_open"] = True
                    st.session_state["review_pending_issue_row_id"] = ""
                    st.session_state["review_queue_completed"] = False
                review_info_col.caption(
                    "large 모달에서 행 확대 보기와 추출 비교를 빠르게 확인할 수 있습니다."
                )

            download_col1, download_col2, download_col3, download_col4 = st.columns(4)
            download_col1.download_button(
                label="📥 validation_report.csv 다운로드",
                data=validation_report_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name="validation_report.csv",
                mime="text/csv",
            )
            download_col2.download_button(
                label="📥 page_validation_summary.csv 다운로드",
                data=page_summary_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name="page_validation_summary.csv",
                mime="text/csv",
            )
            download_col3.download_button(
                label="📥 document_validation_summary.csv 다운로드",
                data=document_summary_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name="document_validation_summary.csv",
                mime="text/csv",
            )
            if suspicious_rows:
                download_col4.download_button(
                    label="📥 suspicious_rows.jsonl 다운로드",
                    data=build_jsonl(suspicious_rows),
                    file_name="suspicious_rows.jsonl",
                    mime="application/json",
                )

            if not issue_count_df.empty:
                st.markdown("**Issue Code별 건수**")
                st.dataframe(issue_count_df, use_container_width=True, hide_index=True)

            if not page_summary_df.empty:
                st.markdown("**페이지별 Warning / Critical 현황**")
                flagged_page_df = page_summary_df[page_summary_df["validation_grade"] != "normal"]
                if flagged_page_df.empty:
                    st.caption("warning 또는 critical로 분류된 페이지가 없습니다.")
                else:
                    st.dataframe(flagged_page_df, use_container_width=True, hide_index=True)

            suspicious_preview_df = validation_report_df[validation_report_df["validation_status"] != "normal"]
            if not suspicious_preview_df.empty:
                suspicious_preview_df = suspicious_preview_df.sort_values(
                    by=["validation_score", "source_file", "page", "row_index"],
                    ascending=[False, True, True, True],
                ).head(20)
                st.markdown("**의심 행 미리보기 상위 20개**")
                st.dataframe(suspicious_preview_df, use_container_width=True, hide_index=True)

            review_decisions_df = build_review_decisions_df()
            if not review_decisions_df.empty:
                st.markdown("**리뷰 액션 저장 결과**")
                st.dataframe(review_decisions_df, use_container_width=True, hide_index=True)
                st.download_button(
                    label="📥 review_decisions.csv 다운로드",
                    data=review_decisions_df.to_csv(index=False, encoding="utf-8-sig"),
                    file_name="review_decisions.csv",
                    mime="text/csv",
                )
    else:
        st.warning("추출된 데이터가 없습니다. extraction_report.csv의 warnings를 확인해 주세요.")


# --- [UI] Streamlit 웹 인터페이스 ---
st.set_page_config(page_title="SSMSP 위험성평가 변환기", layout="wide")

if "conversion_result" not in st.session_state:
    st.session_state["conversion_result"] = None
if "review_modal_open" not in st.session_state:
    st.session_state["review_modal_open"] = False
if "review_pending_issue_row_id" not in st.session_state:
    st.session_state["review_pending_issue_row_id"] = ""
if "review_queue_completed" not in st.session_state:
    st.session_state["review_queue_completed"] = False
if "review_selection_target" not in st.session_state:
    st.session_state["review_selection_target"] = None
initialize_review_mode()
initialize_review_decisions()

st.title("🏗️ 스마트 안전관리 위험성평가 변환기")
st.markdown(
    """
이 도구는 **PDF 위험성평가 결과서**를
**AI 추천 파이프라인 입력용 행 단위 JSONL**과 **검수용 CSV**로 변환해줍니다.
"""
)

style_option = st.radio(
    "📄 변환할 문서 양식을 선택하세요:",
    ("2024년 이후 양식 (최신)", "2022~2023년 양식 (구버전)"),
    index=0,
)

selected_type = TYPE_2024 if "2024" in style_option else TYPE_2022

uploaded_files = st.file_uploader(
    "PDF 파일을 여기에 드래그하거나 선택하세요 (다중 선택 가능)",
    type=["pdf"],
    accept_multiple_files=True,
)

if uploaded_files:
    st.write(f"📂 **{len(uploaded_files)}개의 파일이 선택되었습니다.**")

    if st.button("🚀 변환 시작", type="primary"):
        all_results = []
        file_entries = []
        document_payloads = {}

        progress_bar = st.progress(0)
        status_text = st.empty()

        for index, pdf_file in enumerate(uploaded_files):
            status_text.text(f"처리 중... {pdf_file.name}")

            try:
                file_bytes = pdf_file.read()
                document_id = build_document_id(pdf_file.name, file_bytes)
                data = parse_pdf_bytes(file_bytes, selected_type, pdf_file.name)
                document_payloads[document_id] = {"source_file": pdf_file.name, "file_bytes": file_bytes}

                if data:
                    all_results.extend(data)

                file_entries.append(
                    {
                        "source_file": pdf_file.name,
                        "document_id": document_id,
                        "error_message": "",
                    }
                )
            except Exception as exc:
                file_entries.append(
                    {
                        "source_file": pdf_file.name,
                        "document_id": "",
                        "error_message": str(exc),
                    }
                )

            progress_bar.progress((index + 1) / len(uploaded_files))

        status_text.text("✅ 변환 완료!")
        validation_state = validate_extraction_rows(all_results)
        validated_rows = validation_state["validated_rows"]
        rows_by_document = build_rows_by_document(validated_rows)
        document_summary_map = build_document_summary_map(validation_state["document_summary_rows"])

        file_reports = []
        for file_entry in file_entries:
            document_rows = rows_by_document.get(file_entry["document_id"], [])
            report = build_extraction_report(
                file_entry["source_file"],
                selected_type,
                document_rows,
                error_message=file_entry["error_message"],
                document_id=file_entry["document_id"],
            )
            document_summary = document_summary_map.get(file_entry["document_id"])
            report["suspicious_rows"] = document_summary["suspicious_rows"] if document_summary else 0
            report["critical_pages"] = document_summary["critical_pages"] if document_summary else 0
            report["overall_grade"] = document_summary["overall_grade"] if document_summary else (
                "error" if file_entry["error_message"] else "normal"
            )
            file_reports.append(report)

        st.session_state["conversion_result"] = {
            "all_results": validated_rows,
            "file_reports": file_reports,
            "validation": validation_state,
            "document_payloads": document_payloads,
        }
        st.session_state["review_decisions"] = {}
        st.session_state["review_pending_issue_row_id"] = ""
        st.session_state["review_queue_completed"] = False
        st.session_state["review_selection_target"] = None

        review_rows = build_review_rows(validated_rows)
        if review_rows:
            set_selected_review_row(review_rows[0])
            st.session_state["review_modal_open"] = True
        else:
            st.session_state["review_modal_open"] = False

result_state = st.session_state.get("conversion_result")
if result_state:
    render_conversion_results(result_state)
    if st.session_state.get("review_modal_open"):
        render_quick_review_dialog(result_state)
