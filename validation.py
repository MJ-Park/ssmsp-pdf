import copy
import re
from collections import Counter, defaultdict

ISSUE_WEIGHTS = {
    "MISSING_ROW_ID": 2,
    "MISSING_SOURCE_FILE": 2,
    "INVALID_PAGE": 2,
    "MISSING_CORE_FIELD": 1,
    "CURRENT_CONTROLS_EMPTY": 2,
    "CURRENT_CONTROLS_NUMERIC_ONLY": 3,
    "CURRENT_CONTROLS_SHORT_NUMERIC_LIST": 3,
    "CURRENT_CONTROLS_CASE_ID_ONLY": 3,
    "CURRENT_CONTROLS_TOO_SHORT": 2,
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY": 4,
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_WEAK": 4,
    "CONTROL_LENGTH_IMBALANCE": 2,
    "LOW_RAW_TEXT_COVERAGE": 2,
    "RAW_TEXT_RICH_BUT_STRUCTURED_FIELDS_WEAK": 2,
    "SHIFT_LIKE_VALUE_IN_CONTROLS": 3,
    "POSSIBLE_COLUMN_SHIFT": 4,
    "SEARCH_TEXT_TOO_SHORT": 1,
    "PAGE_CLUSTER_SHIFT_SUSPECT": 2,
    "CONTROLS_EMPTY_BUT_REDUCTION_PRESENT": 0,
    "OVERFLAGGED_NEEDS_REVIEW": 0,
}

ISSUE_REASONS = {
    "MISSING_ROW_ID": "row_id가 비어 있습니다.",
    "MISSING_SOURCE_FILE": "source_file이 비어 있습니다.",
    "INVALID_PAGE": "page 값이 유효하지 않습니다.",
    "MISSING_CORE_FIELD": "핵심 필드가 일부 비어 있습니다.",
    "CURRENT_CONTROLS_EMPTY": "current_controls가 비어 있습니다.",
    "CURRENT_CONTROLS_NUMERIC_ONLY": "current_controls가 숫자만 포함합니다.",
    "CURRENT_CONTROLS_SHORT_NUMERIC_LIST": "current_controls가 짧은 숫자 목록 수준입니다.",
    "CURRENT_CONTROLS_CASE_ID_ONLY": "current_controls가 사례번호 패턴만 포함합니다.",
    "CURRENT_CONTROLS_TOO_SHORT": "current_controls가 지나치게 짧습니다.",
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY": "원문에는 조치 표현이 있지만 current_controls가 비어 있습니다.",
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_WEAK": "원문에는 조치 표현이 있지만 current_controls가 빈약합니다.",
    "CONTROL_LENGTH_IMBALANCE": "위험 설명에 비해 current_controls 길이가 비정상적으로 짧습니다.",
    "LOW_RAW_TEXT_COVERAGE": "구조화 필드 토큰이 raw_row_text에서 충분히 확인되지 않습니다.",
    "RAW_TEXT_RICH_BUT_STRUCTURED_FIELDS_WEAK": "raw_row_text는 풍부하지만 구조화 필드가 약합니다.",
    "SHIFT_LIKE_VALUE_IN_CONTROLS": "current_controls에 사례번호/숫자/날짜/담당자형 값이 끼어든 것으로 보입니다.",
    "POSSIBLE_COLUMN_SHIFT": "열 밀림으로 인해 current_controls가 잘못 매핑되었을 가능성이 큽니다.",
    "SEARCH_TEXT_TOO_SHORT": "search_text가 지나치게 짧습니다.",
    "PAGE_CLUSTER_SHIFT_SUSPECT": "같은 페이지에서 유사한 시프트 의심 행이 반복됩니다.",
    "CONTROLS_EMPTY_BUT_REDUCTION_PRESENT": "현재 안전보건조치가 비어 있지만 감소대책에 조치 문장이 있어 양식상 정상 행일 가능성이 높습니다.",
    "OVERFLAGGED_NEEDS_REVIEW": "기존 needs_review는 true였지만 새 검증 기준에서는 정상에 가깝습니다.",
}

ACTION_KEYWORDS = [
    "설치",
    "배치",
    "착용",
    "교육",
    "점검",
    "확인",
    "준수",
    "고정",
    "정리",
    "유지",
    "통제",
    "유도",
    "보강",
    "사용",
    "조치",
]

CORE_FIELDS = ["sub_process", "hazard_factor", "accident_scenario", "raw_row_text"]
SHIFT_REFERENCE_FIELDS = ["재해사례", "NO", "담당자", "개선 예정일", "개선예정일", "완료일"]
STRONG_REVIEW_CODES = {
    "CURRENT_CONTROLS_NUMERIC_ONLY",
    "CURRENT_CONTROLS_SHORT_NUMERIC_LIST",
    "CURRENT_CONTROLS_CASE_ID_ONLY",
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY",
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_WEAK",
    "SHIFT_LIKE_VALUE_IN_CONTROLS",
    "POSSIBLE_COLUMN_SHIFT",
    "PAGE_CLUSTER_SHIFT_SUSPECT",
}
SHIFT_SIGNAL_CODES = {
    "CURRENT_CONTROLS_NUMERIC_ONLY",
    "CURRENT_CONTROLS_SHORT_NUMERIC_LIST",
    "CURRENT_CONTROLS_CASE_ID_ONLY",
    "SHIFT_LIKE_VALUE_IN_CONTROLS",
    "POSSIBLE_COLUMN_SHIFT",
}
SUPPRESSIBLE_LAYOUT_VARIANT_CODES = {
    "CURRENT_CONTROLS_EMPTY",
    "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY",
    "CONTROL_LENGTH_IMBALANCE",
}

CASE_ID_RE = re.compile(r"^(?:사례|case)\s*[-]?\s*\d+$", re.IGNORECASE)
DATE_RE = re.compile(r"^\d{4}[./-]\d{1,2}[./-]\d{1,2}$")
NUMERIC_RE = re.compile(r"^\d+$")


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def ensure_list(value):
    if isinstance(value, list):
        return [clean_text(item) for item in value if clean_text(item)]
    if not value:
        return []
    return [clean_text(value)] if clean_text(value) else []


def tokenize(text):
    return [token for token in re.split(r"[^0-9A-Za-z가-힣]+", clean_text(text)) if len(token) >= 2]


def contains_action_keyword(text):
    cleaned = clean_text(text)
    return any(keyword in cleaned for keyword in ACTION_KEYWORDS)


def is_positive_int(value):
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def looks_like_case_id(text):
    return bool(CASE_ID_RE.fullmatch(clean_text(text)))


def looks_like_date(text):
    return bool(DATE_RE.fullmatch(clean_text(text)))


def looks_like_numeric_only(text):
    normalized = clean_text(text).replace(",", " ").replace("/", " ")
    tokens = [token for token in normalized.split() if token]
    return bool(tokens) and all(NUMERIC_RE.fullmatch(token) for token in tokens)


def looks_like_short_numeric_list(items):
    return bool(items) and len(items) <= 3 and all(looks_like_numeric_only(item) for item in items)


def looks_like_name(text):
    candidate = clean_text(text)
    return bool(re.fullmatch(r"[가-힣]{2,4}", candidate)) and not contains_action_keyword(candidate)


def has_meaningful_controls(items):
    if not items:
        return False

    joined = " ".join(items)
    if contains_action_keyword(joined):
        return True

    return len(clean_text(joined)) >= 8 and not looks_like_numeric_only(joined) and not looks_like_case_id(joined)


def build_controls_text(row):
    items = ensure_list(row.get("current_controls"))
    return items, " | ".join(items)


def add_issue(row, issue_code):
    if issue_code in row["issue_codes"]:
        return
    row["issue_codes"].append(issue_code)
    row["validation_score"] += ISSUE_WEIGHTS.get(issue_code, 0)


def get_raw_context_text(row):
    parts = [
        row.get("raw_row_text", ""),
        row.get("감소대책", ""),
        row.get("위험성 감소대책", ""),
        row.get("accident_scenario", ""),
    ]
    return " ".join([clean_text(part) for part in parts if clean_text(part)])


def get_reduction_text(row):
    parts = [row.get("감소대책", ""), row.get("위험성 감소대책", "")]
    return " ".join([clean_text(part) for part in parts if clean_text(part)])


def remove_issue(row, issue_code):
    if issue_code not in row["issue_codes"]:
        return
    row["issue_codes"].remove(issue_code)
    row["validation_score"] = max(0, row["validation_score"] - ISSUE_WEIGHTS.get(issue_code, 0))


def seed_row_validation(row):
    validated = copy.deepcopy(row)
    validated["needs_review_original"] = bool(row.get("needs_review"))
    validated["issue_codes"] = []
    validated["validation_score"] = 0
    validated["validation_status"] = "normal"
    validated["short_reason"] = ""

    controls_items, controls_text = build_controls_text(validated)
    raw_text = clean_text(validated.get("raw_row_text"))
    search_text = clean_text(validated.get("search_text"))
    sub_process = clean_text(validated.get("sub_process"))
    hazard_factor = clean_text(validated.get("hazard_factor"))
    accident_scenario = clean_text(validated.get("accident_scenario"))
    raw_context_text = get_raw_context_text(validated)
    reduction_text = get_reduction_text(validated)
    core_missing_count = sum(1 for field in CORE_FIELDS if not clean_text(validated.get(field)))

    if not clean_text(validated.get("row_id")):
        add_issue(validated, "MISSING_ROW_ID")
    if not clean_text(validated.get("source_file")):
        add_issue(validated, "MISSING_SOURCE_FILE")
    if not is_positive_int(validated.get("page")):
        add_issue(validated, "INVALID_PAGE")
    if core_missing_count > 0:
        add_issue(validated, "MISSING_CORE_FIELD")
    if not search_text or len(search_text) < 20:
        add_issue(validated, "SEARCH_TEXT_TOO_SHORT")

    controls_empty = not controls_items
    controls_numeric_only = looks_like_numeric_only(controls_text)
    controls_short_numeric_list = looks_like_short_numeric_list(controls_items)
    controls_case_id_only = bool(controls_items) and all(looks_like_case_id(item) for item in controls_items)
    controls_too_short = bool(controls_items) and len(clean_text(controls_text)) < 8 and not contains_action_keyword(controls_text)

    if controls_empty:
        add_issue(validated, "CURRENT_CONTROLS_EMPTY")
    if controls_numeric_only:
        add_issue(validated, "CURRENT_CONTROLS_NUMERIC_ONLY")
    elif controls_short_numeric_list:
        add_issue(validated, "CURRENT_CONTROLS_SHORT_NUMERIC_LIST")
    if controls_case_id_only:
        add_issue(validated, "CURRENT_CONTROLS_CASE_ID_ONLY")
    if controls_too_short and not controls_numeric_only and not controls_case_id_only:
        add_issue(validated, "CURRENT_CONTROLS_TOO_SHORT")

    action_in_raw = contains_action_keyword(raw_context_text)
    weak_controls = controls_empty or controls_numeric_only or controls_case_id_only or controls_too_short

    if action_in_raw and controls_empty:
        add_issue(validated, "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY")
    elif action_in_raw and weak_controls:
        add_issue(validated, "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_WEAK")

    narrative_length = len(hazard_factor) + len(accident_scenario)
    controls_length = len(clean_text(controls_text))
    if narrative_length >= 30 and controls_length <= 8:
        add_issue(validated, "CONTROL_LENGTH_IMBALANCE")

    structured_text = " ".join([sub_process, hazard_factor, accident_scenario, controls_text])
    structured_tokens = set(tokenize(structured_text))
    raw_tokens = set(tokenize(raw_text))
    if structured_tokens and raw_tokens:
        overlap_ratio = len(structured_tokens & raw_tokens) / max(len(structured_tokens), 1)
        if overlap_ratio < 0.35:
            add_issue(validated, "LOW_RAW_TEXT_COVERAGE")

    if raw_text and len(raw_text) >= 60 and core_missing_count >= 2:
        add_issue(validated, "RAW_TEXT_RICH_BUT_STRUCTURED_FIELDS_WEAK")

    shift_reference_values = [clean_text(validated.get(field)) for field in SHIFT_REFERENCE_FIELDS if clean_text(validated.get(field))]
    shift_like_value = False
    if controls_text:
        shift_like_value = any(reference == controls_text for reference in shift_reference_values)
        shift_like_value = shift_like_value or looks_like_date(controls_text) or looks_like_name(controls_text)

    if shift_like_value:
        add_issue(validated, "SHIFT_LIKE_VALUE_IN_CONTROLS")

    if (controls_numeric_only or controls_case_id_only or shift_like_value) and (
        action_in_raw or validated.get("merged_from_cells")
    ):
        add_issue(validated, "POSSIBLE_COLUMN_SHIFT")

    has_reduction_actions = bool(reduction_text) and contains_action_keyword(reduction_text)
    has_shift_signals = bool(set(validated["issue_codes"]) & SHIFT_SIGNAL_CODES)
    controls_empty_layout_variant = controls_empty and has_reduction_actions and not has_shift_signals
    validated["controls_empty_layout_variant"] = controls_empty_layout_variant

    if controls_empty_layout_variant:
        for issue_code in SUPPRESSIBLE_LAYOUT_VARIANT_CODES:
            remove_issue(validated, issue_code)
        add_issue(validated, "CONTROLS_EMPTY_BUT_REDUCTION_PRESENT")

    return validated


def finalize_row_validation(row):
    score = row["validation_score"]
    issue_code_set = set(row["issue_codes"])
    controls_empty_layout_variant = bool(row.get("controls_empty_layout_variant"))

    if controls_empty_layout_variant and issue_code_set <= {"CONTROLS_EMPTY_BUT_REDUCTION_PRESENT"}:
        status = "normal"
    elif controls_empty_layout_variant and not (issue_code_set & STRONG_REVIEW_CODES):
        status = "warning" if score >= 2 else "normal"
    elif issue_code_set & STRONG_REVIEW_CODES or score >= 6:
        status = "strong_review_needed"
    elif score >= 2:
        status = "warning"
    else:
        status = "normal"

    if row.get("needs_review_original") and status == "normal":
        row["issue_codes"].append("OVERFLAGGED_NEEDS_REVIEW")

    row["validation_status"] = status
    row["needs_review"] = status != "normal"

    reason_codes = [code for code in row["issue_codes"] if code != "OVERFLAGGED_NEEDS_REVIEW"][:2]
    if not reason_codes and row["issue_codes"]:
        reason_codes = row["issue_codes"][:1]
    row["short_reason"] = " / ".join([ISSUE_REASONS.get(code, code) for code in reason_codes])

    return row


def longest_consecutive_run(rows):
    row_numbers = sorted(set(int(row.get("row_index", 0)) for row in rows if is_positive_int(row.get("row_index"))))
    if not row_numbers:
        return 0

    longest = 1
    current = 1
    for prev_value, next_value in zip(row_numbers, row_numbers[1:]):
        if next_value == prev_value + 1:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
    return longest


def build_page_summary_rows(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row.get("document_id"), row.get("source_file"), row.get("page"))].append(row)

    summaries = []
    cluster_pages = set()

    for (document_id, source_file, page), page_rows in grouped.items():
        issue_counter = Counter()
        suspicious_rows = [row for row in page_rows if row.get("validation_status") != "normal"]
        shift_rows = [row for row in page_rows if set(row.get("issue_codes", [])) & SHIFT_SIGNAL_CODES]
        empty_core_rows = [row for row in page_rows if "MISSING_CORE_FIELD" in row.get("issue_codes", [])]
        short_controls_rows = [
            row
            for row in page_rows
            if set(row.get("issue_codes", []))
            & {"CURRENT_CONTROLS_TOO_SHORT", "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_EMPTY", "RAW_TEXT_HAS_ACTION_BUT_CONTROLS_WEAK"}
        ]
        case_rows = [row for row in page_rows if "CURRENT_CONTROLS_CASE_ID_ONLY" in row.get("issue_codes", [])]
        numeric_rows = [
            row
            for row in page_rows
            if set(row.get("issue_codes", [])) & {"CURRENT_CONTROLS_NUMERIC_ONLY", "CURRENT_CONTROLS_SHORT_NUMERIC_LIST"}
        ]

        for row in page_rows:
            issue_counter.update([code for code in row.get("issue_codes", []) if code != "OVERFLAGGED_NEEDS_REVIEW"])

        suspicious_ratio = len(suspicious_rows) / max(len(page_rows), 1)
        consecutive_suspicious = longest_consecutive_run(suspicious_rows)
        cluster_flag = len(page_rows) >= 2 and (
            len(shift_rows) >= 2
            or len(case_rows) + len(numeric_rows) >= 2
            or consecutive_suspicious >= 2
            or suspicious_ratio >= 0.75
        )
        if cluster_flag:
            cluster_pages.add((document_id, page))

        if len(shift_rows) >= 2 or (len(page_rows) >= 2 and suspicious_ratio >= 0.6) or consecutive_suspicious >= 3:
            grade = "critical"
        elif suspicious_rows:
            grade = "warning"
        else:
            grade = "normal"

        summaries.append(
            {
                "source_file": source_file,
                "document_id": document_id,
                "page": page,
                "total_rows": len(page_rows),
                "suspicious_rows": len(suspicious_rows),
                "strong_shift_suspicions": len(shift_rows),
                "empty_core_field_rows": len(empty_core_rows),
                "short_current_controls_rows": len(short_controls_rows),
                "case_number_in_controls_rows": len(case_rows),
                "numeric_controls_rows": len(numeric_rows),
                "validation_grade": grade,
                "dominant_issue_codes": ", ".join([code for code, _ in issue_counter.most_common(3)]),
            }
        )

    summaries.sort(key=lambda item: (item["source_file"], item["page"]))
    return summaries, cluster_pages


def build_document_summary_rows(rows, page_summaries):
    grouped_rows = defaultdict(list)
    for row in rows:
        grouped_rows[(row.get("document_id"), row.get("source_file"))].append(row)

    page_summary_by_document = defaultdict(list)
    for summary in page_summaries:
        page_summary_by_document[(summary["document_id"], summary["source_file"])].append(summary)

    document_summaries = []
    for (document_id, source_file), doc_rows in grouped_rows.items():
        issue_counter = Counter()
        suspicious_rows = [row for row in doc_rows if row.get("validation_status") != "normal"]
        page_summaries_for_doc = page_summary_by_document[(document_id, source_file)]

        for row in doc_rows:
            issue_counter.update([code for code in row.get("issue_codes", []) if code != "OVERFLAGGED_NEEDS_REVIEW"])

        suspicious_pages = sum(1 for summary in page_summaries_for_doc if summary["validation_grade"] in {"warning", "critical"})
        critical_pages = sum(1 for summary in page_summaries_for_doc if summary["validation_grade"] == "critical")

        if critical_pages >= 1 or (len(suspicious_rows) / max(len(doc_rows), 1)) >= 0.6:
            grade = "critical"
        elif suspicious_pages >= 1 or suspicious_rows:
            grade = "warning"
        else:
            grade = "normal"

        document_summaries.append(
            {
                "source_file": source_file,
                "document_id": document_id,
                "total_rows": len(doc_rows),
                "suspicious_rows": len(suspicious_rows),
                "suspicious_pages": suspicious_pages,
                "critical_pages": critical_pages,
                "dominant_issue_codes": ", ".join([code for code, _ in issue_counter.most_common(5)]),
                "overall_grade": grade,
            }
        )

    document_summaries.sort(key=lambda item: item["source_file"])
    return document_summaries


def build_validation_report_rows(rows):
    report_rows = []
    for row in rows:
        report_rows.append(
            {
                "document_id": row.get("document_id", ""),
                "row_id": row.get("row_id", ""),
                "source_file": row.get("source_file", ""),
                "page": row.get("page", ""),
                "row_index": row.get("row_index", ""),
                "validation_status": row.get("validation_status", "normal"),
                "validation_score": row.get("validation_score", 0),
                "issue_codes": " | ".join(row.get("issue_codes", [])),
                "short_reason": row.get("short_reason", ""),
                "sub_process": row.get("sub_process", ""),
                "hazard_factor": row.get("hazard_factor", ""),
                "current_controls": " | ".join(ensure_list(row.get("current_controls"))),
                "raw_row_text": row.get("raw_row_text", ""),
            }
        )
    return report_rows


def build_issue_count_rows(rows):
    counter = Counter()
    for row in rows:
        counter.update(row.get("issue_codes", []))

    return [{"issue_code": code, "count": count} for code, count in counter.most_common()]


def validate_extraction_rows(rows):
    if not rows:
        return {
            "validated_rows": [],
            "validation_report_rows": [],
            "page_summary_rows": [],
            "document_summary_rows": [],
            "issue_count_rows": [],
            "suspicious_rows": [],
        }

    validated_rows = [seed_row_validation(row) for row in rows]

    initial_page_summaries, cluster_pages = build_page_summary_rows(validated_rows)

    if cluster_pages:
        for row in validated_rows:
            page_key = (row.get("document_id"), row.get("page"))
            if page_key in cluster_pages and (
                row["validation_score"] > 0
                or row.get("merged_from_cells")
                or row.get("validation_status") != "normal"
            ):
                add_issue(row, "PAGE_CLUSTER_SHIFT_SUSPECT")

    validated_rows = [finalize_row_validation(row) for row in validated_rows]

    page_summary_rows, _ = build_page_summary_rows(validated_rows)
    document_summary_rows = build_document_summary_rows(validated_rows, page_summary_rows)
    validation_report_rows = build_validation_report_rows(validated_rows)
    issue_count_rows = build_issue_count_rows(validated_rows)
    suspicious_rows = [row for row in validated_rows if row.get("validation_status") == "strong_review_needed"]

    return {
        "validated_rows": validated_rows,
        "validation_report_rows": validation_report_rows,
        "page_summary_rows": page_summary_rows,
        "document_summary_rows": document_summary_rows,
        "issue_count_rows": issue_count_rows,
        "suspicious_rows": suspicious_rows,
    }
