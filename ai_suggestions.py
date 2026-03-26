import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types as genai_types
from pydantic import BaseModel, Field

from settings import settings

DEFAULT_REFERENCE_FILES = [
    "위험성평가(2024년 구조물 개량공사 2권역).pdf",
    "위험성평가(2025년 구조물 개량공사 1권역).pdf",
]
DEFAULT_TEST_FILE = "위험성평가(2025년 구조물 개량공사 2권역).pdf"

CATEGORY_RULES = {
    "추락": ["추락", "떨어짐", "낙상"],
    "전도": ["전도", "넘어짐"],
    "낙하": ["낙하", "낙하물", "비래"],
    "협착": ["협착", "끼임", "말림"],
    "충돌": ["충돌", "부딪힘"],
    "추돌": ["추돌", "들이받", "후진하는 작업차량"],
    "차량": ["차량", "작업차량", "교통사고", "주행"],
    "중장비": ["중장비", "건설기계", "굴삭기", "덤프", "크레인", "고소작업대", "굴절차"],
    "고소작업": ["고소작업", "고소", "비계", "작업대", "사다리", "교량 하부", "안전대"],
    "차로통제": ["차로통제", "차로 통제", "lane closure"],
    "교통통제": ["교통통제", "교통 통제", "교통차단", "차단", "통제구간"],
    "절단_베임": ["절단", "컷팅", "절삭", "베임", "그라인더", "절단기"],
    "감전": ["감전", "전기", "누전", "전선"],
    "안전난간": ["안전난간", "난간"],
    "신호수_유도원": ["신호수", "유도원", "유도자", "작업지휘자", "작업감시자"],
    "야간작업": ["야간", "야간작업", "야간 작업"],
}

TAG_TO_CATEGORIES = {
    "야간작업": ["야간작업"],
    "차로통제": ["차로통제"],
    "교통통제": ["교통통제"],
    "중장비": ["중장비"],
    "굴삭기": ["중장비"],
    "덤프": ["중장비", "차량"],
    "절삭": ["절단_베임"],
    "포장": ["차량"],
    "고소작업": ["고소작업", "추락"],
    "신호수": ["신호수_유도원"],
    "유도원": ["신호수_유도원"],
}

TOKEN_RE = re.compile(r"[^0-9A-Za-z가-힣]+")

SYSTEM_PROMPT = """
너는 도로공사 위험성평가 검토 보조 AI다.

역할:
- 현재 위험성평가 문서(test document)와 과거 유사 사례(reference rows)를 비교하여,
  현재 문서에 추가 검토가 필요할 수 있는 위험성 카테고리 및 세부 위험 유형을 제안한다.
- 너의 역할은 “확정 판단”이 아니라 “추가 검토 후보 제안”이다.

반드시 지킬 원칙:
1. 이미 현재 문서에 충분히 반영된 내용은 다시 제안하지 마라.
2. 일반론만 말하지 마라.
3. 반드시 reference 사례를 근거로 사용하라.
4. 제안은 “누락 가능성”, “보강 필요 가능성”, “추가 검토 필요”의 표현으로 작성하라.
5. 확정적 표현, 단정적 표현, 법률 자문처럼 보이는 표현은 금지한다.
6. 출력은 반드시 구조화된 JSON이어야 한다.
7. 각 제안에는 근거 reference row id를 포함해야 한다.
8. 근거가 약한 경우 confidence를 낮게 주고 human_review_required=true로 설정하라.
9. 제안 수는 최대 5개로 제한하라.
10. 답변은 한국어로 작성하라.

판단 기준:
- 현재 문서의 카테고리 분포
- reference 문서의 카테고리 분포
- test 문서에 이미 존재하는 관련 row
- reference 사례의 세부 위험상황과 현재 문서의 반영 수준
- 같은 공종/유사 작업에서 반복적으로 등장하는 위험 패턴

좋은 제안의 예시 방향:
- “reference에서는 반복적으로 나타나지만 test 문서에서는 확인되지 않는 위험 카테고리”
- “test 문서에 동일 카테고리가 있으나 세부 유형이 부족한 경우”
- “현재 문서에서 작업 맥락상 검토가 필요해 보이지만 반영이 약한 위험요인”

나쁜 제안의 예시 방향:
- reference 근거가 없는 일반적인 안전수칙 나열
- test 문서에 이미 충분히 있는 내용을 중복 제안
- 너무 포괄적이고 추상적인 문장
- 확정 진단처럼 단정하는 표현
""".strip()


class AISuggestion(BaseModel):
    category: str
    suggestion_title: str
    why_review_needed: str
    evidence_reference_rows: list[str] = Field(default_factory=list)
    related_test_rows: list[str] = Field(default_factory=list)
    suggested_risk_description: str
    suggested_controls: list[str] = Field(default_factory=list)
    confidence: float
    human_review_required: bool


class AISuggestionEnvelope(BaseModel):
    suggestions: list[AISuggestion] = Field(default_factory=list)


class AISuggestionConfigError(Exception):
    pass


class AISuggestionLLMError(Exception):
    pass


@dataclass
class ComparisonArtifacts:
    reference_files: list[str]
    test_file: str
    reference_rows: list[dict[str, Any]]
    test_rows: list[dict[str, Any]]
    reference_counts: dict[str, int]
    test_counts: dict[str, int]
    missing_categories: list[str]
    weak_categories: list[str]
    candidate_payloads: list[dict[str, Any]]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def tokenize(text: str) -> set[str]:
    return {token for token in TOKEN_RE.split(clean_text(text)) if len(token) >= 2}


def get_row_text_blob(row: dict[str, Any]) -> str:
    parts = [
        row.get("sub_process", ""),
        row.get("hazard_factor", ""),
        row.get("accident_scenario", ""),
        " ".join(row.get("current_controls", [])) if isinstance(row.get("current_controls"), list) else row.get("current_controls", ""),
        row.get("감소대책", ""),
        row.get("raw_row_text", ""),
    ]
    return " ".join([clean_text(part) for part in parts if clean_text(part)])


def assign_row_categories(row: dict[str, Any]) -> list[str]:
    categories = set()

    for tag in row.get("tags", []) if isinstance(row.get("tags"), list) else []:
        categories.update(TAG_TO_CATEGORIES.get(tag, []))

    haystack = get_row_text_blob(row)
    for category, keywords in CATEGORY_RULES.items():
        if any(keyword in haystack for keyword in keywords):
            categories.add(category)

    return sorted(categories)


def build_category_distribution(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        row_categories = row.get("ai_categories", [])
        counter.update(row_categories)
    return dict(counter)


def build_document_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    major_processes = Counter(clean_text(row.get("major_process")) for row in rows if clean_text(row.get("major_process")))
    sub_processes = Counter(clean_text(row.get("sub_process")) for row in rows if clean_text(row.get("sub_process")))
    return {
        "row_count": len(rows),
        "major_processes": [value for value, _ in major_processes.most_common(5)],
        "sub_processes": [value for value, _ in sub_processes.most_common(8)],
    }


def score_reference_row(category: str, reference_row: dict[str, Any], test_rows: list[dict[str, Any]]) -> int:
    score = 0
    reference_categories = set(reference_row.get("ai_categories", []))
    if category in reference_categories:
        score += 100

    reference_tags = set(reference_row.get("tags", [])) if isinstance(reference_row.get("tags"), list) else set()
    reference_tokens = tokenize(
        " ".join(
            [
                clean_text(reference_row.get("major_process")),
                clean_text(reference_row.get("sub_process")),
                clean_text(reference_row.get("hazard_factor")),
                clean_text(reference_row.get("accident_scenario")),
            ]
        )
    )

    for test_row in test_rows:
        test_tags = set(test_row.get("tags", [])) if isinstance(test_row.get("tags"), list) else set()
        test_tokens = tokenize(
            " ".join(
                [
                    clean_text(test_row.get("major_process")),
                    clean_text(test_row.get("sub_process")),
                    clean_text(test_row.get("hazard_factor")),
                    clean_text(test_row.get("accident_scenario")),
                ]
            )
        )
        score += len(reference_tags & test_tags) * 5
        score += len(reference_tokens & test_tokens) * 2

        if clean_text(reference_row.get("major_process")) and clean_text(reference_row.get("major_process")) == clean_text(test_row.get("major_process")):
            score += 10
        if clean_text(reference_row.get("sub_process")) and clean_text(reference_row.get("sub_process")) == clean_text(test_row.get("sub_process")):
            score += 8

    return score


def build_row_reference_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_id": row.get("row_id", ""),
        "source_file": row.get("source_file", ""),
        "page": row.get("page", ""),
        "sub_process": row.get("sub_process", ""),
        "hazard_factor": row.get("hazard_factor", ""),
        "accident_scenario": row.get("accident_scenario", ""),
        "current_controls": row.get("current_controls", []),
        "ai_categories": row.get("ai_categories", []),
    }


def build_comparison_artifacts(all_rows: list[dict[str, Any]], reference_files: list[str], test_file: str) -> ComparisonArtifacts:
    enriched_rows = []
    for row in all_rows:
        enriched = dict(row)
        enriched["ai_categories"] = assign_row_categories(row)
        enriched_rows.append(enriched)

    reference_rows = [row for row in enriched_rows if row.get("source_file") in reference_files]
    test_rows = [row for row in enriched_rows if row.get("source_file") == test_file]

    reference_counts = build_category_distribution(reference_rows)
    test_counts = build_category_distribution(test_rows)

    all_categories = sorted(set(reference_counts) | set(test_counts))
    missing_categories = [
        category
        for category in all_categories
        if reference_counts.get(category, 0) >= 2 and test_counts.get(category, 0) == 0
    ]
    weak_categories = [
        category
        for category in all_categories
        if reference_counts.get(category, 0) >= 3 and 0 < test_counts.get(category, 0) / max(reference_counts.get(category, 0), 1) < 0.5
    ]

    candidate_payloads = []
    for category in unique_ordered(missing_categories + weak_categories):
        related_test_rows = [row for row in test_rows if category in row.get("ai_categories", [])]
        scored_reference_rows = sorted(
            [
                (score_reference_row(category, row, related_test_rows or test_rows), row)
                for row in reference_rows
                if category in row.get("ai_categories", [])
            ],
            key=lambda item: item[0],
            reverse=True,
        )
        representative_rows = [build_row_reference_summary(row) for _, row in scored_reference_rows[:5]]
        related_rows = [build_row_reference_summary(row) for row in related_test_rows[:3]]

        candidate_payloads.append(
            {
                "category": category,
                "reference_count": reference_counts.get(category, 0),
                "test_count": test_counts.get(category, 0),
                "comparison_type": "missing" if category in missing_categories else "weak",
                "representative_reference_rows": representative_rows,
                "related_test_rows": related_rows,
            }
        )

    return ComparisonArtifacts(
        reference_files=reference_files,
        test_file=test_file,
        reference_rows=reference_rows,
        test_rows=test_rows,
        reference_counts=reference_counts,
        test_counts=test_counts,
        missing_categories=missing_categories,
        weak_categories=weak_categories,
        candidate_payloads=candidate_payloads,
    )


def unique_ordered(items: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def build_user_prompt(comparison: ComparisonArtifacts) -> str:
    test_summary = build_document_summary(comparison.test_rows)
    reference_summary = build_document_summary(comparison.reference_rows)
    payload = {
        "test_document": {
            "source_file": comparison.test_file,
            "summary": test_summary,
            "category_distribution": comparison.test_counts,
        },
        "reference_documents": {
            "source_files": comparison.reference_files,
            "summary": reference_summary,
            "category_distribution": comparison.reference_counts,
        },
        "missing_categories": comparison.missing_categories,
        "weak_categories": comparison.weak_categories,
        "category_candidates": comparison.candidate_payloads,
        "output_schema": AISuggestionEnvelope.model_json_schema(),
    }

    return (
        "다음은 reference 문서와 test 문서를 비교한 결과다.\n"
        "현재 문서에 추가 검토가 필요한 위험성 후보만 최대 5개 제안하라.\n"
        "이미 충분히 반영된 내용은 제외하고, 반드시 reference row를 근거로 사용하라.\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def configure_vertex_environment() -> None:
    credentials_path = clean_text(settings.GOOGLE_APPLICATION_CREDENTIALS or os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    if not credentials_path:
        raise AISuggestionConfigError("GOOGLE_APPLICATION_CREDENTIALS 경로가 설정되지 않았습니다.")

    credentials_file = Path(credentials_path)
    if not credentials_file.exists():
        raise AISuggestionConfigError(f"서비스 계정 JSON 파일을 찾을 수 없습니다: {credentials_file}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_file)
    os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"
    os.environ["GOOGLE_CLOUD_PROJECT"] = settings.GCP_PROJECT_ID
    os.environ["GOOGLE_CLOUD_LOCATION"] = settings.GCP_LOCATION


def create_vertex_client() -> genai.Client:
    configure_vertex_environment()
    return genai.Client(
        vertexai=True,
        project=settings.GCP_PROJECT_ID,
        location=settings.GCP_LOCATION,
        http_options=genai_types.HttpOptions(api_version="v1"),
    )


def parse_generate_content_response(response: Any) -> AISuggestionEnvelope:
    parsed = getattr(response, "parsed", None)
    if isinstance(parsed, AISuggestionEnvelope):
        return parsed
    if isinstance(parsed, dict):
        return AISuggestionEnvelope.model_validate(parsed)

    response_text = getattr(response, "text", "") or ""
    if not response_text and getattr(response, "candidates", None):
        parts = []
        for candidate in response.candidates:
            content = getattr(candidate, "content", None)
            for part in getattr(content, "parts", []) or []:
                part_text = getattr(part, "text", "")
                if part_text:
                    parts.append(part_text)
        response_text = "\n".join(parts)

    if not response_text:
        raise AISuggestionLLMError("LLM 응답에서 JSON 본문을 찾지 못했습니다.")

    json_match = re.search(r"\{.*\}", response_text, flags=re.DOTALL)
    if not json_match:
        raise AISuggestionLLMError("LLM 응답에서 JSON 블록을 찾지 못했습니다.")

    return AISuggestionEnvelope.model_validate(json.loads(json_match.group(0)))


def deduplicate_suggestions(suggestions: list[AISuggestion]) -> list[AISuggestion]:
    deduped: dict[tuple[str, str], AISuggestion] = {}
    for suggestion in suggestions:
        key = (clean_text(suggestion.category), clean_text(suggestion.suggestion_title))
        if key not in deduped or suggestion.confidence > deduped[key].confidence:
            deduped[key] = suggestion

    filtered = []
    for suggestion in deduped.values():
        if not suggestion.evidence_reference_rows:
            continue
        filtered.append(suggestion)

    filtered.sort(key=lambda item: item.confidence, reverse=True)
    return filtered[:5]


def generate_ai_suggestions(all_rows: list[dict[str, Any]], reference_files: list[str], test_file: str) -> dict[str, Any]:
    comparison = build_comparison_artifacts(all_rows, reference_files, test_file)

    if not comparison.test_rows:
        raise AISuggestionConfigError("선택한 test 문서에서 행 데이터를 찾지 못했습니다.")
    if not comparison.reference_rows:
        raise AISuggestionConfigError("선택한 reference 문서에서 행 데이터를 찾지 못했습니다.")
    if not comparison.candidate_payloads:
        return {
            "comparison": comparison,
            "suggestions": [],
            "raw_response_text": "",
        }

    client = create_vertex_client()
    prompt = build_user_prompt(comparison)
    response = client.models.generate_content(
        model=settings.LLM_MODEL,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=0.2,
            response_mime_type="application/json",
            response_schema=AISuggestionEnvelope,
        ),
    )
    parsed = parse_generate_content_response(response)
    suggestions = deduplicate_suggestions(parsed.suggestions)

    return {
        "comparison": comparison,
        "suggestions": [suggestion.model_dump() for suggestion in suggestions],
        "raw_response_text": getattr(response, "text", "") or "",
        "model_name": settings.LLM_MODEL,
    }


def comparison_to_summary_rows(comparison: ComparisonArtifacts) -> list[dict[str, Any]]:
    all_categories = sorted(set(comparison.reference_counts) | set(comparison.test_counts))
    return [
        {
            "category": category,
            "reference_count": comparison.reference_counts.get(category, 0),
            "test_count": comparison.test_counts.get(category, 0),
            "status": (
                "missing"
                if category in comparison.missing_categories
                else "weak"
                if category in comparison.weak_categories
                else "covered"
            ),
        }
        for category in all_categories
    ]


def suggestions_to_csv_rows(suggestions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for suggestion in suggestions:
        rows.append(
            {
                "category": suggestion.get("category", ""),
                "suggestion_title": suggestion.get("suggestion_title", ""),
                "why_review_needed": suggestion.get("why_review_needed", ""),
                "evidence_reference_rows": " | ".join(suggestion.get("evidence_reference_rows", [])),
                "related_test_rows": " | ".join(suggestion.get("related_test_rows", [])),
                "suggested_risk_description": suggestion.get("suggested_risk_description", ""),
                "suggested_controls": " | ".join(suggestion.get("suggested_controls", [])),
                "confidence": suggestion.get("confidence", 0),
                "human_review_required": suggestion.get("human_review_required", False),
            }
        )
    return rows
