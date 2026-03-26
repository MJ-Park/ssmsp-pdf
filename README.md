# ssmsp-pdf

WSL/Linux 환경에서 PDF 위험성평가표를 CSV와 JSONL로 변환하는 Streamlit 앱입니다.
기존 PDF 표 추출 기능을 유지하면서, AI 추천 기능과 연결하기 쉬운 행 단위 입력 데이터를 생성합니다.

## Requirements

- `uv`
- Python 3.12

## Setup

프로젝트 루트에서 아래 명령으로 의존성을 동기화합니다.

```bash
uv sync
```

Python 3.12가 시스템에 없다면 먼저 설치할 수 있습니다.

```bash
uv python install 3.12
uv sync
```

## Run

```bash
uv run streamlit run app.py
```

기본 접속 주소는 `http://localhost:8501` 입니다.

## Outputs

- `risk_rows.jsonl`
  - AI 추천/비교 파이프라인 입력용 행 단위 JSONL
- `preview.csv`
  - 사람이 검토하기 쉬운 CSV
- `extraction_report.csv`
  - 파일별 추출 결과, 검토 필요 행 수, 사용된 페이지, 경고 요약
- `validation_report.csv`
  - 행 단위 검증 결과, issue code, validation score, short reason
- `page_validation_summary.csv`
  - 페이지 단위 suspicious row 수와 validation grade 요약
- `document_validation_summary.csv`
  - 문서 단위 suspicious row/page 및 overall grade 요약
- `suspicious_rows.jsonl`
  - 강한 의심 행만 별도로 모은 JSONL

주요 필드는 `document_id`, `row_id`, `source_file`, `page`, `tags`, `search_text`, `raw_row_text`, `needs_review` 입니다.
`tags`는 LLM 없이 규칙 기반 키워드 매핑으로 생성되며, 이 출력은 이후 AI 추천 기능의 입력 데이터로 바로 사용할 수 있습니다.
추출 검증은 규칙 기반으로 수행되며, `validation_status`, `validation_score`, `issue_codes`, `short_reason` 필드로 왜 의심되는지 설명합니다.
결과 화면의 `검토하기` 버튼을 누르면 large 모달이 열리며, 선택한 행의 PDF 확대 보기와 추출 비교를 빠르게 확인할 수 있습니다.

## Review UI

- `행 단위 리뷰`
  - large 모달 상단에서 행 확대 보기만 보고, 하단에서 PDF 열 순서대로 추출된 값을 가로 표로 비교합니다.
  - 최하단의 `문제 없음` / `문제 있음` 버튼으로 빠르게 다음 의심 행으로 넘어갈 수 있습니다.
- `provenance`
  - 최종 필드가 어떤 원시 열/셀에서 왔는지 설명하는 메타데이터입니다.
  - `source_column`, `source_cell_index`, `source_text`, `mapping_rule`로 표시됩니다.
- `review_decisions.csv`
  - 리뷰 액션을 저장한 뒤 메인 화면의 검증 결과 섹션에서 다운로드할 수 있습니다.
  - 파일은 브라우저 다운로드 위치에 저장되며, `review_scope`, `row_id`, `source_file`, `page`, `reviewer_action`, `reviewer_note`, `reviewed_at` 등을 포함합니다.

## AI Suggestions

- `reference 문서`와 `test 문서`
  - 데모에서는 `input/` 폴더의 문서를 기준으로 reference와 test를 나눠 비교합니다.
  - 기본값은 `2024년 2권역 + 2025년 1권역 = reference`, `2025년 2권역 = test` 입니다.
- `AI 제안의 의미`
  - AI가 만드는 결과는 “확정 판단”이 아니라 “추가 검토 후보”입니다.
  - 이미 현재 문서에 충분히 반영된 내용은 제외하고, reference 사례가 있는 항목만 제안하도록 설계합니다.
- `근거 reference row 확인`
  - 각 제안에 표시되는 reference `row_id / source_file / page`를 통해 근거를 확인할 수 있습니다.
  - `검토 열기` 버튼을 누르면 기존 빠른 검토 모달에서 해당 row를 다시 볼 수 있습니다.
- `비교 방식`
  - 벡터DB 없이 category 비교와 대표 reference 사례 선정을 기반으로 데모가 동작합니다.
  - category 분포 차이와 representative row를 LLM에 넣어 추가 위험성 후보를 생성합니다.

## Sample Test

의존성 설치 후 원하는 PDF 파일 경로를 넘겨 간단히 확인할 수 있습니다.

```bash
uv run python test.py /path/to/file.pdf
```

`test.py`는 지정한 PDF의 특정 페이지에서 표 추출 결과를 출력합니다.

## Troubleshooting

- 의존성이 꼬였다고 느껴지면 `uv sync --reinstall`으로 환경을 다시 맞춥니다.
- Python 버전 문제가 나면 `uv python install 3.12` 후 다시 `uv sync`를 실행합니다.
- 실행 경로 확인이 필요하면 `uv run which python` 또는 `uv run which streamlit`으로 현재 환경을 확인합니다.
