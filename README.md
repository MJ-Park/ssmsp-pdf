# ssmsp-pdf

WSL/Linux 환경에서 PDF 위험성평가표를 CSV와 JSONL로 변환하는 Streamlit 앱입니다.

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
