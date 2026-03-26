import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

# 프로젝트 루트 경로
BASE_DIR = Path(__file__).resolve().parent

class Settings(BaseSettings):
    """애플리케이션 설정"""

    # === 프로젝트 정보 ===
    PROJECT_NAME: str = "SSMSP 통합 위험성평가 API"
    VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1"

    # === Google Cloud 설정 ===
    GCP_PROJECT_ID: str = "cokiri-ssmsp"
    GCP_LOCATION: str = "asia-northeast3"
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None

    # === Vertex AI 설정 ===
    EMBEDDING_MODEL: str = "text-embedding-004"
    EMBEDDING_DIMENSION: int = 768
    LLM_MODEL: str = "gemini-2.5-flash"

    # === 벡터 검색 설정 ===
    ENDPOINT_RESOURCE_NAME: str = "projects/95390507074/locations/asia-northeast3/indexEndpoints/7307495015687651328"
    DEPLOYED_INDEX_ID: str = "risk_assessment_deployed_v1"
    SIMILARITY_TOP_K: int = 3
    RAG_VECTOR_TOP_K_MULTIPLIER: float = 5.0
    RAG_SCORE_ALPHA: float = 0.7
    RAG_SCORE_THRESHOLD: float = 0.3

    # === 데이터 파일 경로 ===
    RAG_INDEX_DIR: str = str(BASE_DIR / "rag_indexes")
    DATA_FILE_STRUCTURE_2024_2025: str = str(BASE_DIR / "rag_indexes" / "embedded_risk_data_structure_improvement_2024_2025.json")
    DATA_FILE_SNOW_REMOVAL_2024_2025: str = str(BASE_DIR / "rag_indexes" / "embedded_risk_data_snow_removal_2024_2025.json")
    PDF_FORMAT_DIR: str = str(BASE_DIR / "config" / "pdf_formats")
    UPLOAD_DIR: str = str(BASE_DIR / "uploads")
    JOBS_DIR: str = str(BASE_DIR / "uploads" / "jobs")  # 작업 디렉토리
    DEBUG_DIR: str = str(BASE_DIR / "debug")  # Gemini API 디버그 로그

    # === PDF 파싱 설정 ===
    # 2024~2025 구조물 개량공사 헤더
    HEADERS_STRUCTURE_2024_2025: list = [
        "세부작업", "위험분류", "위험상황결과", "현재 안전보건조치", "재해사례",
        "현재 위험성(가능성)", "현재 위험성(중대성)", "현재 위험성",
        "NO", "감소대책", "개선 후 위험성", "개선 예정일", "완료일", "담당자"
    ]

    # 2024~2025 제설 위험성평가 헤더 (현재는 구조물 개량공사와 동일)
    HEADERS_SNOW_REMOVAL_2024_2025: list = [
        "세부작업", "기인물", "위험분류", "위험 세부분류", "위험발생상황 및 결과",
        "관련근거", "현재 안전보건조치", "현재 위험성(가능성)", "현재 위험성(중대성)", "현재 위험성",
        "위험성 감소대책", "개선 후 위험성", "개선예정일", "완료일", "담당자", "비고"
    ]

    # === API 설정 ===
    MAX_UPLOAD_SIZE: int = 200 * 1024 * 1024  # 200MB
    ALLOWED_EXTENSIONS: list = ["pdf"]

    # === AI 평가 설정 ===
    AI_BATCH_SIZE: int = 20  # 한 번에 AI에게 전송할 항목 수
    AI_MAX_WORKERS: int = 2  # 동시 처리 쓰레드 수 (1 권장, 병렬 처리 시 오류 발생)

    # === AI 평가 설정 ===
    # AI 배치 평가 크기 (한 번의 Gemini API 호출로 처리할 항목 수)
    # - 값이 클수록: API 호출 횟수↓, 비용↓, 속도↑ (단, 개별 요청 시간↑)
    # - 값이 작을수록: 안정성↑, 에러 복구 용이, 개별 요청 시간↓
    # 권장값: 10~20 (Gemini 응답 시간 고려)
    AI_BATCH_SIZE: int = 20  # 10개씩 처리 (약 30-40초/배치)

    # Gemini API 타임아웃 (초)
    # 배치 크기가 클수록 응답 시간이 길어지므로 여유있게 설정
    # None = 시간제한 없음
    AI_TIMEOUT: Optional[int] = None

    # === 로깅 설정 ===
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

    # === 개발 모드 ===
    DEBUG: bool | str = False

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )

# 싱글톤 인스턴스
settings = Settings()

# 디렉토리 생성
os.makedirs(settings.RAG_INDEX_DIR, exist_ok=True)
os.makedirs(settings.PDF_FORMAT_DIR, exist_ok=True)
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
os.makedirs(settings.JOBS_DIR, exist_ok=True)
os.makedirs(settings.DEBUG_DIR, exist_ok=True)
