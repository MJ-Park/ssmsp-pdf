import pdfplumber
import pandas as pd
import os
import sys

DEFAULT_PAGE = 49

# 2022년 양식 헤더 정의
HEADERS_2022 = [
    "소공종", "세부작업", "기인물", "위험분류", "위험 세부분류", 
    "위험발생 상황 및 결과", "관련근거(법적기준)", "현재의 안전보건조치", 
    "가능성(빈도)", "중대성(강도)", "위험성", "위험성 감소대책",
    "개선 후 위험성", "개선예정일", "완료일", "담당자"
]

def clean_text(text):
    return str(text).replace("\n", " ").strip() if text else ""

def extract_table_test(pdf_path, page_number):
    if not os.path.exists(pdf_path):
        print(f"파일을 찾을 수 없습니다: {pdf_path}")
        return

    with pdfplumber.open(pdf_path) as pdf:
        if page_number > len(pdf.pages):
            print(f"페이지 번호가 범위를 벗어났습니다. (총 {len(pdf.pages)} 페이지)")
            return
        
        page_index = page_number - 1
        page = pdf.pages[page_index]
        
        # app.py의 테이블 추출 설정 참고
        tables = page.extract_tables(table_settings={
            "vertical_strategy": "lines", 
            "horizontal_strategy": "lines",
            "snap_tolerance": 5,
        })

        print(f"--- {page_number} 페이지 테이블 추출 결과 ({len(tables)}개) ---")

        for i, table in enumerate(tables):
            print(f"\n[Table {i+1}] Raw Data:")
            cleaned_rows = []
            for row in table:
                # 텍스트 정제만 수행 (None -> "", 줄바꿈 -> 공백)
                cleaned_row = [clean_text(cell) for cell in row]
                
                # 빈 칸 압축 (데이터 밀림 방지)
                compressed = [c for c in cleaned_row if c != ""]
                
                # 데이터가 너무 적으면 스킵
                if len(compressed) < 2: continue
                
                # 헤더 행 스킵 로직 (2022년 양식 기준)
                row_text = "".join(compressed)
                
                # 1. 제목 및 메인 헤더 키워드 필터링 (공사명, 작업공정명, 위험성평가표 등)
                if any(keyword in row_text for keyword in ["공사명", "작업공정명", "위험성평가표"]):
                    print(f"  [Skip Title] {compressed}")
                    continue

                # 2. 컬럼 헤더 키워드 필터링 (소공종, 기인물, 위험분류 등)
                if any(keyword in row_text for keyword in ["소공종", "세부작업", "유해위험요인", "기인물", "위험분류", "가능성", "중대성"]):
                    print(f"  [Skip Header] {compressed}")
                    continue

                cleaned_rows.append(cleaned_row)
            
            if cleaned_rows:
                df = pd.DataFrame(cleaned_rows)
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', 1000)
                pd.set_option('display.max_colwidth', 30)
                print(df)
            else:
                print("  (데이터 없음)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: uv run python test.py /path/to/file.pdf [page_number]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    page_number = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PAGE
    extract_table_test(pdf_path, page_number)
