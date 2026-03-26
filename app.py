# 웹을 통해 pdf에서 위험성평가표만 분류하여 데이터를 csv, jsonl로 변환하는 프로그램
# streamlit run app.py 를 통해 실행

import streamlit as st
import pdfplumber
import pandas as pd
import io
import json

# --- [Core Logic] 파싱 설정 ---
# 2024년 이후 양식 헤더
HEADERS_2024 = [
    "세부작업", "위험분류", "위험상황결과", "현재 안전보건조치", "재해사례",
    "현재 위험성(가능성)", "현재 위험성(중대성)", "현재 위험성",
    "NO", "감소대책", "개선 후 위험성", "개선 예정일", "완료일", "담당자"
]

# 2022~2023년 양식 헤더
HEADERS_2022 = [
    "소공종", "세부작업", "기인물", "위험분류", "위험 세부분류", 
    "위험발생 상황 및 결과", "관련근거(법적기준)", "현재의 안전보건조치", 
    "가능성(빈도)", "중대성(강도)", "위험성", "위험성 감소대책",
    "개선 후 위험성", "개선예정일", "완료일", "담당자"
]

# 양식 타입 정의
TYPE_2024 = "2024_STYLE"
TYPE_2022 = "2022_STYLE"

def clean_text(text):
    return str(text).replace("\n", " ").strip() if text else ""

def _parse_table_rows(table, headers, skip_keywords):
    """공통 테이블 파싱 로직"""
    extracted_rows = []
    
    for row in table:
        cleaned = [clean_text(cell) for cell in row]
        # 빈 칸 압축 (데이터 밀림 방지)
        compressed = [c for c in cleaned if c != ""]

        if len(compressed) < 2: continue

        # 행 텍스트에서 공백 제거 (키워드 매칭 정확도 향상)
        row_text_nospace = "".join(compressed).replace(" ", "")
        
        # 헤더나 제목 행이면 스킵
        if any(k in row_text_nospace for k in skip_keywords):
            print(f"[Debug] Skipping header/title row: {compressed}")
            continue

        # 데이터 매핑
        row_dict = {}
        for idx, header in enumerate(headers):
            if idx < len(compressed):
                row_dict[header] = compressed[idx]
            else:
                row_dict[header] = ""
        
        # 유효 데이터 저장 (내용이 있는 경우만)
        # 2022년 양식에는 '위험상황결과' 대신 '위험발생 상황 및 결과'가 있음
        check_val = row_dict.get("세부작업", "") + row_dict.get("위험상황결과", "") + row_dict.get("위험발생 상황 및 결과", "")
        if check_val.strip():
            extracted_rows.append(row_dict)
            
    return extracted_rows

def parse_pdf_bytes(file_bytes, style_type):
    """업로드된 PDF 바이너리 데이터를 받아 데이터 리스트 반환"""
    extracted_data = []
    print(f"[Debug] Parsing PDF with style: {style_type}")
    
    # 스타일별 설정
    if style_type == TYPE_2024:
        target_headers = HEADERS_2024
        # 스킵할 키워드 (헤더, 제목 등)
        skip_keywords = ["위험분류", "세부작업", "공사명", "작업공정명", "위험성평가결과서"]
        # 페이지 필터링 키워드
        page_keywords = ["공사명", "작업공정명", "위험성평가 결과서"]
    else:
        target_headers = HEADERS_2022
        # 스킵할 키워드 (헤더, 제목 등)
        skip_keywords = ["소공종", "기인물", "위험분류", "위험성평가표", "공사명", "작업공정명", "유해위험요인"]
        # 페이지 필터링 키워드
        page_keywords = ["위험성평가표", "유해위험요인 파악"]
    
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        print(f"[Debug] Total pages: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages):
            # 1. 페이지 필터링
            text = page.extract_text()
            if not text:
                continue
            
            # 선택된 양식의 키워드가 모두 포함되어야 유효한 페이지
            if not all(k in text for k in page_keywords):
                continue
            
            print(f"[Debug] Page {i+1} matched keywords.")

            # 2. 테이블 추출 (Lines 전략)
            tables = page.extract_tables(table_settings={
                "vertical_strategy": "lines", 
                "horizontal_strategy": "lines",
                "snap_tolerance": 5,
            })
            
            print(f"[Debug] Page {i+1} tables found: {len(tables)}")

            # 3. 파싱
            for table in tables:
                rows = _parse_table_rows(table, target_headers, skip_keywords)
                if rows:
                    print(f"[Debug] Extracted {len(rows)} rows from table.")
                    extracted_data.extend(rows)
    
    return extracted_data

# --- [UI] Streamlit 웹 인터페이스 ---
st.set_page_config(page_title="SSMSP 위험성평가 변환기", layout="wide")

st.title("🏗️ 스마트 안전관리 위험성평가 변환기")
st.markdown("""
이 도구는 **PDF 위험성평가 결과서**를 AI 학습용 데이터(**JSONL**)와 엑셀 데이터(**CSV**)로 변환해줍니다.
""")

# 1. 양식 선택 (라디오 버튼)
style_option = st.radio(
    "📄 변환할 문서 양식을 선택하세요:",
    ("2024년 이후 양식 (최신)", "2022~2023년 양식 (구버전)"),
    index=0
)

selected_type = TYPE_2024 if "2024" in style_option else TYPE_2022

# 2. 파일 업로드 (Drag & Drop)
uploaded_files = st.file_uploader("PDF 파일을 여기에 드래그하거나 선택하세요 (다중 선택 가능)", 
                                  type=["pdf"], accept_multiple_files=True)

if uploaded_files:
    st.write(f"📂 **{len(uploaded_files)}개의 파일이 선택되었습니다.**")
    
    if st.button("🚀 변환 시작", type="primary"):
        all_results = []
        file_stats = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, pdf_file in enumerate(uploaded_files):
            status_text.text(f"처리 중... {pdf_file.name}")
            
            try:
                # 파싱 실행 (선택된 양식 타입 전달)
                file_bytes = pdf_file.read()
                data = parse_pdf_bytes(file_bytes, selected_type)
                
                # 1. 각 데이터에 파일명 추가
                for entry in data:
                    entry["Source_File"] = pdf_file.name
                
                # 2. 전체 결과 리스트에 합치기
                if data:
                    all_results.extend(data)
                
                # 3. 파일별 추출 개수 기록
                file_stats.append({
                    "파일명": pdf_file.name,
                    "추출된 행 개수": len(data),
                    "상태": "✅ 성공" if len(data) > 0 else "⚠️ 데이터 없음"
                })

            except Exception as e:
                # 에러 발생 시 기록
                file_stats.append({
                    "파일명": pdf_file.name,
                    "추출된 행 개수": 0,
                    "상태": f"❌ 오류: {str(e)}"
                })
            
            # 진행률 업데이트
            progress_bar.progress((i + 1) / len(uploaded_files))

        status_text.text("✅ 변환 완료!")
        
        # 결과 화면 표시
        if all_results or file_stats:
            
            # [New] 4. 파일별 통계 표 출력 (화면 상단 배치)
            st.subheader("📊 파일별 추출 현황")
            stats_df = pd.DataFrame(file_stats)
            st.dataframe(stats_df, use_container_width=True, hide_index=True)

            st.divider() # 구분선

            if all_results:
                st.success(f"총 {len(all_results)}개의 데이터 행을 추출했습니다.")
                
                st.subheader("📋 전체 데이터 미리보기")
                # 5. 전체 데이터 DataFrame 생성
                df = pd.DataFrame(all_results)
                
                # 컬럼 순서 정렬 (Source_File 맨 앞으로)
                if not df.empty and "Source_File" in df.columns:
                    cols = ["Source_File"] + [c for c in df.columns if c != "Source_File"]
                    df = df[cols]
                
                st.dataframe(df, use_container_width=True)

                # 6. 다운로드 버튼 생성
                col1, col2 = st.columns(2)

                # (1) CSV 다운로드
                csv_data = df.to_csv(index=False, encoding="utf-8-sig")
                col1.download_button(
                    label="📥 CSV (엑셀용) 다운로드",
                    data=csv_data,
                    file_name="위험성평가_통합결과.csv",
                    mime="text/csv"
                )

                # (2) JSONL 다운로드
                jsonl_data = ""
                for row in all_results:
                    jsonl_data += json.dumps(row, ensure_ascii=False) + "\n"
                
                col2.download_button(
                    label="📥 JSONL (AI학습용) 다운로드",
                    data=jsonl_data,
                    file_name="위험성평가_학습데이터.jsonl",
                    mime="application/json"
                )
            else:
                st.warning("추출된 데이터가 없습니다. 위의 현황표를 확인하여 파일 형식을 점검해주세요.")