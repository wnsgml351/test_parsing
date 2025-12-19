import pdfplumber
import pandas as pd
import os

target_filename = "2336499_1.pdf" # 파일명 확인
base_path = os.getcwd()
pdf_path = os.path.join(base_path, target_filename)
output_folder = os.path.join(base_path, "pdf_result")
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "all_pages_result.xlsx")

if os.path.exists(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        # 엑셀 파일 작성을 위한 준비
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            
            # 모든 페이지를 순회
            for p_idx, page in enumerate(pdf.pages):
                # 해당 페이지의 텍스트 하단 좌표 찾기 (마지막 행 인식 보정)
                words = page.extract_words()
                bottom_most = max(word['bottom'] for word in words) if words else page.bbox[3]

                table_settings = {
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 6,
                    "join_tolerance": 6,
                    "explicit_horizontal_lines": [bottom_most + 5], # 바닥선 강제 생성
                    "intersection_tolerance": 15,
                }

                # 테이블 추출
                tables = page.extract_tables(table_settings=table_settings)

                # 만약 'lines'로 안 나오면 'text' 전략으로 재시도
                if not tables:
                    table_settings["horizontal_strategy"] = "text"
                    tables = page.extract_tables(table_settings=table_settings)

                # 추출된 테이블을 엑셀 시트로 저장
                if tables:
                    for t_idx, table in enumerate(tables):
                        df = pd.DataFrame(table)
                        df = df.fillna("")
                        # 텍스트 정리 (줄바꿈 제거 및 공백 정리)
                        df = df.applymap(lambda x: str(x).replace('\n', ' ').strip())
                        
                        # 시트 이름 예: P1_T1 (1페이지 1번 테이블)
                        sheet_name = f"P{p_idx + 1}_T{t_idx + 1}"
                        # 이름이 너무 길면 엑셀에서 오류가 나므로 슬라이싱
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False, header=False)
                    
                    print(f"📄 {p_idx + 1}페이지 처리 완료")
                else:
                    print(f"⚠️ {p_idx + 1}페이지에서 테이블을 찾지 못했습니다.")

    print(f"\n✨ 모든 페이지 추출 완료! 파일 확인: {output_file}")
else:
    print(f"❌ 파일을 찾을 수 없습니다: {pdf_path}")