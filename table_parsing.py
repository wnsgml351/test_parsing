# 페이지 하단에 가로줄이 없으면 마지막 행 데이터가 유실 되는 현상 발생

import pdfplumber
import pandas as pd
from bs4 import BeautifulSoup
import json
from io import StringIO
import re
import webbrowser
import os

DATE_PATTERN = re.compile(r"(\d{4})[\.\s-]\d{1,2}[\.\s-]\d{1,2}")

def parse_pdf_to_table(pdf_path):
    all_data = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:

            # 테이블 추출 설정
            table_settings = {
                "vertical_strategy": "lines",   # 표의 세로선을 기준으로 열 구분
                "horizontal_strategy": "lines", # 표의 가로선을 기준으로 행 구분
                "snap_tolerance": 3, # 선과 선 사이의 거리 - **표의 테두리 선(Line)**이 미세하게 끊어져 있을 때 이를 하나의 선으로 이어주는 역할
                "join_tolerance": 3, # 텍스트간의 거리 - 서로 떨어져 있는 텍스트 문자들을 하나의 단어 또는 문장으로 합칠 때 기준
            }
            
            # 페이지에서 테이블 추출
            table = page.extract_table(table_settings)
            if table:
                df = pd.DataFrame(table)
                # df = df.ffill() # 셀 병합처리
                # 페이지 내에서의 미세한 빈칸만 채우고 원본 구조를 최대한 유지
                df = df.replace('', None)
                all_data.append(df)

    if not all_data:
        return None

    # 모든 페이지 데이터 통합
    final_df = pd.concat(all_data, ignore_index=True)
    # final_df.replace('', None, inplace=True)
    final_df.ffill()
    return final_df



def extract_key_info_from_html(html_data):
    soup = BeautifulSoup(html_data, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(StringIO(str(table)))[0]
    df = df.fillna("").astype(str)

    result = {
        "사건번호": "", "매각물건번호": "", "작성일자": "",
        "최선순위설정": "", "배당요구종기": "", "점유자현황": []
    }

    try:
        # 1. 상단 기본 정보 추출 (고정 인덱스 대신 안전한 접근 사용)
        case_rows = df[df.iloc[:, 0].str.contains("사건", na=False)]
        if not case_rows.empty:
            result["사건번호"] = str(case_rows.iloc[0, 1]).replace('\n', ', ').strip()
        
        # 8번째 열(iloc[:, 7])에서 "매각" 키워드 검색
        maegak_rows = df[df.iloc[:, 7].str.contains("매각", na=False)]
        if not maegak_rows.empty:
            result["매각물건번호"] = maegak_rows.iloc[0].iloc[8]
        
        # 9번째 열(iloc[:, 9])에서 "작성" 키워드 검색 후 바로 옆 칸(iloc[:, 10]) 값 가져오기
        write_date_rows = df[df.iloc[:, 9].str.contains("작성", na=False)]
        if not write_date_rows.empty:
            result["작성일자"] = write_date_rows.iloc[0].iloc[10].strip()

        # "부동산 및 감정평가액" 행에서 최선순위 및 배당종기 추출
        info_rows = df[df.iloc[:, 0].str.contains("부동산 및 감정평가액", na=False)]
        if not info_rows.empty:
            info_row = info_rows.iloc[0]
            result["최선순위설정"] = info_row.iloc[8]
            result["배당요구종기"] = info_row.iloc[14]


        # 2. 점유자 현황 추출 (버퍼 로직 도입)
        start_search = df[df.iloc[:, 0].str.contains("점유자", na=False)]
        end_search = df[df.iloc[:, 0].str.contains("<비고>|※", na=False)]

        if not start_search.empty:
            start_idx = start_search.index[0] + 1
            end_idx = end_search.index[0] if not end_search.empty else len(df)
            occ_df = df.iloc[start_idx:end_idx].copy()

            current_name = ""

            pageNextCheck = False
            for _, row in occ_df.iterrows():
                row_list = [str(v).replace('\\n', ' ').replace('\n', ' ').strip() for v in row.tolist()]
                row_str = "".join(row_list)

                # 헤더 스킵
                if not row_str or any(k in row_str for k in ["점유부분", "정보출처", "성 명"]):
                    continue
                
                # 점유자 설정
                if row_list[0] and row_list[0] not in ["None", "nan"]:
                    current_name = row_list[0]

                # 각 페이지에 맞게 데이터 설정
                COL_MAPPING = {
                    "점유자": current_name,
                    "점유부분": row_list[2],
                    "정보출처": row_list[3],
                    "점유의권원": row_list[5],
                    "임대차기간": row_list[6],
                    "보증금": row_list[8].replace(',', ''),
                    "차임": row_list[11] if len(row_list) > 11 else "",
                    "전입신고": row_list[13] if len(row_list) > 13 else "",
                    "확정일자": row_list[15] if len(row_list) > 15 else "",
                    "배당요구": row_list[16] if len(row_list) > 16 else ""
                }

                # 페이지가 바뀌면 틀어지는 부분
                if pageNextCheck is True or row_list[3] == '':
                    pageNextCheck = True
                    COL_MAPPING["점유부분"] = row_list[1]
                    COL_MAPPING["정보출처"] = row_list[2]
                    COL_MAPPING["점유의권원"] = row_list[3]
                    COL_MAPPING["임대차기간"] = row_list[4]
                    COL_MAPPING["보증금"] = row_list[5]
                    COL_MAPPING["차임"] = row_list[6]
                    COL_MAPPING["전입신고"] = row_list[7]
                    COL_MAPPING["확정일자"] = row_list[8]
                    COL_MAPPING["배당요구"] = row_list[9]


                # 현재 행 데이터 정리
                temp_data = {
                    "점유자": current_name,
                    "점유부분": COL_MAPPING["점유부분"],
                    "정보출처": COL_MAPPING["정보출처"],
                    "점유의권원": COL_MAPPING["점유의권원"],
                    "임대차기간": COL_MAPPING["임대차기간"],
                    "보증금": COL_MAPPING["보증금"],
                    "차임": COL_MAPPING["차임"],
                    "전입신고": COL_MAPPING["전입신고"],
                    "확정일자": COL_MAPPING["확정일자"],
                    "배당요구": COL_MAPPING["배당요구"],
                }

                if temp_data: # 마지막 남은 데이터 추가
                    result["점유자현황"].append(temp_data)

    except Exception as e:
        print(f"파싱 중 오류 발생: {e}")

    return result

# 실행 및 HTML 변환
df = parse_pdf_to_table("2572788_1.pdf")
if df is not None:
    # Pandas를 이용해 깨끗한 HTML 코드로 변환
    html_output = df.to_html(index=False, border=1, justify='center')
    # print(html_output)

    # HTML 파일로 저장
    file_name = "debug_view.html"
    with open(file_name, "w", encoding="utf-8") as f:
        # 브라우저에서 한글이 깨지지 않도록 가벼운 HTML 헤더를 추가해주는 것이 좋습니다.
        f.write('<meta charset="utf-8">')
        f.write('<style>table { border-collapse: collapse; width: 100%; } th, td { padding: 8px; text-align: left; }</style>')
        f.write(html_output)

    print(f"✅ HTML 파일이 '{file_name}'으로 저장되었습니다.")

    # 브라우저에서 자동으로 파일 열기 (선택 사항)
    abs_path = os.path.abspath(file_name)
    webbrowser.open("file://" + abs_path)

    parsed_result = extract_key_info_from_html(html_output)
    print(json.dumps(parsed_result, indent=4, ensure_ascii=False))