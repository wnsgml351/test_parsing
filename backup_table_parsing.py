import pdfplumber
import pandas as pd
from bs4 import BeautifulSoup
import json
from io import StringIO
import re

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
    # 1. HTML 파싱 및 DataFrame 로드
    soup = BeautifulSoup(html_data, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(StringIO(str(table)))[0]
    df = df.fillna("").astype(str)

    result = {
        "사건번호": "",
        "매각물건번호": "",
        "작성일자": "",
        "최선순위설정": "",
        "배당요구종기": "",
        "점유자현황": []
    }

    # 2. 상단 기본 정보 추출
    
    try:
        # 첫 번째 열(iloc[:, 0])에서 "사건" 키워드 검색
        case_rows = df[df.iloc[:, 0].str.contains("사건", na=False)]
        if not case_rows.empty:
            case_row = case_rows.iloc[0]
            result["사건번호"] = str(case_row.iloc[1]).replace('\\n', ',').strip()
        
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

        # 3. 점유자(임차인) 현황 추출
        # 첫 번째 열에서 "점유자"와 "<비고>"의 위치 탐색
        start_search = df[df.iloc[:, 0].str.contains("점유자", na=False)]
        end_search = df[df.iloc[:, 0].str.contains("<비고>", na=False)]

        if not start_search.empty:
            start_idx = start_search.index[0] + 1

            # end_idx가 없으면(비고가 없으면) 끝까지 읽도록 슬라이싱 처리
            if not end_search.empty:
                end_idx = end_search.index[0]
                occ_df = df.iloc[start_idx:end_idx].copy()
            else:
                occ_df = df.iloc[start_idx:].copy()

            current_name = ""  # 이름 병합을 위한 변수

            for _, row in occ_df.iterrows():
                
                row_list = [str(v).replace('\\n', ' ').strip() for v in row.tolist()]
    
                # 1. 헤더 및 빈 행 스킵
                row_str = "".join(row_list)
                if not row_str or any(k in row_str for k in ["점유부분", "정보출처", "성 명"]):
                    continue

                # 2. 이름 업데이트
                if row_list[0] and not any(k in row_list[0] for k in ["None", "nan"]):
                    current_name = row_list[0]

                occ_info = {
                    "점유자": current_name, "점유부분": "", "정보출처": "", "점유의권원": "",
                    "임대차기간": "", "보증금": "", "차임": "", "전입신고": "", "확정일자": "", "배당요구": ""
                }

                # 3. 데이터 성격에 따른 동적 분류
                dates_and_unknowns = [] # 날짜와 '미상'을 함께 담을 리스트
                
                for val in row_list:
                    if not val or val in ["None", "nan"]: continue
                    
                    # 임대차 기간 : 날짜 패턴이 있으면서 '-' 또는 '~'가 포함된 경우
                    if DATE_PATTERN.search(val) and (("-" in val) or ("~" in val)):
                        occ_info["임대차기간"] = val
                    # 날짜 형식 이거나 '미상'인 경우 수집
                    elif DATE_PATTERN.search(val) or "미상" in val:
                        dates_and_unknowns.append(val)
                    # 보증금 (금액)
                    elif val.replace(',', '').isdigit() and int(val.replace(',', '')) > 1000000:
                        occ_info["보증금"] = val.replace(',', '')
                    # 정보출처
                    elif any(k in val for k in ["등기", "현황", "권리"]):
                        occ_info["정보출처"] = val
                    # 점유부분
                    elif any(k in val for k in ["호", "층", "동", "전부"]):
                        occ_info["점유부분"] = val
                    # 점유의 권원
                    elif "임차인" in val:
                        occ_info["점유의권원"] = val

                # 4. 수집된 날짜/미상 데이터를 논리적 순서대로 배정
                # 보통 표의 순서상 전입신고 -> 확정일자 -> 배당요구 순으로 배치됩니다. 
                if len(dates_and_unknowns) >= 1: 
                    occ_info["전입신고"] = dates_and_unknowns[0]
                if len(dates_and_unknowns) >= 2: 
                    occ_info["확정일자"] = dates_and_unknowns[1]
                if len(dates_and_unknowns) >= 3: 
                    # 마지막 항목이 날짜 형식인 경우에만 배당요구일로 판단 (배당요구에 '미상'은 드물기 때문)
                    if DATE_PATTERN.search(dates_and_unknowns[-1]):
                        occ_info["배당요구"] = dates_and_unknowns[-1]

                if occ_info["점유자"]:
                    result["점유자현황"].append(occ_info)

    except Exception as e:
        print(f"파싱 중 오류 발생: {e}")

    return result

# 실행 및 HTML 변환
df = parse_pdf_to_table("2433827_1.pdf")
if df is not None:
    # Pandas를 이용해 깨끗한 HTML 코드로 변환
    html_output = df.to_html(index=False, border=1)
    # print(html_output)

    parsed_result = extract_key_info_from_html(html_output)
    print(json.dumps(parsed_result, indent=4, ensure_ascii=False))