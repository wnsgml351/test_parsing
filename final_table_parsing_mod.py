import pdfplumber
import json
import os
import re
import pandas as pd

target_filename = "1010-1915009_1.pdf"  # 파일명 확인
base_path = os.getcwd()

# 대상 폴더명 설정
target_folder = "test_pdf"

# 폴더 경로를 포함하여 전체 PDF 경로 생성
pdf_path = os.path.join(base_path, target_folder, target_filename)

print(f"===== {target_filename} 파일을 파싱합니다. =====")

# 최종 결과를 담을 구조
result = {
    "case_no": "", # 사건번호 (예: 2025타경100211)
    "item_no": "", # 매각물건번호 (예: 1)
    "priority_date": "", # 최선순위권 설정일 및 권리 종류 (예: 2023.10.16. 압류)
    "dividend_end_date": "", # 배당요구종기일
    "document_date": "", # 작성일자
    "occupants": [], # 점유자별 상세정보
    "tenant_note": "", # 임차인 관련 비고 전체 문구
    "surviving_rights": "", # 말소되지 않는 권리 목록
    "surface_right_summary": "", # 지상권 관련 문구 전체
    "general_note": "", # 매각물건 명세서 마지막 문서 전체 비고 내용
    "auction_rounds": [], # 회차별 기일정보
}

def clean_val(val):
    return str(val).replace('\n', ' ').strip() if val else ""

def get_value_next_to_header(df, keyword):
    target_keyword = keyword.replace(" ", "")
    for i in range(len(df)):
        row = df.iloc[i].tolist()
        for col_idx, cell_val in enumerate(row):
            if cell_val:
                # 셀 데이터에서도 모든 공백과 줄바꿈을 제거 후 비교
                clean_cell = str(cell_val).replace(" ", "").replace("\n", "")
                if target_keyword in clean_cell:
                    # 키워드를 찾았으면 그 다음 칸부터 실제 데이터가 있는 곳 탐색
                    for next_idx in range(col_idx + 1, len(row)):
                        next_val = clean_val(row[next_idx])
                        if next_val:
                            return next_val
    return ""

if os.path.exists(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:

        output_dir = os.path.join(base_path, "pdf_result")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📂 폴더 생성 완료: {output_dir}")

        # 병합된 이름 처리를 위한 변수
        current_name = ""

        # 이전 배열 요소를 참조하기 위한 변수
        last_occupant = None

        # 비고 수집 체크 로직
        is_collecting_bigo = False

        # 비고란 수집 체크 로직
        is_general_note = False

        for p_idx, page in enumerate(pdf.pages):

            # 회차정보 추출
            full_text = page.extract_text()
            if full_text:
                # 정규식 패턴:
                # 1. (\d+회): 회차
                # 2. (\d{4}\.\d{2}\.\d{2}): 날짜
                # 3. ([\d,]+): 최저매각가격
                # 4. (?:\s+([\d,]+))?: 보증금 (있을 수도 있고 없을 수도 있음)
                # 5. (?=\s|$): 뒤에 공백이나 줄바꿈이 오는지 확인 (다음 회차 번호를 보증금으로 먹지 않도록 제한)
                round_pattern = re.compile(r"(\d+회)\s+(\d{4}\.\d{2}\.\d{2})\s+([\d,]+)(?:\s+([\d,]{7,}))?")
                # {7,} 의미: 보증금은 보통 액수가 크므로 7자리(백만 단위) 이상일 때만 보증금으로 인정
                # 이렇게 하면 '2회' 같은 짧은 텍스트를 보증금으로 오인하는 것을 방지합니다.

                matches = round_pattern.finditer(full_text)
                for match in matches:
                    round_str = match.group(1)
                    date_str = match.group(2)
                    min_price = match.group(3).replace(",", "")
                    
                    # 보증금이 추출되었고, 최저가보다 작은 경우에만 보증금으로 인정 (논리적 체크)
                    raw_deposit = match.group(4)
                    deposit = ""
                    if raw_deposit:
                        clean_dep = raw_deposit.replace(",", "")
                        # 보증금이 최저가보다 작을 때만(일반적으로 10%) 데이터로 수용
                        if int(clean_dep) < int(min_price):
                            deposit = clean_dep

                    round_data = {
                        "round_no": round_str, # 회차번호
                        "auction_date": date_str, # 매각기일
                        "min_bid_price": min_price, # 최저매각가격
                        "bid_deposit": deposit # 매수신청보증금
                    }

                    if not any(r['round_no'] == round_str and r['auction_date'] == date_str for r in result["auction_rounds"]):
                        result["auction_rounds"].append(round_data)


            # 하단 좌표 찾기 (마지막 행 인식 보정)
            words = page.extract_words()
            bottom_most = max(word['bottom'] for word in words) if words else page.bbox[3]

            table_settings = {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 6,
                "join_tolerance": 6,
                "explicit_horizontal_lines": [bottom_most + 5],
                "intersection_tolerance": 15,
            }

            # 테이블 추출
            tables = page.extract_tables(table_settings=table_settings)

            # 'lines'로 안 나올 경우 'text' 전략 시도
            if not tables:
                table_settings["horizontal_strategy"] = "text"
                tables = page.extract_tables(table_settings=table_settings)

            # 테이블 페이징 체크 변경
            tablePageCheck = False

            for table in tables:

                tablePageCheck = True

                # 1. 테이블을 데이터프레임으로 변환
                df = pd.DataFrame(table)

                # 데이터가 없는 경우 스킵
                if df.empty:
                    continue

                # --- [상단 정보 추출: Pandas 방식] ---
                if len(df.columns) >= 10:
                    
                    # 1. 사건번호
                    if not result["case_no"]:
                        result["case_no"] = get_value_next_to_header(df, "사건")

                    # 2. 매각물건번호
                    if not result["item_no"]:
                        result["item_no"] = get_value_next_to_header(df, "물건번호")

                    # 3. 작성일자
                    if not result["document_date"]:
                        result["document_date"] = get_value_next_to_header(df, "작성 일자")

                    # 4. 최선순위 설정
                    if not result["priority_date"]:
                        result["priority_date"] = get_value_next_to_header(df, "최선순위")

                    # 5. 배당요구종기
                    if not result["dividend_end_date"]:
                        result["dividend_end_date"] = get_value_next_to_header(df, "배당요구종기")

                    # 점유자 추출 범위 설정
                    start_search = df[df.iloc[:, 0].str.contains("점유자", na=False)]
                    end_search = df[df.iloc[:, 0].str.contains("<비고>|※|등기된 부동산|매각에 따라", na=False)]
                    
                    if not start_search.empty:
                        start_idx = start_search.index[0] + 1
                        is_occupant_extending = True
                    elif is_occupant_extending:
                        start_idx = 0
                    else:
                        start_idx = -1

                    if not end_search.empty:
                        end_idx = end_search.index[0]
                        is_occupant_extending = False
                    else:
                        end_idx = len(df)

                    # 점유자 데이터 파싱
                    if start_idx != -1:
                        occ_df = df.iloc[start_idx:end_idx].copy()

                        for _, row in occ_df.iterrows():
                            # 데이터 정제: None 제거 및 줄바꿈 처리
                            row_list = [str(v).replace('\\n', ' ').replace('\n', ' ').strip() if v is not None else "" for v in row.tolist()]
                            if not "".join(row_list) or any(k in "".join(row_list) for k in ["성명", "점유부분", "정보출처"]):
                                continue

                            # print("row_list", row_list, ", 길이 : ", len(row_list))
                            # 점유자 이름 설정
                            if row_list[0]:
                                current_name = row_list[0]

                            # 루프 시작 시점에 mapping 초기화
                            mapping = {k: "" for k in ["점유부분", "정보출처", "점유의권원", "임대차기간", "보증금", "차임", "전입신고", "확정일자", "배당요구"]}

                            # case1: 1페이지에 <비고>가 없고 점유자 리스트가 1페이지에 다 있는 경우 (1010-1915009_1)
                            if len(row_list) == 14:
                                # print("case1: 1페이지에 <비고>가 없고 점유자 리스트가 1페이지에 다 있는 경우 (1010-1915009_1)")

                                mapping = {
                                    "점유부분": row_list[2],
                                    "정보출처": row_list[3],
                                    "점유의권원": row_list[5],
                                    "임대차기간": row_list[6],
                                    "보증금": row_list[7].replace(',', ''),
                                    "차임": row_list[10] if len(row_list) > 10 else "",
                                    "전입신고": row_list[11] if len(row_list) > 11 else "",
                                    "확정일자": row_list[12] if len(row_list) > 12 else "",
                                    "배당요구": row_list[13] if len(row_list) > 13 else "",
                                }
                            elif len(row_list) == 15:
                                # print("case3: 칸이 15줄인경우 1010-2303787_1.pdf")

                                mapping = {
                                    "점유부분": row_list[2],
                                    "정보출처": row_list[3],
                                    "점유의권원": row_list[5],
                                    "임대차기간": row_list[6],
                                    "보증금": row_list[8],
                                    "차임": row_list[10] if (len(row_list) > 10 and row_list[10] != "") else (row_list[11] if len(row_list) > 11 else ""),
                                    "전입신고": row_list[12] if len(row_list) > 12 else "",
                                    "확정일자": row_list[13] if len(row_list) > 13 else "",
                                    "배당요구": row_list[14] if len(row_list) > 14 else "",
                                }

                            elif len(row_list) == 16:
                                # print("case2: 1페이지에 <비고>가 없고 점유자 리스트가 여러 페이지에 있는 경우 (2433827_1)")

                                mapping = {
                                    "점유부분": row_list[2],
                                    "정보출처": row_list[3],
                                    "점유의권원": row_list[5],
                                    "임대차기간": row_list[6],
                                    "보증금": row_list[8],
                                    "차임": row_list[11],
                                    "전입신고": row_list[13],
                                    "확정일자": row_list[14],
                                    "배당요구": row_list[15],
                                }

                            elif len(row_list) == 10:
                                # print("case2-1: 1페이지에 <비고>가 없고 점유자 리스트가 여러 페이지에 있는 경우 - 1페이지가 아닌 경우 (2433827_1)")

                                # 페이지가 바뀌면서 위의 내용과 연결되어있는지 체크하고 이전 내용에 추가하는 부분
                                # print("row_list", row_list, tablePageCheck, ((row_list[0]!= "" and row_list[2] == "")  or (row_list[0] == "" and row_list[2] == "")))

                                if tablePageCheck and ((row_list[0]!= "" and row_list[2] == "")  or (row_list[0] == "" and row_list[2] == "")):
                                    tablePageCheck = False
                                    if last_occupant:
                                        last_occupant["name"] = (last_occupant["name"] + " " + row_list[0]).strip()
                                        last_occupant["unit"] = (last_occupant["unit"] + " " + row_list[1]).strip()
                                        last_occupant["info_source"] = (last_occupant["info_source"] + " " + row_list[2]).strip()
                                        last_occupant["occupancy_type"] = (last_occupant["occupancy_type"] + " " + row_list[3]).strip()
                                        last_occupant["move_in_date"] = (last_occupant["move_in_date"] + " " + row_list[7]).strip()
                                        last_occupant["confirmed_date"] = (last_occupant["confirmed_date"] + " " + row_list[8]).strip()
                                        last_occupant["dividend_claim_date"] = (last_occupant["dividend_claim_date"] + " " + row_list[9]).strip()
                                        last_occupant["deposit"] = (last_occupant["deposit"] + " " + row_list[5]).strip()
                                        last_occupant["rent"] = (last_occupant["rent"] + " " + row_list[6]).strip()
                                    continue

                                if tablePageCheck:
                                    tablePageCheck = False

                                mapping = {
                                    "점유부분": row_list[1],
                                    "정보출처": row_list[2],
                                    "점유의권원": row_list[3],
                                    "임대차기간": row_list[4],
                                    "보증금": row_list[5],
                                    "차임": row_list[6],
                                    "전입신고": row_list[7],
                                    "확정일자": row_list[8],
                                    "배당요구": row_list[9],
                                }

                            
                            # 최종 데이터 구조화
                            temp_data = {
                                "name": current_name,
                                "unit": mapping["점유부분"],
                                "info_source": mapping["정보출처"],
                                "occupancy_type": mapping["점유의권원"],
                                # "임대차기간": mapping["임대차기간"], # 현재 스키마에 없음
                                "move_in_date": mapping["전입신고"],
                                "confirmed_date": mapping["확정일자"],
                                "dividend_claim_date": mapping["배당요구"],
                                "deposit": mapping["보증금"],
                                "rent": mapping["차임"],
                            }

                            # 데이터 추가
                            result["occupants"].append(temp_data)

                            # 방금 넣은거 마지막으로 추가
                            last_occupant = result["occupants"][-1]
                            # print("="*60)
                            # print(last_occupant)
                            # print("="*60)

                # 권리 및 비고정보 넣기
                for i in range(len(df)):
                    # 행 전체 텍스트 합치기
                    full_row_text = " ".join([clean_val(v) for v in df.iloc[i].tolist()])
                    # print("===========")
                    # print('full_row_text', full_row_text)

                    # <비고> 수집 종료 조건 체크
                    stop_keywords = ["※ 최선순위 설정일자보다 대항요건을", "등기된 부동산", "매각에 따라 설정된", "비고란", "※1: 매각목적물에서 제외되는"]
                    if any(k in full_row_text for k in stop_keywords):
                        # print('해당 키워드 발견 ')
                        is_collecting_bigo = False
                        is_general_note = False

                    # <비고>
                    if "<비고>" in full_row_text:
                        is_collecting_bigo = True
                        content = full_row_text.replace("<비고>", "").strip()
                        # print("content", content)
                        if content:
                            result["tenant_note"] = (result["tenant_note"] + " " + content).strip()
                        continue

                    # <비고> 내용 누적 (플래그가 True일 때만 실행)
                    if is_collecting_bigo:
                        if full_row_text:  # 빈 행이 아닐 때만
                            result["tenant_note"] = (result["tenant_note"] + " " + full_row_text).strip()

                    # 등기된 부동산에 관한 권리 또는 가처분으로 매각으로 그 효력이 소멸되지 아니하는 것
                    if "등기된 부동산에 관한 권리 또는 가처분으로 매각으로" in full_row_text:
                        if i + 1 < len(df):
                            content = clean_val(df.iloc[i+1, 0])
                            if content:
                                result["surviving_rights"] = content.strip()

                    # 매각에 따라 설정된 것으로 보는 지상권의 개요
                    if "매각에 따라 설정된 것으로 보는 지상권의 개요" in full_row_text:
                        if i + 1 < len(df):
                            content = clean_val(df.iloc[i+1, 0])
                            if content:
                                result["surface_right_summary"] = content

                    # # 비고란
                    # if "비고란" in full_row_text:
                    #     if i + 1 < len(df):
                    #         content = clean_val(df.iloc[i+1, 0])
                    #         if content:
                    #             result["general_note"] = content

                    # 비고란
                    if "비고란" in full_row_text:
                        is_general_note = True
                        content = full_row_text.replace("비고란", "").strip()
                        if content:
                            result["general_note"] = (result["general_note"] + " " + content).strip()
                        continue

                    # 비고란 내용 누적 (플래그가 True일 때만 실행)
                    if is_general_note:
                        if full_row_text:  # 빈 행이 아닐 때만
                            result["general_note"] = (result["general_note"] + " " + full_row_text).strip()


    # 결과 출력 및 저장
    final_json = json.dumps(result, ensure_ascii=False, indent=4)
    # print(final_json)

    # 파일명 설정 (.pdf -> .txt)
    output_filename = target_filename.replace(".pdf", ".txt")
    output_path = os.path.join(output_dir, output_filename)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_json)

    print(f"\n===== {target_filename} 파일 파싱 완료했습니다. =====")

else:
    print(f"❌ 파일을 찾을 수 없습니다: {pdf_path}")