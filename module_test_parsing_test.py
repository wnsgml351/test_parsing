import os
from db_manager import get_db_connection
import pdfplumber
import re
import pandas as pd
import datetime
import json
import time

def write_error_log(main_folder, sub_folder, filename, error_msg):
    """
    에러 발생 시 error/error_{main_folder}.txt 파일에 기록합니다.
    """
    error_dir = "error"
    if not os.path.exists(error_dir):
        os.makedirs(error_dir)
    print('main_folder', main_folder)
    log_file_path = os.path.join(error_dir, f"error_{main_folder}.txt")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] File: {sub_folder}/{filename}\n")
        f.write(f"Error: {error_msg}\n")
        f.write("-" * 50 + "\n")


# DB 데이터 정제
def clean_special_chars(val):
    if val is None:
        return None
    if not isinstance(val, str):
        return val  # 숫자인 경우 그대로 반환

    # 1. 문제의 \u2024를 일반 마침표로 변경
    # 2. 기타 euckr에서 깨질 수 있는 유니코드 공백(\u00a0 등) 처리
    cleaned = val.replace('\u2024', '.').replace('\u00a0', ' ')
    return cleaned.strip()

# DB 저장
def save_to_db(data):
    # 공통 함수를 호출하여 연결 객체 생성
    conn = get_db_connection()
    if not conn:
        return False, "DB Connection Error"

    cursor = conn.cursor()
    try:

        # 매각물건번호 정수형으로 전환
        item_no = int(data['item_no']) if str(data['item_no']).isdigit() else None

        # 매각 테이블에 추가
        master_sql = """
                    INSERT INTO tmp_maegak (
                        case_no, item_no, priority_date, dividend_end_date, document_date,
                        tenant_note, surviving_rights, surface_right_summary, general_note,
                        pdf_file_path
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        cursor.execute(master_sql, (
            clean_special_chars(data['case_no']), item_no, clean_special_chars(data['priority_date']),
            clean_special_chars(data['dividend_end_date']), clean_special_chars(data['document_date']),
            clean_special_chars(data['tenant_note']), clean_special_chars(data['surviving_rights']),
            clean_special_chars(data['surface_right_summary']), clean_special_chars(data['general_note']),
            clean_special_chars(data['pdf_path'])
        ))

        # 생성된 PK 가져오기
        parent_idx = cursor.lastrowid

        # 회차 정보 테이블 (temp_maegak_rounds) 저장
        if data['auction_rounds']:
            rounds_sql = """
                        INSERT INTO tmp_maegak_rounds (
                            parent_idx, round_no, auction_date, min_bid_price, bid_deposit
                        ) VALUES (%s, %s, %s, %s, %s)
                    """
            for r in data['auction_rounds']:
                # '1회' -> 1 숫자만 추출
                round_num = int(re.sub(r'[^0-9]', '', r['round_no'])) if r['round_no'] else 0
                # 가격 데이터 콤마/공백 제거 후 숫자로 변환
                min_price = int(re.sub(r'[^0-9]', '', r['min_bid_price'])) if r['min_bid_price'] else 0
                deposit = int(re.sub(r'[^0-9]', '', r['bid_deposit'])) if r['bid_deposit'] else 0

                cursor.execute(rounds_sql, (parent_idx, round_num, clean_special_chars(r['auction_date']), min_price, deposit))

        # 점유자 정보 저장
        if data['occupants']:
            occ_sql = """
                        INSERT INTO tmp_maegak_occupants (
                            parent_idx, name, unit, info_source, occupancy_type,
                            move_in_date, confirmed_date, dividend_claim_date, deposit, rent
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            for d in data['occupants']:
                cursor.execute(occ_sql, (
                    parent_idx,
                    clean_special_chars(d['name']),
                    clean_special_chars(d['unit']),
                    clean_special_chars(d['info_source']),
                    clean_special_chars(d['occupancy_type']),
                    clean_special_chars(d['move_in_date']),
                    clean_special_chars(d['confirmed_date']),
                    clean_special_chars(d['dividend_claim_date']),
                    clean_special_chars(d['deposit']),
                    clean_special_chars(d['rent'])
                ))

        conn.commit()


        return True, parent_idx
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()


# \n -> 한 칸 띄어씌기로 변경 함수
def line_change_spacing_val(val):
    return str(val).replace('\n', ' ').strip() if val else ""

# \n -> 공백 제거 함수
def line_change_no_spacing_val(val):
    return str(val).replace('\n', '').strip() if val else ""


# 키워드 다음의 값 찾기 (사건번호, 매각물건번호, 작성일자, 최선순위 설정, 배당요구종기)
def get_value_next_keyword(df, keyword):
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
                        next_val = line_change_spacing_val(row[next_idx])
                        if next_val:
                            return next_val
    return ""


# 기본사건정보(Header) 가져오기
def get_default_case_data(table):
    try:

        # 테이블을 데이터프레임으로 변환
        df = pd.DataFrame(table)

        # 데이터가 없는 경우 스킵
        if df.empty:
            return None

        # 결과 데이터
        result = {}

        if len(df.columns) >= 10:

            # 사건번호
            result["case_no"] = get_value_next_keyword(df, "사건")

            # 매각물건번호
            result["item_no"] = get_value_next_keyword(df, "물건번호")

            # 작성일자
            result["document_date"] = get_value_next_keyword(df, "작성 일자")

            # 최선순위 설정
            result["priority_date"] = get_value_next_keyword(df, "최선순위")

            # 배당요구종기
            result["dividend_end_date"] = get_value_next_keyword(df, "배당요구종기")

        return result

    except Exception as e:
        print(f"❌ get_default_case_data 오류 발생: {e}")
        return None


# 점유자 현황 가져오기
def get_occupants(table, current_name, last_occupant, is_change_table_page):

    try:
        # 테이블을 데이터프레임으로 변환
        df = pd.DataFrame(table)

        # 데이터가 없는 경우 스킵
        if df.empty:
            return None

        # 결과 데이터
        result = []

        if len(df.columns) >= 10:
            # 점유자 추출 범위 설정
            start_search = df[df.iloc[:, 0].str.contains("점유자", na=False)]
            end_search = df[df.iloc[:, 0].str.contains("<비고>|※|등기된 부동산|매각에 따라", na=False)]

            start_idx = start_search.index[0] + 1 if not start_search.empty else 0
            end_idx = end_search.index[0] if not end_search.empty else len(df)

            # 점유자 데이터 파싱
            if start_idx != -1:
                occ_df = df.iloc[start_idx:end_idx].copy()

                for _, row in occ_df.iterrows():
                    # 데이터 정제: None 제거 및 줄바꿈 처리
                    row_list = [str(v).replace('\\n', ' ').replace('\n', ' ').strip() if v is not None else "" for v in
                                row.tolist()]
                    if not "".join(row_list) or any(k in "".join(row_list) for k in ["점유자", "성명", "점유부분", "정보출처"]):
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
                            "차임": row_list[10] if (len(row_list) > 10 and row_list[10] != "") else (
                                row_list[11] if len(row_list) > 11 else ""),
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
                            "전입신고": row_list[12] if row_list[12] != "" else row_list[13],
                            "확정일자": row_list[13] if row_list[13] != "" and row_list[14] == "" else row_list[14],
                            "배당요구": row_list[15],
                        }

                    elif len(row_list) == 10:
                        # print("case2-1: 1페이지에 <비고>가 없고 점유자 리스트가 여러 페이지에 있는 경우 - 1페이지가 아닌 경우 (2433827_1)")

                        # 페이지가 바뀌면서 위의 내용과 연결되어있는지 체크하고 이전 내용에 추가하는 부분
                        # print("row_list", row_list, tablePageCheck, ((row_list[0]!= "" and row_list[2] == "")  or (row_list[0] == "" and row_list[2] == "")))

                        if is_change_table_page and (
                                (row_list[0] != "" and row_list[2] == "") or (row_list[0] == "" and row_list[2] == "")):
                            is_change_table_page = False
                            if last_occupant:
                                last_occupant["name"] = (last_occupant["name"] + " " + row_list[0]).strip()
                                last_occupant["unit"] = (last_occupant["unit"] + " " + row_list[1]).strip()
                                last_occupant["info_source"] = (
                                            last_occupant["info_source"] + " " + row_list[2]).strip()
                                last_occupant["occupancy_type"] = (
                                            last_occupant["occupancy_type"] + " " + row_list[3]).strip()
                                last_occupant["move_in_date"] = (
                                            last_occupant["move_in_date"] + " " + row_list[7]).strip()
                                last_occupant["confirmed_date"] = (
                                            last_occupant["confirmed_date"] + " " + row_list[8]).strip()
                                last_occupant["dividend_claim_date"] = (
                                            last_occupant["dividend_claim_date"] + " " + row_list[9]).strip()
                                last_occupant["deposit"] = (last_occupant["deposit"] + " " + row_list[5]).strip()
                                last_occupant["rent"] = (last_occupant["rent"] + " " + row_list[6]).strip()
                            continue

                        if is_change_table_page:
                            is_change_table_page = False

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
                    details = {
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

                    result.append(details)
                    last_occupant = result[-1]

        return { "occupants": result }

    except Exception as e:
        print(f"❌ get_general_notes 오류 발생: {e}")
        return None


# 권리 및 비고 정보 가져오기
def get_general_notes(table, is_collecting_bigo, is_surviving_rights, is_surface_right_summary, is_general_note):

    try:
        df = pd.DataFrame(table)
        if df.empty:
            return None

        # 결과 데이터
        result = {
            "tenant_note": "", # 임차인 관련 비고 전체 문구
            "surviving_rights": "", # 말소되지 않는 권리 목록
            "surface_right_summary": "", # 지상권 관련 문구 전체
            "general_note": "", # 매각물건 명세서 마지막 문서 전체 비고 내용
        }

        # 권리 및 비고정보 넣기
        for i in range(len(df)):
            # 행 전체 텍스트 합치기
            full_row_text = " ".join([line_change_spacing_val(v) for v in df.iloc[i].tolist()])
            # print("===========")
            # print('full_row_text', full_row_text)

            # <비고> 수집 종료 조건 체크
            stop_keywords = ["※ 최선순위 설정일자보다 대항요건을", "등기된 부동산", "매각에 따라 설정된", "비고란", "※1: 매각목적물에서 제외되는"]
            if any(k in full_row_text for k in stop_keywords):
                # print('해당 키워드 발견 ')
                is_collecting_bigo = False
                is_surviving_rights = False
                is_surface_right_summary = False
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
                is_surviving_rights = True
                content = full_row_text.replace("등기된 부동산에 관한 권리 또는 가처분으로 매각으로 그 효력이 소멸되지 아니하는 것", "").strip()
                if content:
                    result["surviving_rights"] = (result["surviving_rights"] + " " + content).strip()
                continue

            # "등기된 부동산에 관한 권리 또는 가처분으로 매각으로 그 효력이 소멸되지 아니하는 것" 내용 누적 (플래그가 True일 때만 실행)
            if is_surviving_rights:
                if full_row_text:  # 빈 행이 아닐 때만
                    result["surviving_rights"] = (result["surviving_rights"] + " " + full_row_text).strip()

            # 매각에 따라 설정된 것으로 보는 지상권의 개요
            if "매각에 따라 설정된 것으로 보는 지상권의 개요" in full_row_text:
                is_surface_right_summary = True
                content = full_row_text.replace("매각에 따라 설정된 것으로 보는 지상권의 개요", "").strip()
                if content:
                    result["surface_right_summary"] = (result["surface_right_summary"] + " " + content).strip()
                continue

            if is_surface_right_summary:
                if full_row_text:  # 빈 행이 아닐 때만
                    result["surface_right_summary"] = (
                                result["surface_right_summary"] + " " + full_row_text).strip()

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

        return result

    except Exception as e:
        print(f"❌ get_general_notes 오류 발생: {e}")
        return None




# 회차별 기일 정보
def get_rounds_data(page):

    # 결과
    results = []

    full_text = page.extract_text()
    if not full_text:
        return results

    # 정규식 패턴으로 회차별 기일정보 찾기
    round_pattern = re.compile(r"(\d+회)\s+(\d{4}\.\d{2}\.\d{2})\s+([\d,]+)(?:\s+([\d,]{7,}))?")
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
            "round_no": round_str,  # 회차번호
            "auction_date": date_str,  # 매각기일
            "min_bid_price": min_price,  # 최저매각가격
            "bid_deposit": deposit  # 매수신청보증금
        }

        if not any(r['round_no'] == round_str and r['auction_date'] == date_str for r in results):
            results.append(round_data)

    return results


# PDF 파싱 로직
def pdf_maegak_parsing(pdf_path):

    # 리턴 데이터
    result = {
        "result_code": 200,
        "result_msg": "정상처리되었습니다.",
        "pdf_path": pdf_path,
        "case_no": "",  # 사건번호 (예: 2025타경100211)
        "item_no": "",  # 매각물건번호 (예: 1)
        "priority_date": "",  # 최선순위권 설정일 및 권리 종류 (예: 2023.10.16. 압류)
        "dividend_end_date": "",  # 배당요구종기일
        "document_date": "",  # 작성일자
        "occupants": [],  # 점유자별 상세정보
        "tenant_note": "",  # 임차인 관련 비고 전체 문구
        "surviving_rights": "",  # 말소되지 않는 권리 목록
        "surface_right_summary": "",  # 지상권 관련 문구 전체
        "general_note": "",  # 매각물건 명세서 마지막 문서 전체 비고 내용
        "auction_rounds": [],  # 회차별 기일정보
    }

    if not os.path.exists(pdf_path):
        result["result_code"] = 404
        result["result_msg"] = "해당 파일이 없습니다."
        return result

    # 비고 수집 체크 로직
    is_collecting_bigo = False

    # 말소되지 않는 권리 목록 체크 로직
    is_surviving_rights = False

    # 지상권 관련 문구 전체 체크 로직
    is_surface_right_summary = False

    # 비고란 수집 체크 로직
    is_general_note = False

    # 병합된 이름 처리를 위한 변수
    current_name = ""

    # 이전 배열 요소를 참조하기 위한 변수
    last_occupant = None

    try:

        # PDF 파싱 시작
        with pdfplumber.open(pdf_path) as pdf:

            for p_idx, page in enumerate(pdf.pages):

                ##### PDF 테이블 가져오는 부분 시작 #####

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

                ##### PDF 테이블 가져오는 부분 종료 #####

                ##### 데이터 가져오기 및 결과 데이터 설정 #####
                for table in tables:

                    # 테이블 페이징 체크 변경
                    is_change_table_page = True

                    # 테이블을 데이터프레임으로 변환
                    df = pd.DataFrame(table)

                    # 데이터가 없는 경우 스킵
                    if df.empty:
                        continue

                    # 1. 기본사건정보(Header) 가져오기
                    if result["case_no"] == "":
                        case_data = get_default_case_data(table)
                        if case_data and case_data["case_no"]:
                            result["case_no"] = case_data["case_no"]
                            result["item_no"] = case_data["item_no"]
                            result["priority_date"] = case_data["priority_date"]
                            result["dividend_end_date"] = case_data["dividend_end_date"]
                            result["document_date"] = case_data["document_date"]

                    # 2. 권리 및 비고정보 가져오기
                    general_notes = get_general_notes(table, is_collecting_bigo, is_surviving_rights, is_surface_right_summary, is_general_note)
                    if general_notes:
                        if general_notes["tenant_note"] and result["tenant_note"] == "":
                            result["tenant_note"] = general_notes["tenant_note"]
                        if general_notes["surviving_rights"] and result["surviving_rights"] == "":
                            result["surviving_rights"] = general_notes["surviving_rights"]
                        if general_notes["surface_right_summary"] and result["surface_right_summary"] == "":
                            result["surface_right_summary"] = general_notes["surface_right_summary"]
                        if general_notes["general_note"] and result["general_note"] == "":
                            result["general_note"] = general_notes["general_note"]

                    # 3. 점유자별 상세정보 가져오기
                    occupants_data = get_occupants(table, current_name, last_occupant, is_change_table_page)
                    # print('occupants_data', occupants_data, len(occupants_data))
                    if occupants_data and "occupants" in occupants_data:
                        result["occupants"].extend(occupants_data["occupants"])
                        if result["occupants"]:
                            last_occupant = result["occupants"][-1]
                            current_name = last_occupant["name"]

                # 4. 회차별 기일 정보 가져오기
                rounds = get_rounds_data(page)
                if rounds and len(rounds) > 0:
                    result["auction_rounds"].extend(rounds)


    except Exception as e:
        # print(f"❌ 알 수 없는 오류 발생: {e}")
        result["result_code"] = 999
        result["result_msg"] = str(e)
    return result




# 실행
if __name__ == "__main__":

    # 파싱할 메인 폴더
    parsing_folder_name = "parsing_pdf_test"

    # 1. 메인 경로 설정
    base_path = os.getcwd()  # 현재 작업 디렉토리 기준
    target_sub_path = os.path.join("VM-02", "업로드")

    full_main_path = os.path.join(base_path, parsing_folder_name)

    # 메인 폴더 존재 여부 확인
    if not os.path.exists(full_main_path):
        print(f"❌ '{parsing_folder_name}' 폴더를 찾을 수 없습니다.")
        exit(0)

    # 시작시간
    start_time = time.time()

    print(f"🔍 집계 시작 경로: {full_main_path}\n" + "=" * 45)

    # 폴더명만 필터링하여 리스트업
    parsing_list_folders = [d for d in os.listdir(full_main_path)
                            if os.path.isdir(os.path.join(full_main_path, d))]

    for item in parsing_list_folders:
        # print('item', item)

        # 최종 탐색 경로 생성
        upload_path = os.path.join(full_main_path, item, target_sub_path)

        if os.path.exists(upload_path):
            # ~_1 로 끝나는 PDF 파일 찾기
            pdf_files = [f for f in os.listdir(upload_path) if f.lower().endswith('_1.pdf')]

            count = len(pdf_files)
            print(f"{item} 폴더: {count}개의 PDF 파일이 있습니다.")

            # 해당 pdf_files를 pdf_maegak_parsing 돌리기
            for pdf_file in pdf_files:
                pdf_full_path = os.path.join(upload_path, pdf_file)
                print(f"   📄 [{pdf_file}] 파싱 시작...")

                parsed_data = pdf_maegak_parsing(pdf_full_path)
                print(json.dumps(parsed_data, indent=4, ensure_ascii=False))
                # print(parsed_data["document_date"], len(parsed_data["document_date"]))



        else:
            print(f"{item} : (경로 없음 - {target_sub_path})")

    # 종료시간
    end_time = time.time()

    # 3. 소요 시간 계산 (초 단위)
    elapsed_total_seconds = int(end_time - start_time)

    # 4. 시, 분, 초로 변환
    hours = elapsed_total_seconds // 3600
    minutes = (elapsed_total_seconds % 3600) // 60
    seconds = elapsed_total_seconds % 60

    print("=" * 45)

    # 결과 출력
    print(f"⏱️ 총 소요 시간: {hours}시간 {minutes}분 {seconds}초")

    print(f"✨ 전체 작업 완료. 에러 내역은 error/error_{parsing_folder_name}.txt 를 확인하세요.")