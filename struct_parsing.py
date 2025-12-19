import fitz
import re

from rich.console import Console
from rich.pretty import Pretty

console = Console()

PDF_PATH = "2433827_1.pdf"

# 찾는 패턴 정규식
DATE_PATTERN = re.compile(r"\d{4}\.\s*\d{1,2}\.\s*\d{1,2}")
ONLY_NUMBER = re.compile(r"^\d+$")
KOREAN_NAME = re.compile(r"^[가-힣]{2,4}$")
SEPARATOR_PATTERN = re.compile(r"-{5,}")
MONEY_PATTERN = re.compile(r"\b\d{1,3}(?:,\d{3})+\b")


# =================================================
# 1️⃣ 필드 정의 (여기만 수정하면 됨)
# =================================================
FIELD_DEFS = {
    "사건": {
        "label": "사건",
        "pattern": "below_multi",
        "x_range": (20, 120),
        "y_range": (-15, 30),
        "join": ", "
    },
    "매각물건번호": {
        "label": "물건번호",
        "pattern": "nearest_number",
        "max_dist": 25
    },
    "작성일자": {
        "label": "작성",
        "pattern": "right_or_below_date"
    },
    "담임법관": {
        "label": "담임법관",
        "pattern": "right_person"
    },
    "부동산 및 감정평가액": {
        "label": "부동산 및 감정평가액",
        "pattern": "second_line_below"
    },
    "최선순위 설정": {
        "label": "최선순위",
        "pattern": "second_line_below"
    },
    "배당요구종기": {
        "label": "배당요구종기",
        "pattern": "value_below"
    },
    "<비고>": {
        "label": "<비고>",
        "pattern": "same_x_diff_y",
        "stop_str": "※ 최선순위 설정일자보다"
    },
    "등기된 부동산에 관한 권리": {
        "label": "등기된 부동산에 관한 권리 또는 가처분으로 매각으로 그 효력이 소멸되지 아니하는 것",
        "pattern": "same_x_diff_y",
        "stop_str": "매각에 따라 설정된 것으로 보는 지상권의 개요"
    },
    "매각에 따라 설정된 것으로 보는 지상권의 개요": {
        "label": "매각에 따라 설정된 것으로 보는 지상권의 개요",
        "pattern": "same_x_diff_y",
        "stop_str": "비고란"
    },
    "비고란": {
        "label": "비고란",
        "pattern": "same_x_diff_y",
        "stop_str": "사건"
    }
}

COLUMNS = {
    "점유자": (30, 70),
    "점유부분": (40, 80),
    "정보출처": (90, 130),
    "점유의권원": (140, 180),
    "임대차기간": (180, 230),
    "보증금": (260, 300),
    "차임": (340, 360),
    "전입신고": (380, 435),
    "확정일자": (440, 500),
    "배당요구": (505, 565),
}


def normalize(text):
    return text.replace(" ", "")

# <비고> 찾기
def find_bigo_y(lines):
    for l in lines:
        if l["text"].strip() == "<비고>":
            return l["y0"]
    return None

# 비고 찾기 결과
bigoFindCheck = False

Y_TOLERANCE = 35.0

# 점유자 / 암차인 테이블 파싱
def extract_occupancy_table(lines):

    global bigoFindCheck

    header_line = next(
        (l for l in lines if "점유자" in l["text"] or "성 명" in l["text"]), 
        None
    )

    if bigoFindCheck is True:
        return
    
    # 해당 페이지에 헤더가 없다면, 이전 페이지에서 테이블이 이어지는지 확인하는 로직 필요
    if not header_line:
        # 만약 이전 페이지에서 테이블이 종료되지 않았다면 상단(y=0)부터 데이터로 간주
        header_y = 0 
    else:
        header_y = header_line["y0"] + 60

    # '<비고>' 찾기
    bigo_y = find_bigo_y(lines)
    if bigo_y:
        bigoFindCheck = True
        print('<비고>찾음')
    elif bigo_y is None and bigoFindCheck is False:
        print("<비고> 섹션의 Y 좌표를 찾을 수 없습니다.")
        # bigo_y를 찾지 못했으면 임시로 아주 큰 값(문서 끝)을 설정
        bigo_y = 1000
    else:
        bigo_y = 1000


    # 4. 필터링: header_y보다 크고 (<비고>보다 작은) 모든 lines 출력
    filtered_lines = [
        l for l in lines
        if header_y < l["y0"] < bigo_y
    ]
    if not filtered_lines: return []

    print('filter line', filtered_lines)


    # 3. 논리적 행 시작점 찾기 (정보출처 칼럼 근처의 시작 텍스트)
    # X 좌표 110~170 사이의 텍스트를 기준으로 행을 구분합니다.
    # (예: '등기사항전', '현황조사', '권리신고')
    row_start_lines = []
    # 이미 처리된 Y 좌표를 저장하여 중복된 행 시작 방지
    processed_y = set()


    for l in filtered_lines:
        # 정보출처 또는 점유의 권원 칼럼 근처에 있는 텍스트만 시작점으로 고려
        if 90 <= l["x0"] <= 180:
            current_y = l["y0"]
            
            if not any(abs(current_y - y) < 60 for y in processed_y):
                row_start_lines.append(l)
                processed_y.add(current_y)
    
    row_start_lines.sort(key=lambda x: x["y0"])

    results = []
    last_occupant = ""


    for i, start_line in enumerate(row_start_lines):
        y_start = start_line["y0"]
        y_end = row_start_lines[i+1]["y0"] if i+1 < len(row_start_lines) else bigo_y
        
        # 해당 Y 범위 내의 모든 텍스트 수집
        current_row_lines = [l for l in filtered_lines if y_start - 5 <= l["y0"] < y_end - 2]
        
        temp_row = {}
        for col, (x1, x2) in COLUMNS.items():
            # X 좌표 범위 내 텍스트 수집 후 Y 좌표순 정렬
            col_lines = [l for l in current_row_lines if x1 <= l["x0"] <= x2]
            col_lines.sort(key=lambda x: x["y0"])
            
            # 텍스트 합치기 (필요에 따라 " " 또는 "" 선택)
            combined = " ".join([l["text"].strip() for l in col_lines])
            temp_row[col] = combined

        # 이름이 비어있으면 위 행의 이름을 가져옴 (Vertical Merge 대응)
        if temp_row["점유자"]:
            last_occupant = temp_row["점유자"]
        else:
            temp_row["점유자"] = last_occupant

        results.append(temp_row)


    return results


# =================================================
# PDF 파싱 메인
# =================================================
def parse_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    result = {}

    after_separator = False
    all_occupancy_list = []


    for page in doc:
        lines = extract_lines(page)
        labels = find_labels(lines)

        # for field, cfg in FIELD_DEFS.items():
        #     label = cfg["label"]
        #     if label not in labels:
        #         continue

        #     extractor = EXTRACTORS[cfg["pattern"]]
        #     value = extractor(lines, labels[label], cfg)
        #     result[field] = value

        # for l in lines:
        #     text = l["text"].strip()

        #     # 1️⃣ 구분선 발견
        #     if SEPARATOR_PATTERN.search(text):
        #         after_separator = True
        #         continue

        #     # 2️⃣ 구분선 이후의 감정평가액만 허용
        #     if after_separator and "감정평가액" in text:
        #         m = MONEY_PATTERN.search(text)
        #         if m:
        #             result["감정평가액"] = m.group()

        # money_list = extract_auction_price_table(lines)
        # if money_list:
        #     result['회차 리스트'] = money_list

        page_occupancy = extract_occupancy_table(lines)
        if page_occupancy:
            all_occupancy_list.extend(page_occupancy)
        
    result = {"점유자현황": all_occupancy_list}
    return result


# =================================================
# 3️⃣ line 추출
# =================================================
def extract_lines(page):
    lines = []

    for b in page.get_text("dict")["blocks"]:
        if b["type"] != 0:
            continue

        for line in b["lines"]:
            text = "".join(span["text"] for span in line["spans"]).strip()
            if not text:
                continue

            x0, y0, _, _ = line["bbox"]

            lines.append({
                "text": text,
                "x0": x0,
                "y0": y0,
            })

    lines.sort(key=lambda l: l["y0"])
    return lines


# =================================================
# 4️⃣ 라벨 찾기
# =================================================
def find_labels(lines):
    labels = {}
    for l in lines:
        if l["text"] in [cfg["label"] for cfg in FIELD_DEFS.values()]:
            labels[l["text"]] = l
    return labels


# =================================================
# 5️⃣ 패턴별 추출기
# =================================================
def extract_below_multi(lines, base, cfg):
    bx, by = base["x0"], base["y0"]
    values = []

    for l in lines:
        x_diff = l["x0"] - bx
        y_diff = l["y0"] - by

        if cfg["x_range"][0] <= x_diff <= cfg["x_range"][1] \
            and cfg["y_range"][0] <= y_diff <= cfg["y_range"][1]:
            values.append(l["text"])

    return cfg.get("join", " ").join(values) if values else None


def extract_nearest_number(lines, base, cfg):
    by = base["y0"]
    candidates = []

    for l in lines:
        if abs(l["y0"] - by) <= cfg["max_dist"]:
            if ONLY_NUMBER.match(l["text"]):
                candidates.append((abs(l["y0"] - by), l["text"]))

    candidates.sort()
    return candidates[0][1] if candidates else None


def extract_right_or_below_date(lines, base, cfg):
    bx, by = base["x0"], base["y0"]

    for l in lines:
        y_diff = l["y0"] - by
        x_diff = l["x0"] - bx

        if 5 <= y_diff <= 20 and x_diff >= 20:
            m = DATE_PATTERN.search(l["text"])
            if m:
                return m.group()

    return None


def extract_right_person(lines, base, cfg):
    bx, by = base["x0"], base["y0"]

    candidate = ""

    for l in lines:
        x_diff = l["x0"] - bx
        y_diff = l["y0"] - by

        # 오른쪽 블록
        if x_diff < 50:
            continue

        # 아래 영역 (같은 줄 제외)
        if not (5 <= y_diff <= 30):
            continue

        text = l["text"].strip()

        # 이름 패턴
        if KOREAN_NAME.match(text):
            candidate = text
    
    return candidate


# 오른쪽 바로 아래 두번째줄 텍스트 가져오기
def extract_second_line_in_next_block(lines, base, cfg):
    bx = base["x0"]
    by = base["y0"]

    candidate = ""

    for l in lines:
        y_diff = l["y0"] - by
        x_diff = abs(l["x0"] - bx)

        # print(f"l: {l}, y_diff: {y_diff} , x_diff: {x_diff}")

        # 같은 컬럼 + 아래
        if 5 <= y_diff <= 10 and x_diff <= 120:
            candidate = l["text"].strip()

    return candidate

# 바로 아래값 가져오기
def extract_value_below_label(lines, label_text, cfg):
    
    label_line = next(
        (l for l in lines if l["text"].strip() == label_text["text"]),
        None
    )

    if not label_line:
        return None

    bx = label_line["x0"]
    by = label_line["y0"]

    candidates = []

    for l in lines:
        y_diff = l["y0"] - by
        x_diff = abs(l["x0"] - bx)

        # 배당요구종기 x좌표만 다른 경우
        if y_diff == 0 and x_diff <= 70:
            candidates.append((y_diff, l["text"].strip()))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])

    # 날짜만 필터 (선택)
    for _, text in candidates:
        if DATE_PATTERN.search(text):
            return text

    return candidates[0][1]


# X좌표는 같고 Y좌표만 다른 경우 가져오기
def extract_same_x_diff_y(lines, label_text, cfg):

    # print(label_text)
    label_line = next(
        (l for l in lines if l["text"].strip() == label_text["text"]),
        None
    )

    if not label_line:
        return None

    bx = label_line["x0"]
    by = label_line["y0"]

    candidates = []
    
    for l in lines:

        stop_str = cfg.get("stop_str")
        stop_flag = False
        y_diff = l["y0"] - by
        x_diff = abs(l["x0"] - bx)

        # x좌표는 같고 y좌표가 다른 경우 여러줄
        if x_diff == 0 and y_diff > 0:

            if (stop_str and stop_str in l["text"]) or stop_flag == True:
                stop_flag = True
                break
            # print(f"{l}, x_diff: {x_diff}, y_diff: {y_diff}")
            candidates.append(l["text"].strip())

    return "\n".join(candidates)


def extract_auction_price_table(lines):

    # 1️⃣ 헤더 찾기
    header_line = next(
        (l for l in lines if "회차" in normalize(l["text"]) and "기일" in normalize(l["text"])),
        None
    )

    if not header_line:
        return None


    header_text = header_line["text"]
    header_x = header_line["x0"]
    header_y = header_line["y0"]


    # 2️⃣ 컬럼 x 기준 수집
    col_names = ["회차", "기 일", "최저매각가격", "매수신청보증금"]
    columns = {}
    for name in col_names:
        idx = header_text.find(name)
        if idx != -1:
            # 글자 위치 → 대략적인 x좌표 환산
            columns[name] = header_x + idx * 5

    if len(columns) < 4:
        return None

    # x 기준 정렬
    sorted_cols = sorted(columns.items(), key=lambda x: x[1])

    # 3️⃣ 데이터 row 수집
    rows = []
    for l in lines:

        if l["y0"] <= header_y:
            continue
        text = l["text"].strip()
        if not SEPARATOR_PATTERN.search(text):
            rows.append(l)

    # y 기준 그룹핑 (같은 줄)
    row_map = {}
    for l in rows:
        key = round(l["y0"], 1)
        row_map.setdefault(key, []).append(l)

    # 4️⃣ row → dict 변환

    ROW_PATTERN = re.compile(
        r'(?P<회차>\d+회)\s+'
        r'(?P<기일>\d{4}\.\d{2}\.\d{2})\s+'
        r'(?P<최저매각가격>[\d,]+)\s+'
        r'(?P<매수신청보증금>[\d,]+)'
    )

    results = []
    for _, row_lines in sorted(row_map.items()):
        if not row_lines:
            continue

        text = row_lines[0]["text"]

        m = ROW_PATTERN.search(text)
        if not m:
            continue

        results.append({
            "회차": m.group("회차"),
            "기 일": m.group("기일"),
            "최저매각가격": m.group("최저매각가격"),
            "매수신청보증금": m.group("매수신청보증금"),
        })

    return results

# =================================================
# 6️⃣ 패턴 매핑
# =================================================
EXTRACTORS = {
    "below_multi": extract_below_multi,
    "nearest_number": extract_nearest_number,
    "right_or_below_date": extract_right_or_below_date,
    "right_person": extract_right_person,
    "second_line_below": extract_second_line_in_next_block,
    "value_below": extract_value_below_label,
    "same_x_diff_y": extract_same_x_diff_y
}


# =================================================
# 7️⃣ 실행
# =================================================
if __name__ == "__main__":
    data = parse_pdf(PDF_PATH)

    console.print("[bold white]===== 파싱 결과 =====[/bold white]")

    for k, v in data.items():
        console.print(f"[bold cyan]{k}[/bold cyan]:")

        if isinstance(v, (list, dict)):
            console.print(Pretty(v, indent_guides=True))
        else:
            console.print(f"  {v}")