import fitz
import re

PDF_PATH = "1010-2303787_1.pdf"
print(f"===== {PDF_PATH} 파일 파싱 =====")

CASE_PATTERN = re.compile(r"\d{4}타경\d+")
DATE_PATTERN = re.compile(r"\d{4}\.\s*\d{1,2}\.\s*\d{1,2}")
ONLY_NUMBER = re.compile(r"^\d+$")


def parse_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    result = {}

    for page in doc:
        lines = extract_lines(page)
        # print(lines)

        # 1️⃣ 라벨 위치 매핑 (lines는 여기서 딱 1번만 순회)
        labels = find_labels(lines)

        # 2️⃣ 사건 (2줄 이상 가능)
        if "사건" in labels:
            case_text = parse_case(lines, labels["사건"])
            if case_text:
                result["사건"] = case_text

        # 3️⃣ 매각물건번호
        if "물건번호" in labels:
            num = find_nearest_number(lines, labels["물건번호"])
            if num:
                result["매각물건번호"] = num

        # 4️⃣ 작성일자 (라벨 오른쪽 + 아래)
        if "작성" in labels:
            date = find_date_near_label(lines, labels["작성"])
            if date:
                result["작성일자"] = date

        # 필요한 값 다 찾았으면 페이지 종료
        if len(result) >= 2:
            break

    return result


# -------------------------------------------------
# line 추출
# -------------------------------------------------
def extract_lines(page):
    lines = []

    for b_idx, b in enumerate(page.get_text("dict")["blocks"]):
        if b["type"] != 0:
            continue

        for l_idx, line in enumerate(b["lines"]):
            text = "".join(span["text"] for span in line["spans"]).strip()
            if not text:
                continue

            x0, y0, x1, y1 = line["bbox"]

            # print(f"[RAW] block={b_idx} line={l_idx} y={y0:.1f} x={x0:.1f} text='{text}'")

            lines.append({
                "text": text,
                "x0": x0,
                "y0": y0,
            })

    # y 기준 정렬
    lines.sort(key=lambda l: l["y0"])
    return lines


# -------------------------------------------------
# 라벨 위치 찾기
# -------------------------------------------------
def find_labels(lines):
    labels = {}

    for l in lines:
        txt = l["text"].strip()

        if txt == "사건":
            labels["사건"] = l
        elif txt == "물건번호":
            labels["물건번호"] = l
        elif txt == "작성":
            labels["작성"] = l

    return labels


# -------------------------------------------------
# 사건 (여러 줄 처리)
# -------------------------------------------------
def parse_case(lines, base):
    base_y = base["y0"]
    base_x = base["x0"]

    values = []

    for l in lines:
        txt = l["text"].strip()
        if not txt:
            continue

        y_diff = l["y0"] - base_y
        x_diff = l["x0"] - base_x

        # print(
        #     f"[CASE] '{txt}' "
        #     f"x0={l['x0']:.1f} y0={l['y0']:.1f} "
        #     f"y_diff={y_diff:.1f} x_diff={x_diff:.1f}"
        # )

        # ❌ 같은 행의 다른 컬럼 → 스킵
        if abs(y_diff) < 3 and x_diff > 150:
            continue

        # ✔ 사건 컬럼 범위
        if -15 <= y_diff <= 30 and 20 <= x_diff <= 120:
            values.append(txt)

    return ",".join(values) if values else None

# -------------------------------------------------
# 매각물건번호
# -------------------------------------------------
def find_nearest_number(lines, base_line, max_dist=25):
    base_y = base_line["y0"]
    candidates = []

    for l in lines:
        if abs(l["y0"] - base_y) <= max_dist:
            if ONLY_NUMBER.match(l["text"]):
                candidates.append(
                    (abs(l["y0"] - base_y), l["text"])
                )

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# -------------------------------------------------
# 작성일자 (라벨 오른쪽 + 아래)
# -------------------------------------------------
def find_date_near_label(lines, base_line):
    base_y = base_line["y0"]
    base_x = base_line["x0"]

    candidates = []

    for l in lines:
        y_diff = l["y0"] - base_y
        x_diff = l["x0"] - base_x

        # ✔ 작성 아래 줄
        if 5 <= y_diff <= 20:
            # ✔ 작성 오른쪽
            if x_diff >= 20:
                m = DATE_PATTERN.search(l["text"])
                if m:
                    candidates.append(
                        (abs(y_diff), m.group())
                    )

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# -------------------------------------------------
# 실행
# -------------------------------------------------
if __name__ == "__main__":
    data = parse_pdf(PDF_PATH)

    print("===== 파싱 결과 =====")
    for k, v in data.items():
        print(f"{k}: {v}")