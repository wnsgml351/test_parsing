import fitz

pdf_path = '1010-2303787_1.pdf'

doc = fitz.open(pdf_path)

print(f"--- Extracting text from {pdf_path} ---")

def in_area(block, area):
    x0, y0, x1, y1 = block[:4]
    ax0, ay0, ax1, ay1 = area
    return ax0 <= x0 <= ax1 and ay0 <= y0 <= ay1


pdf_text = ""
for page_num in range(len(doc)):
    print(f"===== {page_num} =====")
    page = doc[page_num]
    blocks = page.get_text("blocks")

    for block in blocks:
        x0, y0, x1, y1, text = block[:5]
        if page_num == 0:
            print(f"[{x0:.1f}, {y0:.1f}, {x1:.1f}, {y1:.1f}] {text}")
        # text = text.strip()
        # if not text:
        #     continue



print(pdf_text)
