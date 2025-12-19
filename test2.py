import fitz

pdf_path = 'test_pdf/2588117_1.pdf'

doc = fitz.open(pdf_path)

print(f"--- Extracting text from {pdf_path} ---")

pdf_text = ""

for page_num in range(len(doc)):
    page = doc[page_num]
    pdf_text += page.get_text("text")
    print(pdf_text)