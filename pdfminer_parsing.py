from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import BytesIO

def convert_pdf_to_html(pdf_path, output_html_path):
    # 1. 리소스 관리자 및 출력 스트림 설정
    rsrcmgr = PDFResourceManager()
    retstr = BytesIO()
    
    # 2. HTML 변환기 설정 (여기서 LAParams로 레이아웃 분석)
    # 텍스트 기반 스트림이 아닌 바이너리 스트림(BytesIO)을 사용하므로 
    # 반드시 codec='utf-8'을 지정해야 합니다.
    device = HTMLConverter(rsrcmgr, retstr, codec='utf-8', laparams=LAParams())
    
    # 3. PDF 파일 열기 및 인터프리터 실행
    with open(pdf_path, 'rb') as fp:
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        # 페이지별로 처리
        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
    
    # 4. 결과물 가져오기 및 파일 저장
    html_content = retstr.getvalue().decode('utf-8')
    
    with open(output_html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # 리소스 닫기
    device.close()
    retstr.close()
    
    print(f"변환 완료: {output_html_path}")

if __name__ == "__main__":
    try:
        convert_pdf_to_html("1010-2303787_1.pdf", "full_page.html")
    except Exception as e:
        print(f"오류 발생: {e}")