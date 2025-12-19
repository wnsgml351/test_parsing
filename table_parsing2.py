import pdfplumber
import pandas as pd

def pdf_to_excel(pdf_path, excel_path):
    # 1. PDF 열기
    with pdfplumber.open(pdf_path) as pdf:
        all_tables = []
        
        # 2. 모든 페이지를 순회하며 표 추출
        for page in pdf.pages:
            # table_settings: 선(lines)을 기준으로 표를 인식
            table = page.extract_table(table_settings={
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 3, # 선이 약간 어긋나도 합쳐서 인식
            })
            
            if table:
                df = pd.DataFrame(table)
                
                # 3. 병합된 셀(Rowspan) 처리
                # PDF 표에서 병합된 셀은 첫 줄만 값이 있고 나머지는 None인 경우가 많습니다.
                # 이를 위에서 아래로(forward fill) 채워줍니다.
                df = df.fillna(method='ffill')
                
                all_tables.append(df)

        # 4. 여러 표가 있을 경우 하나로 병합
        if all_tables:
            final_df = pd.concat(all_tables, ignore_index=True)
            
            # 5. 엑셀 파일로 저장
            final_df.to_excel(excel_path, index=False, header=False)
            print(f"성공: {excel_path} 파일이 생성되었습니다.")
        else:
            print("표를 찾지 못했습니다.")

# 실행
pdf_to_excel("2560813_1.pdf", "경매물건명세서_결과.xlsx")