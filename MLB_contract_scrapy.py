# MLB合約資料爬蟲 2021.01.19

from tqdm import tqdm
import calendar
import time
from polib.CsvEngn import *
import concurrent.futures

# def mlb_contract_scrapy_year_loop(star_year, end_year=2020):
    
#     start_time = time.time()
#     result_df = pd.DataFrame(columns=range(0,4+1))
#     for year in tqdm(range(star_year, end_year+1)):
#         for month in range(1,12):
#             print(f">> {year}-{month}\n")
#             n=0
#             while True:
# #                 print(n)
#                 url_object = f"http://www.prosportstransactions.com/baseball/Search/SearchResults.php?Player=&Team=&BeginDate={year}-{str(month).zfill(2)}-01&EndDate={year}-{str(month).zfill(2)}-{str(calendar.monthrange(year, month)[1])}&PlayerMovementChkBx=yes&MinorsChkBx=yes&DLChkBx=yes&InjuriesChkBx=yes&PersonalChkBx=yes&DisciplinaryChkBx=yes&LegalChkBx=yes&submit=Search&start={n}"
#                 df = pd.read_html(url_object)
#                 if len(df[0])==1:
#                     break
#     #             print(result_df)
#                 result_df = pd.concat([result_df, df[0].iloc[1:,:]])
#                 n = n + 25

#         result_df = result_df.drop_duplicates().reset_index(drop=True)

#         col_name_lst = ['Date', 'Team', 'Acquired', 'Relinquished', 'Notes']
#         result_df.columns = col_name_lst
#         end_time = time.time()

#         formtoPkl(result_df, f"MLB合約資料(每日)_{year}年")
#         print(f">> {(end_time - start_time)/60} 分鐘爬取 ")
        
def mlb_contract_scrapy_one_year(year):
    year = int(year)
    start_time = time.time()
    result_df = pd.DataFrame(columns=range(0,4+1))
    for month in tqdm(range(1,12)):
        print(f">> {year}-{month}\n")
        n=0
        while True:
#             print(n)
            url_object = f"http://www.prosportstransactions.com/baseball/Search/SearchResults.php?Player=&Team=&BeginDate={year}-{str(month).zfill(2)}-01&EndDate={year}-{str(month).zfill(2)}-{str(calendar.monthrange(year, month)[1])}&PlayerMovementChkBx=yes&MinorsChkBx=yes&DLChkBx=yes&InjuriesChkBx=yes&PersonalChkBx=yes&DisciplinaryChkBx=yes&LegalChkBx=yes&submit=Search&start={n}"
            df = pd.read_html(url_object)
            if len(df[0])==1:
                break
            result_df = pd.concat([result_df, df[0].iloc[1:,:]])
            n = n + 25

    result_df = result_df.drop_duplicates().reset_index(drop=True)

    col_name_lst = ['Date', 'Team', 'Acquired', 'Relinquished', 'Notes']
    result_df.columns = col_name_lst
    end_time = time.time()

    formtoPkl(result_df, f"MLB合約資料(每日)_{year}年")
    print(f">> {(end_time - start_time)/60} 分鐘爬取 ")
    
if __name__ == '__main__':
    year = [2002,2005,2007,2008]
    # 同時建立及啟用每年一個執行緒
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(year)) as executor:
        executor.map(mlb_contract_scrapy_one_year, year)
        
    print("\n\n>> 全數執行完畢!!")