import pandas as pd
import numpy as np
import sqlite3

def 連接資料庫(db):
    global dfRevenue,dfFin,ids

    # 連接到資料庫A ====================================================================
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    sql = '''select * from revenue t'''
    dfRevenue = pd.read_sql(sql,conn)

    # 連接到資料庫B
    sql2 = '''select * from financialStatements t'''
    dfFin = pd.read_sql(sql2,conn)

    # 連接到資料庫C
    sqlStock = '''select * from stock t'''
    dfStock = pd.read_sql(sqlStock,conn)
    ids = dfStock['code']
    # ==================================================================================
    conn.close()

def 月營收統計(stocks):
    global dataExport1

    # 整理【月營收】欄位
    dfRevenue2 = dfRevenue.copy()
    dfRevenue2 = dfRevenue2.drop(columns=['open','high','low','updownYuan','updown'])
    dfRevenue2.columns = ['代碼','月別','收盤','營收(億)','月增(%)','年增(%)','累計營收(億)','累計年增(%)']
    dfRevenue2

    # 【月營收資料】
    dataExport1 = pd.DataFrame(columns = ['代碼','收排','收盤','營收(億)','月增(%)','年增(%)','月別','累計營收(億)','累計年增(%)','當月營收排名','累計排名'])

    for id in stocks:
        dfRevenue2mask = dfRevenue2['代碼'] == id
        dfRevenue3 = dfRevenue2[dfRevenue2mask]
        dfRevenue3 = dfRevenue3.sort_values(by='月別', ascending=False) # 重整排名

        # 收盤排名,當月營收排名 ========================================================
        dfRevenue3.insert(8,'收排',dfRevenue3['收盤'].rank(ascending=False).round(2).astype(int))
        dfRevenue3.insert(9,'當月營收排名',dfRevenue3['營收(億)'].rank(ascending=False).round(2).astype(int))

        # 當月累計排名 ================================================================
        當月 = dfRevenue3.iloc[0]['月別'].split('/',1) # 去除年份取月份
        當月 = 當月[1]
        累計 = dfRevenue3['月別'].str.contains(當月,na=False) # 取出所有同期月份，並為同期月份進行排名
        dfRevenue3.insert(10,'累計排名',dfRevenue3[累計]['累計營收(億)'].rank(ascending=False))
        dfRevenue3 = dfRevenue3.fillna(0)
        # ============================================================================
        dataExport1 = pd.concat([dataExport1,dfRevenue3],axis=0,ignore_index = True)

def 季財報統計(stocks):
    global dataExport2
    
    dfFin2 = dfFin.copy()
    # loc可以使用文字，iloc則是使用數字
    dfFin2 = dfFin2.loc[:,['代碼','營業利益率','每股稅後盈餘(元)','稅後淨利年成長率','營業利益年成長率','季度']]

    # 【季財報資料】
    dataExport2 = pd.DataFrame(columns = ['代碼','營業利益率','盈比','盈4','盈單','稅後淨利年成長率','營業利益年成長率','季度','每股稅後盈餘(元)'])

    for id in stocks:
        dfFin2mask = dfFin2['代碼'] == id
        dfFin3 = dfFin2[dfFin2mask]
        dfFin3 = dfFin3.sort_values(by='季度', ascending=False) # 重整排名

        盈餘單 = []
        盈餘4 = []

        for y in range(len(dfFin3['季度'])):
            期別期 = dfFin3.iloc[y]['季度'][4:]

            if 期別期 == 'Q1':
                # 盈單 =================================================
                try:
                    盈餘 = dfFin3.iloc[y]['每股稅後盈餘(元)']
                    盈餘單.append(盈餘)
                except:
                    盈餘單.append(0)

                # 盈4
                try:
                    盈4 = dfFin3.iloc[y]['每股稅後盈餘(元)'] + dfFin3.iloc[y+1]['每股稅後盈餘(元)'] - dfFin3.iloc[y+4]['每股稅後盈餘(元)']
                    盈餘4.append(盈4)
                except:
                    盈餘4.append(0)
            elif 期別期 == 'Q2':
                # 盈單 =================================================
                try:
                    盈餘 = dfFin3.iloc[y]['每股稅後盈餘(元)'] - dfFin3.iloc[y+1]['每股稅後盈餘(元)']
                    盈餘單.append(盈餘)
                except:
                    盈餘單.append(0)

                # 盈4
                try:
                    盈4 = dfFin3.iloc[y]['每股稅後盈餘(元)'] + dfFin3.iloc[y+2]['每股稅後盈餘(元)'] - dfFin3.iloc[y+4]['每股稅後盈餘(元)']
                    盈餘4.append(盈4)
                except:
                    盈餘4.append(0)
            elif 期別期 == 'Q3':
                # 盈單
                try:
                    盈餘 = dfFin3.iloc[y]['每股稅後盈餘(元)'] - dfFin3.iloc[y+1]['每股稅後盈餘(元)']
                    盈餘單.append(盈餘)
                except:
                    盈餘單.append(0)

                # 盈4
                try:
                    盈4 = dfFin3.iloc[y]['每股稅後盈餘(元)'] + dfFin3.iloc[y+3]['每股稅後盈餘(元)'] - dfFin3.iloc[y+4]['每股稅後盈餘(元)']
                    盈餘4.append(盈4)
                except:
                    盈餘4.append(0)
            elif 期別期 == 'Q4':
                # 盈單
                try:
                    盈餘 = dfFin3.iloc[y]['每股稅後盈餘(元)'] - dfFin3.iloc[y+1]['每股稅後盈餘(元)']
                    盈餘單.append(盈餘)
                except:
                    盈餘單.append(0)

                # 盈4
                try:
                    盈4 = dfFin3.iloc[y]['每股稅後盈餘(元)']
                    盈餘4.append(盈4)
                except:
                    盈餘4.append(0)
            
        try:
            dfFin3['盈單'] = 盈餘單
        except:
            dfFin3['盈單'] = 0

        try:
            dfFin3['盈4'] = 盈餘4
        except:
            dfFin3['盈4'] = 0

        dfFin3['盈比'] = dfFin3['盈單']*4/dfFin3['盈4']
        dfFin3.replace([np.inf, -np.inf], 0, inplace=True)
        dataExport2 = pd.concat([dataExport2,dfFin3],axis=0,ignore_index = True)

def 合併報表(stocks):
    global data導出

    data月營收 = dataExport1.drop(columns=['營收(億)','累計營收(億)'])
    data財報 = dataExport2.drop(columns=['每股稅後盈餘(元)'])
    data月營收.fillna(0)
    data財報.fillna(0)

    data導出 = pd.DataFrame(columns = ['代碼','營業利益率','盈比','盈4','盈單','稅後淨利年成長率','營業利益年成長率','季度','收排','收盤','月增(%)','年增(%)','月別','累計年增(%)','當月營收排名','累計排名'])

    for id in ids:
        data月營收mask = data月營收['代碼'] == id
        data月營收2 = data月營收[data月營收mask]
        data財報mask = data財報['代碼'] == id
        data財報2 = data財報[data財報mask]

        for y in range(len(data月營收2['月別'])):

            def 合併月營收季營收(datamonth,dataSeason):
                global data導出
                dataExport1mask = data月營收2['月別'] == datamonth
                dataExport1b = data月營收2[dataExport1mask]

                dataExport2mask = data財報2['季度'] == dataSeason
                dataExport2b = data財報2[dataExport2mask]
                dataExportConcat = pd.merge(dataExport1b,dataExport2b,on='代碼')
                data導出 = pd.concat([data導出,dataExportConcat],axis=0,ignore_index = True)

            季報月 = data月營收2.iloc[y]['月別']
            判斷月別 = data月營收2.iloc[y]['月別'].split('/',1)
            判斷年 = 判斷月別[0]
            判斷月 = 判斷月別[1]


            if 判斷月 == '04' or 判斷月 == '05' or 判斷月 == '06': # Q1
                季度判斷年 = 判斷年+'Q1'
                合併月營收季營收(季報月,季度判斷年)
            elif 判斷月 == '07' or 判斷月 == '08' or 判斷月 == '09': # Q2
                季度判斷年 = 判斷年+'Q2'
                合併月營收季營收(季報月,季度判斷年)
            elif 判斷月 == '10' or 判斷月 == '11' or 判斷月 == '12': # Q3
                季度判斷年 = 判斷年+'Q3'
                合併月營收季營收(季報月,季度判斷年)
            elif 判斷月 == '01' or 判斷月 == '02' or 判斷月 == '03': # Q4
                季度判斷年 = 判斷年+'Q4'
                合併月營收季營收(季報月,季度判斷年)

if __name__ == "__main__":
    連接資料庫('goodinfoRevenue.db')
    月營收統計(ids)
    季財報統計(ids)
    合併報表(ids)