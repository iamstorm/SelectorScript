import os
import sqlite3
import datetime
import pandas as pd
import sys
import requests
import re
from . import Utils

tu = Utils.InitTuShare()

def FetchRunTimeData():
    df = tu.ts.get_today_all()
    df = df.loc[:, ['code','trade','open','high','low','volume','amount']]
    dfValid = df[df['open']>0]
    dfValid = Utils.NormlizePrice(dfValid, ['open', 'high', 'low', 'trade'])
    dfValid['volume'] = (dfValid['volume'].astype(float)/100.0)
    dfValid['amount'] = (dfValid['amount'].astype(float)/1000.0)
    return dfValid

def FetchRunTimeDataByDFCFW(stockCount):
    url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQueryABC&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&cmd=C._A&st=(ChangePercent)&sr=-1&p=1&ps={0}'.format(stockCount+1000)

    response = requests.get(url)
    rawstr = response.content.decode()
    datastr = re.sub(r'^jQueryABC\(\s*', "", rawstr)
    datastr = re.sub(r'\)\s*$', "", datastr)
    dataArr=eval(datastr)
    df = pd.DataFrame()
    sArr = []
    for x in dataArr:
        s = pd.Series(x.split(',')).iloc[[1, 3, 6, 7, 9, 10, 11]]
        sArr.append(s)

    df = df.append(sArr, ignore_index=True)
    df.rename(columns={1:'code', 3:'trade', 6: 'volume', 7: 'amount', 9: 'high', 10: 'low', 11: 'open'}, inplace = True)
    df = df[df['volume'] != '-']
    df = Utils.NormlizePrice(df, ['trade', 'high', 'low', 'open'])
    df['amount'] = (df['amount'].astype(float)/1000.0)
    return df

def Fetch000001RunTimeData():
    df = tu.ts.get_realtime_quotes('sh')
    df.rename(columns={'price':'close', 'volume':'vol'}, inplace = True)
    df = df.loc[:,['open','close','high','low','vol']]
    df = df.loc[:, df.columns]
    df['vol'] = (df['vol'].astype(float)*100)
    return df

def UpdateRunTime(bindir):
    datadir = os.path.join(bindir, "data")
    now = datetime.datetime.now();
    dbFile = Utils.GetGlobalFilePath(bindir)
    conn = sqlite3.connect(dbFile)
    runtime_time = int(Utils.GetValFromSys(conn, 'runtime_time', defVal='0'))
    if runtime_time>int(now.date().strftime("%Y%m%d15")):
        return

    stockCount = int(Utils.ExeScalar(conn, "Select Count(1) From Stock"))

    Utils.Info("开始下载实时数据...")
    nowtime = int(now.date().strftime("%Y%m%d")+now.time().strftime("%H"))
    df = FetchRunTimeDataByDFCFW(stockCount)
    conn.execute("Delete From runtime");
    conn.commit()
    df.drop_duplicates(['code'], inplace=True)
    df.to_sql(name='runtime', con=conn, if_exists='append', index=False)

    df = Fetch000001RunTimeData()
    conn.execute("Delete From [000001runtime]");
    conn.commit()
    df.to_sql(name='000001runtime', con=conn, if_exists='append', index=False)

    Utils.SetValToSys(conn, 'runtime_time', nowtime)
    conn.commit()
    Utils.Info("完成实时数据获取。")

    conn.close()

def Run(bindir):
    try:
        UpdateRunTime(bindir)
    except:
        print(sys.exc_info())
        traceback.print_exc(file=sys.stdout)
        raise    
    Utils.MarkAsSuc(bindir)
