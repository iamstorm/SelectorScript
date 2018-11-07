import os
import sys
import sqlite3
import traceback
import datetime
import pandas as pd
import time
import struct
from . import Utils, Setting

tu = Utils.InitTuShare()

def AppendToBinary(df, daydir, symbol):
    binfile = os.path.join(daydir, symbol+".day")
    with open(binfile,'ab') as f:
        for index, row in df.iterrows():
            for col in ['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']:
                f.write(struct.pack('i', int(row[col])))



def CreateDaySqliteTable(conn, stocks, orgEmpty):
    if orgEmpty:
        sSQL = '''
        CREATE TABLE SysInfo ( 
            [KEY] VARCHAR( 200 )   PRIMARY KEY
                                   NOT NULL
                                   UNIQUE,
            val   VARCHAR( 4000 ) 
        );
        '''
        conn.execute(sSQL)
        conn.commit()
        conn.execute("Insert into SysInfo (key, val) Values ('update_date', '{0}')".format(Setting.DataFrom))
        conn.commit()
    Utils.Info("正在为每个股票创建表结构...")
    progress = Utils.Progress(len(stocks))
    for ts_code,symbol in stocks:
        progress.show("正在创建{0}的表".format(symbol))
        sSQL = '''
            CREATE TABLE IF NOT EXISTS [{0}] (
                trade_date   INT         PRIMARY KEY
                                         NOT NULL
                                         UNIQUE,
                open   INT,
                close  INT,
                high   INT,
                low    INT,
                vol INT,
                amount INT
            );
            '''.format(symbol)
        conn.execute(sSQL)
        progress.step()
    progress.finish("完成为每个股票创建表结构")
    conn.commit()


def DownloadDayRecord(datadir, conn, globalConn, stocks):
    lastTradeDay = Utils.GetLastTradeDate(globalConn)
    allUpdateDate = int(Utils.GetValFromSys(conn, 'update_date'))
    if allUpdateDate >= lastTradeDay :
        Utils.Info("所有股票的历史数据是完整的，不需要更新。")
        return

    daydir = os.path.join(datadir, "day")
    if not os.path.exists(daydir):
        Utils.Info("目录：{0}不存在，现在创建了, 下载数据...".format(daydir))
        os.mkdir(daydir)

    progress = Utils.Progress(len(stocks))      
    for ts_code,symbol in stocks:
        start = int(Utils.ExeScalar(conn, "Select ifnull(max(trade_date), {0}) From [{1}]".format(Setting.DataFrom, symbol)))
        if start >= lastTradeDay:
            msg = "{0}的历史数据{1}-现在是完整的...".format(ts_code, start)
            progress.show(msg)
            progress.step()
            continue

        msg = "开始获取{0}的历史数据{1}-现在...".format(ts_code, start)
        progress.show(msg)
        while True:
            try:
                df = tu.pro.daily(ts_code=ts_code, start_date=str(start), end_date='')
                break
            except:
                Utils.Err("获取{0}的历史数据{1}-现在异常，将等待5秒后自动重试...".format(ts_code, start))
                time.sleep(5)

        df = df.loc[:,['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']]
        for index, row in df.iterrows():
            row['trade_date'] = row['trade_date'].replace('-','');

        dfInRange = df[(df['vol']>0) & (df['trade_date']>str(start))]
        dfInRange = Utils.NormlizePrice(dfInRange, ['open', 'high', 'low', 'close'])

        dfInRange.sort_values(by=['trade_date'], inplace=True)
        dfInRange.to_sql(name=symbol, con=conn, if_exists='append', index=False)
        AppendToBinary(dfInRange, daydir, symbol)
        conn.commit()
        progress.step()

    Utils.SetValToSys(conn, 'update_date', lastTradeDay)
    conn.commit()
    progress.finish("完成所有股票历史数据的获取。")

def Downlod(datadir, globalConn):
    dbFile =Utils.GetDaySqliteFilePath(datadir)
    orgEmpty = not os.path.exists(dbFile) 
    conn = sqlite3.connect(dbFile)
    stocks = Utils.GetStockCodeInfos(globalConn)
    CreateDaySqliteTable(conn, stocks, orgEmpty)
    DownloadDayRecord(datadir, conn, globalConn, stocks)

def UpdateDayData(bindir):
    datadir = os.path.join(bindir, "data")
    dbFile = Utils.GetGlobalFilePath(bindir)
    globalConn = sqlite3.connect(dbFile)
    Downlod(datadir, globalConn)
    globalConn.close()
    Utils.MarkAsSuc(bindir)