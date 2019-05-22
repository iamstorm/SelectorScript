import os
import datetime
import sqlite3
import baostock as bs
import pandas as pd
import easyquotation
from . import Utils
from . import DownDaydata as DY, Setting

tu = Utils.InitTuShare()

def FetchStockBasic():
    df0 = tu.pro.stock_basic(is_hs='N', fields='ts_code,symbol,name,list_date');
    df0['is_hs'] = pd.Series('N', df0.index);
    df1 = tu.pro.stock_basic(is_hs='H', fields='ts_code,symbol,name,list_date');
    df1['is_hs'] = pd.Series('H', df1.index);
    df2 = tu.pro.stock_basic(is_hs='S', fields='ts_code,symbol,name,list_date');
    df2['is_hs'] = pd.Series('S', df2.index);
    df = pd.concat([df0, df1, df2], ignore_index=True)
    return df

def FetchStock():
    Utils.Info("正在获取股票基本信息...")
    df = FetchStockBasic()
    for index, row in df.iterrows():
        row['list_date'] = row['list_date'].replace('-','');

    df['industry'] = pd.Series('', df.index)
    df['area'] = pd.Series('', df.index)
    df['sme'] = pd.Series(0, df.index)
    df['gem'] = pd.Series(0, df.index)
    df['st'] = pd.Series(0, df.index)
    df['hs300'] = pd.Series(0, df.index)
    df['sz50'] = pd.Series(0, df.index)
    df['zz500'] = pd.Series(0, df.index)

    return df

    Utils.Info("正在获取行业基本信息...")
    dfIndustry = tu.ts.get_industry_classified()
    Utils.Info("正在获取地区基本信息...")
    dfArea = tu.ts.get_area_classified()
    Utils.Info("正在获取中小板基本信息...")
    dfSme = tu.ts.get_sme_classified()
    Utils.Info("正在获取创业板基本信息...")
    dfGem = tu.ts.get_gem_classified()
    Utils.Info("正在获取ST基本信息...")
    dfSt = tu.ts.get_st_classified()
    Utils.Info("正在获取沪深300基本信息...")
    dfHs300 = tu.ts.get_hs300s()
    Utils.Info("正在获取上证50基本信息...")
    dfsz50 = tu.ts.get_sz50s()
    Utils.Info("正在获取中证500基本信息...")
    dfzz500 = tu.ts.get_zz500s()
    Utils.Info("正在设置基本信息...")
    progress = Utils.Progress(len(df))
    for index, row in df.iterrows():
        code =  row['symbol']
        progress.show("正在设置{0}的基本信息".format(code))
        rs = dfIndustry[dfIndustry['code'] == code]
        if not rs.empty:
            df.loc[index, 'industry'] = rs.iloc[0].at['c_name']

        rs = dfArea[dfArea['code'] == code]
        if not rs.empty:
            df.loc[index, 'area']  = rs.iloc[0].at['area']

        rs = dfSme[dfSme['code'] == code]
        if not rs.empty:
            df.loc[index, 'sme'] = 1

        rs = dfGem[dfGem['code'] == code]
        if not rs.empty:
            df.loc[index, 'gem'] = 1

        rs = dfSt[dfSt['code'] == code]
        if not rs.empty:
            df.loc[index, 'st'] = 1

        rs = dfHs300[dfHs300['code'] == code]
        if not rs.empty:
            df.loc[index, 'hs300'] = 1

        rs = dfsz50[dfsz50['code'] == code]
        if not rs.empty:
            df.loc[index, 'sz50'] = 1

        rs = dfzz500[dfzz500['code'] == code]
        if not rs.empty:
            df.loc[index, 'zz500'] = 1

        progress.step()

    progress.finish("完成设置基本信息。")
    return  df

def UpdateStock(conn):
    df = FetchStock()
    Utils.Info("正在写入股票基本信息...")
    conn.execute("DELETE FROM stock")
    df.to_sql(name='stock', con=conn, if_exists='append', index=False)
    Utils.Info("完成股票基本信息的写入。")
    conn.commit()
    return df

def UpdateAdjFactor(conn):
    stocks = Utils.GetStockCodeInfos(conn)
    Utils.Info("正在写入股票复权因子...")
    progress = Utils.Progress(len(stocks))
    for ts_code,symbol in stocks:
        start = int(Utils.ExeScalar(conn, "Select ifnull(max(trade_date), {0}) From AdjFactor Where code = '{1}'".format(Setting.DataFrom, symbol)))
        df = tu.pro.adj_factor(ts_code=ts_code, trade_date='')

        dfInRange = df[df['trade_date']>str(start)]
        dfInRange.sort_values(by=['trade_date'], inplace=True)
        dfInRange.drop_duplicates(['adj_factor'], inplace=True)
        dfInRange['code'] = pd.Series(symbol, dfInRange.index)
        dfInRange = dfInRange.loc[:,['code', 'trade_date', 'adj_factor']]
        dfInRange.to_sql(name='AdjFactor', con=conn, if_exists='append', index=False)
        progress.show("正在设置{0}的复权因子".format(symbol))
        progress.step()
    conn.commit()
    progress.finish("完成设置复权因子。")


def FetchTradeDate(conn):
    start = int(Utils.ExeScalar(conn, "Select ifnull(max(cal_date), {0}) From trade_date".format(Setting.DataFrom)))
    df = tu.pro.trade_cal(exchange_id='', start_date=str(start), end_date='')
    for index, row in df.iterrows():
        row['cal_date'] = row['cal_date'].replace('-','')
    df = df[(df.is_open==1) & (df['cal_date']>str(start))]
    df = df.loc[:,['cal_date']]
    return df

def UpdateTradeDate(conn):
    df = FetchTradeDate(conn)
    df.to_sql(name='trade_date', con=conn, if_exists='append', index=False)
    conn.commit()


def Fetch000001WithBaostock(startDate):
    #### 登陆系统 ####
    lg = bs.login()
    # 详细指标参数，参见“历史行情指标参数”章节
    rs = bs.query_history_k_data("sh.000001",
        "date,open,high,low,close,volume",
        start_date=Utils.DateForm1(startDate), end_date='',
        frequency="d", adjustflag="3")

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)

    for index, row in df.iterrows():
        row['date'] = row['date'].replace('-','');

    df.rename(columns={'date':'trade_date', 'volume':'vol'}, inplace = True)
    dfInRange = df[(df['trade_date']>str(startDate))]

    bs.logout()
    return dfInRange

def Fetch000001(startDate):
    df = tu.ts.get_k_data('000001', index=True, start=Utils.DateForm1(startDate), end='')
    df.rename(columns={'date':'trade_date', 'volume':'vol'}, inplace = True)
    for index, row in df.iterrows():
        df.loc[index, 'trade_date'] = df.loc[index, 'trade_date'].replace('-','');
    dfInRange = df[(df['trade_date']>str(startDate))]
    dfInRange = dfInRange.loc[:,['trade_date','open','close','high','low','vol']]
    return dfInRange

def Update000001(conn):
    start = Utils.ExeScalar(conn, "Select ifnull(max(trade_date), {0}) From [000001]".format(Setting.DataFrom))
    lastTradeDay = Utils.GetLastTradeDate(conn)
    if start >= lastTradeDay:
        print("大盘历史数据是完整的，不需要更新。")
        return
    Utils.Info("正在更新大盘数据{0}-现在...".format(start))
    df = Fetch000001(start);
 #  df = Fetch000001(start)
    df.to_sql(name='000001', con=conn, if_exists='append', index=False)
    Utils.Info("完成更新大盘数据。")


def Run(bindir):
    datadir = os.path.join(bindir, "data")
    if not os.path.exists(datadir):
         os.mkdir(datadir)
         Utils.Info("目录：{0}不存在，现在创建了, 开始执行初始化...".format(datadir))
    else:
         Utils.Info("目录：{0}已存在，继续执行初始化...".format(datadir))

    dbFile = Utils.GetGlobalFilePath(bindir)
    conn = sqlite3.connect(dbFile)
    stock_update_date = int(Utils.GetValFromSys(conn, "stock_update_date", "0"))
    if stock_update_date >= Utils.NowDate():
        print("股票基本资料目前已经是最新了。")
    else:
        count = Utils.ExeScalar(conn, "Select Count(*) From Stock")
        Utils.Info("正在获取最新股票数量...")
        fetchCount = len(FetchStockBasic())
        if count != fetchCount:
            Utils.Info("目前本地股票数量和服务器不一致，开始更新股票基本资料...")
            UpdateStock(conn)
            UpdateAdjFactor(conn)
        else:
            Utils.Info("目前本地股票数量和服务器一致，不需要更新股票基本资料。")

        UpdateTradeDate(conn)
        Utils.SetValToSys(conn, "stock_update_date", Utils.NowDate)
        conn.commit()


    Update000001(conn)
    easyquotation.update_stock_codes()


