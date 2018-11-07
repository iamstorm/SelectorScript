import os
import sys
import sqlite3
import datetime
import tushare as ts
from . import Setting


class Tu:
   def __init__(self, ts, pro):
      self.ts = ts
      self.pro = pro

def InitTuShare():
    ts.set_token(Setting.ApiToken)
    pro = ts.pro_api()
    return Tu(ts, pro)


def GetGlobalFilePath(bindir):
    return os.path.join(bindir, "global.data")


def GetDaySqliteFilePath(datadir):
    return os.path.join(datadir, "day.data")

def NowDate():
    return int(datetime.datetime.now().strftime('%Y%m%d'))

def ExeScalar(conn, sSQL):
    c = conn.execute(sSQL)
    for row in c:
        return row[0]

    raise Exception("SQL:{0}没有任何记录".format(sSQL))

def GetValFromSys(conn, key, defVal=''):
    c = conn.execute("Select val From SysInfo Where key = '{0}'".format(key))

    for row in c:
        return row[0]
    return defVal

def SetValToSys(conn, key, val):
    conn.execute("Update SysInfo Set val = '{0}' Where key = '{1}'".format(val, key))

class Progress:
    def __init__(self, count, bar_length=Setting.DefBarLen):
      self.count = count
      self.finishCount = 0
      self.bar_length = bar_length

    def show(self, msg):
        percent = self.finishCount*100.0/self.count
        hashes = '#' * int(percent/100.0 * self.bar_length)
        spaces = ' ' * (self.bar_length - len(hashes))
        sys.stdout.write("\r{:}总进度[{:}] {:.2f}%".format(msg, hashes + spaces, percent))
        sys.stdout.flush()

    def step(self, count = 1):
        self.finishCount += 1;

    def finish(self, msg):
        percent = 100
        hashes = '#' * int(percent/100.0 * self.bar_length)
        spaces = ' ' * (self.bar_length - len(hashes))
        sys.stdout.write("\r{:}总进度[{:}] {:.2f}%".format(msg, hashes + spaces, percent))
        sys.stdout.flush()
        print("\n")

def DateForm1(dateFrom0, sep='-'):
    year = int(dateFrom0/10000)
    month =  int((dateFrom0-year*10000)/100)
    day = dateFrom0 - year*10000 - month*100
    month = '{:02}'.format(month)
    day = '{:02}'.format(day)
    return "{0}{1}{2}{3}{4}".format(year, sep, month, sep, day)

def MarkAsSuc(bindir):
    dbFile = GetGlobalFilePath(bindir)
    conn = sqlite3.connect(dbFile)
    SetValToSys(conn, "pyrun", "1")
    conn.commit()
    conn.close()

def GetLastTradeDate(conn):
    now = NowDate()
    return int(ExeScalar(conn, "SELECT cal_date from trade_date where cal_date < {0}  order by cal_date desc limit 1".format(now)))

def GetNextTradeDay(conn, date):
    return int(ExeScalar(conn, "SELECT cal_date from trade_date where cal_date > {0}  order by cal_date limit 1".format(date)))


def GetStockCodeInfos(conn):
    c = conn.execute("Select ts_code,symbol from stock order by ts_code")
    retlist = []
    for row in c:
        retlist.append((row[0], row[1]))

    return retlist

def NormlizePrice(df, colNames):
    cp = df.loc[:, df.columns]
    for colName in colNames:
        cp[colName] = (cp[colName].astype(float)*10000).astype(int)
    return cp


def Info(msg):
    if Setting.PrintInfo:
        print(msg)

def Err(msg):
    print(msg)
