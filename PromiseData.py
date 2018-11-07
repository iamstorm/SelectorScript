import sys, getopt
import os
import tushare as ts
import datetime
from ProcessMode import Utils, InitMode, StartMode, RunTimeMode, EndMode

print("exe.start")

def usage():
    print("python.exe PromiseData.py path -m start//runtime, end, fetch")

opts, args = getopt.getopt(sys.argv[2:], "hm:")
bindir = sys.argv[1]
mode=""
for op, value in opts:
    if op == "-m":
        mode = value
    elif op == "-h":
        usage()
        sys.exit()

mode = mode.strip()

if mode == 'start':
    print("开始数据的准备工作...")
    InitMode.Run(bindir)
    StartMode.Run(bindir)
elif mode == 'runtime':
    print("开始获取实时数据...")
    RunTimeMode.Run(bindir)
elif mode == 'end':
    print("退出前的一些数据整理工作...")
    EndMode.Run(bindir)
else:
    raise Exception("Invalid mode!", mode)
