import os
import sys
import sqlite3
import datetime
import pandas as pd
from . import Utils, RunTimeMode, DownDaydata

tu = Utils.InitTuShare()

def Run(bindir):
    Utils.MarkAsSuc(bindir)

