import os
import sqlite3
import pandas as pd
import datetime
from . import Utils
from . import DownDaydata as DY

tu = Utils.InitTuShare()

def Run(bindir):
    DY.UpdateDayData(bindir)

    Utils.MarkAsSuc(bindir)
