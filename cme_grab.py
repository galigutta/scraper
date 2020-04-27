#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 23:18:43 2020

@author: vamsi
"""

import pandas_market_calendars as mcal
import requests
import time

cme = mcal.get_calendar('CME')

trade_cal=cme.valid_days(start_date='20200101', end_date='20200426')
trade_cal_str=trade_cal.strftime('%Y%m%d')
open_days=[]
for tc in trade_cal_str:
    open_days.append(tc)
open_days.sort(reverse=True)
pid=str(429)


def get_xls_from_cme(rep_date,pid):
	url = 'https://www.cmegroup.com/CmeWS/exp/voiProductDetailsViewExport.ctl?media=xls&tradeDate='+rep_date+'&reportType=P&productId='+pid
	r = requests.get(url, allow_redirects=True)
	open('/tmp/cme/'+rep_date+'.xls', 'wb').write(r.content)

for tc in open_days:
    print(tc)
    time.sleep(1)
    get_xls_from_cme(tc,pid)