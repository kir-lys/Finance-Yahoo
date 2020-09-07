# coding=utf-8
import requests
import csv
import time
import MySQLdb
import string
import datetime
import math
import _mysql_exceptions as DB_EXC
import os
import sys
import json

def old_api_data(ticker, period_sec=300, amount="30d"):
    url = 'https://finance.google.com/finance/getprices'
    payload = {'q': ticker,  # Stock
               # 'x': 'NASD',         # Exchange
               'i': period_sec,  # Interval seconds  ('i': '300')
               'p': amount,  # Period: load 25 minute (25m)
               'f': 'd,c',  # Data: date,close,volume,open,high,low ('f': 'd,c,v,o,h,l')
               'df': 'cpct',
               # 'auto': '0',
               # 'ts': '1508436697187' # Current timestamp
               }
    headers = {
        'authority': 'finance.google.com',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
        'referer': 'https://finance.google.com/finance',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'x-chrome-uma-enabled': '1',
        'x-compress': 'null',
        'x-requested-with': 'ShockwaveFlash/27.0.0.170'
    }
    r = requests.get(url, params=payload, headers=headers)
    #print r.url
    #print r.text
    strs = r.text.split("\n")
    data = []
    ts =0
    for i in range(7, len(strs)-1):
        #print strs[i]
        try:
            n, close = strs[i].split(',')
            # Получаем время текущей цены
            if n[0] == 'a':
                ts = int(n[1::])
                ts_n = ts
                # print ts, ' ', time.ctime(ts)
            elif ts != 0:
                ts_n = ts + int(n) * period_sec
            else:
                print "Error timestamp read!"
                break
            #print ts_n, '', close
            data.append([ts_n, close])
        except ValueError:
            print "ValueError: ", strs[i]
    return data

def api_data(ticker, interval='15m', range="5d"):
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/'+ticker
    payload = {
               'interval': interval,
               'range': range,

               'region': 'US',
               'lang': 'en-US',
               'includePrePost': 'false',
               'corsDomain': 'finance.yahoo.com',
               'tsrc': 'finance'
               }
    headers = {
        'authority': 'query1.finance.yahoo.com',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'referer': 'https://finance.yahoo.com/quote',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'x-compress': 'null',
    }
    r = requests.get(url, params=payload, headers=headers)
    #print r.url
    #print r.text

    obj = json.loads(r.text)
    if obj["chart"]["result"] is None:
        data = None
        return data

    try:
        timestamps = obj["chart"]["result"][0]["timestamp"]
    except KeyError:
        data = None
        return data
    prices = obj["chart"]["result"][0]["indicators"]["quote"][0]["close"]

    data = []
    for ts, close in zip(timestamps, prices):
        # print ts, close
        data.append([int(round(ts, -1)), round(close, 4)])

    return data
    
    
    
    
class Loader:
    def __init__(self,host="",user="",passwd="",db="dashboard"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        pass

    def __enter__(self):
        self.db = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd,
                                  db=self.db, charset='utf8', init_command='SET NAMES UTF8')
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
        #print "Connection DB closed!"

    def _insert_quotes(self, ticker_id, price, timestamp=None, table='Quotes_history'):
        if timestamp is None:
            # Current time is rounded to 5 minutes
            dt = datetime.datetime.today()
            minute = int(math.floor(datetime.datetime.now().minute / 5) * 5)
            dt = dt.replace(minute=minute, second=00)
            # print dt.strftime('%Y-%m-%d %H:%M:%S')  #2017-10-14 13:05:00
            # ------------------------------------
        else:
            dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(timestamp)))
        
        sql = "INSERT INTO `"+table+"`(ticker_id, datetime, price) VALUES ('" + str(ticker_id) + "','" + str(dt) + "','" + str(price) + "')"
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except DB_EXC.IntegrityError, e:
            print dt, ' ', ticker_id, ' ', price, ' ', e          ###### Error duplicated
            pass
        
    def insert_quotes(self, ticker_id, price, timestamp):
        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(timestamp)))
        query = "INSERT INTO Quotes_history(datetime, price, ticker_id) " \
                "VALUES(%s,%s,%s)"
        args = (dt, price, ticker_id)
        try:
            self.cursor.execute(query, args)
            self.db.commit()
        except DB_EXC.IntegrityError, e:
            #print dt, ' ', ticker_id, ' ', price, ' ', e          ###### Error duplicated
            pass
        
        """
        sql = "INSERT INTO `Quotes_history` (datetime, price, ticker_id) VALUES (" + str(ticker_id) + ",'" + str(dt) + "','" + str(price) + "')"
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except DB_EXC.IntegrityError, e:
            print dt, ' ', ticker_id, ' ', price, ' ', e          ###### Error duplicated
        """
        
    def _select(self,sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data

    def get_all_tickers(self):
        sql = "SELECT id, ticker FROM `Quotes_ticker` ORDER BY `avg_vol` DESC"
        data = self._select(sql)
        tickers = list()
        for rec in data:
            ticker, id = rec
            # Delete format symbols ex.:(u'BAC',)
            ticker = string.replace(str(ticker), "(u'", "")
            ticker = string.replace(ticker, "',)", "")
            # Add to array tickers
            tickers.append([ticker,id])
        return tickers

    def Load_M5_data(self, amount="1d"):
        # Tickers
        tickers = self.get_all_tickers()
        # Load
        if amount=="1d":   
            for t in tickers:
                print t[0], ' ', t[1]
                #print amount
                data1 = api_data(t[1],"15m",amount)
                data2 = api_data(t[1],"1d",amount)
                if (data1 is not None) and (data2 is not None):
                    data = data1 + [data2[1]]  # data2 - догружаем цену закрытия дня (23:00)
                else:
                    data = None

                #print t,' ', data
                if data is None:    # Если данные неудалось загрузить по какой либо причине
                    print t, " Empty ticker: ", data
                    continue
                
                for i in data:
                    try:
                        #print t, i[1], i[0]
                        self.insert_quotes(int(t[0]), float(i[1]), int(i[0]))
                    except IndexError:
                        print t, " IndexError: list index out of range! ", data
                    except TypeError:
                        print t, " TypeError! "
        else:
            for t in tickers:
                print t[0], ' ', t[1]
                #print amount
                data = api_data(t[1], "15m", amount)
                
                #print t,' ', data
                if data is None:    # Если данные неудалось загрузить по какой либо причине
                    print t, " Empty ticker: ", data
                    continue
                
                for i in data:
                    try:
                        # print t, i[1], i[0]
                        self.insert_quotes(int(t[0]), float(i[1]), int(i[0]))
                    except IndexError:
                        print t, " IndexError: list index out of range! ", data
                    except TypeError:
                        print t, " TypeError! "
        return tickers


# Check on the work of the exchange ------------------------------------------
def exchange_is_open(now=datetime.datetime.now()):
    # Checking WINTER TIME
    if now.date() >= datetime.date(2017, 11, 5) and now.date() < datetime.date(2018, 03, 11):
        if now.weekday()!= 5 and now.weekday() != 6:
            if now.time() >= datetime.time(17, 30) or now.time() <= datetime.time(00, 04):
                return True
    else:
        if now.weekday() != 5 and now.weekday() != 6:
            if now.time() >= datetime.time(16, 30) and now.time() <= datetime.time(23, 04):
                return True
    return False
# ----------------------------------------------------------------------------










print datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),' ',  #2017-10-14 13:05:00
_startTime = time.time()

# Принимаем из командной строки сколько нужно загрузить данных
if len(sys.argv) > 1:
    amount = str(sys.argv[1])
else:
    # Загрузить последний бар
    amount = "1d"
    # Check EXCHANGE is Open ======================================================
    """
    if not exchange_is_open():
        exit(200)
        pass
    """
    # =============================================================================


with Loader() as l:
    l.Load_M5_data(amount)
    #l._insert_quotes(7,56.64, 1509112800)
    #l.insert_quotes(77,56.64, 1509112800)
    
    """
    # Проверить какие тикеры не работают
    tkrs = l.get_all_tickers()
    for t in tkrs:
        if api_data(t[1],"1d","1d") is None:
            print t[0], ' ', t[1], ' isnt working'
    """

print "Running time: "+str(int(time.time() - _startTime))+" s."





