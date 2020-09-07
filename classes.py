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

def api_data(ticker, period_sec=300, amount="30d"):
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



class Loader:
    def __init__(self):
        pass

    def __enter__(self):
        self.db = MySQLdb.connect(host="", user="", passwd="",
                                  db="", charset='utf8', init_command='SET NAMES UTF8')
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
        #print "Connection DB closed!"

    def _insert_quotes(self, ticker, price, timestamp=None, table='intraday_history'):
        if timestamp is None:
            # Current time is rounded to 5 minutes
            dt = datetime.datetime.today()
            minute = int(math.floor(datetime.datetime.now().minute / 5) * 5)
            dt = dt.replace(minute=minute, second=00)
            # print dt.strftime('%Y-%m-%d %H:%M:%S')  #2017-10-14 13:05:00
            # ------------------------------------
        else:
            dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(timestamp)))

        sql = "INSERT INTO `"+table+"`(ticker, datetime, price) VALUES ('" + ticker + "','" + str(dt) + "','" + str(price) + "')"
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except DB_EXC.IntegrityError, e:
            #print dt, ' ', ticker, ' ', price, ' ', e          ###### Error duplicated
            pass

    def _select(self,sql):
        # sql = "SELECT distinct ticker,ticker_googlefin FROM `intraday_tickers` ORDER BY `av_volume` DESC"
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data

    def get_all_tickers(self,with_google_fin=False, only_google_fin=False):
        sql = "SELECT distinct ticker,ticker_googlefin FROM `intraday_tickers` ORDER BY `av_volume` DESC"
        data = self._select(sql)
        tickers = list()
        for rec in data:
            ticker, ticker_googlefin = rec
            # Delete format symbols ex.:(u'BAC',)
            ticker = string.replace(str(ticker), "(u'", "")
            ticker = string.replace(ticker, "',)", "")
            ticker_googlefin = string.replace(str(ticker_googlefin), "(u'", "")
            ticker_googlefin = string.replace(ticker_googlefin, "',)", "")
            # Add to array tickers
            if with_google_fin:
                tickers.append([ticker,ticker_googlefin])
            elif only_google_fin:
                tickers.append(ticker_googlefin)
            else:
                tickers.append(ticker)
        return tickers

    def Load_M5_data(self, ticker=None, last_bar=True):
        if last_bar is True:
            amount = "25m"
        else:
            amount = last_bar
        # Tickers
        if ticker is None:
            tickers = self.get_all_tickers()
        else:
            """ # If String
            tickers = ticker.split(',')
            for i in range(len(tickers)):
                tickers[i] = ' '.join(tickers[i].split())
            """
            tickers = ticker

        # Load
        if last_bar is True:
            for t in tickers:
                get = api_data(t, 300, amount)
                try:
                    data = get[len(get)-1]
                    if int(data[0])==int((time.time() // (5*60)) * 5*60):
                        self._insert_quotes(t.encode('utf-8'), float(data[1]), int(data[0]))
                except IndexError:
                    #print t, " IndexError: list index out of range! ", get
                    pass
                except TypeError:
                    print t, " TypeError! ", get
        else:
            for t in tickers:
                data = api_data(t, 300, amount)
                for i in data:
                    try:
                        # print t, i[1], i[0]
                        self._insert_quotes(t, i[1], i[0])
                    except IndexError:
                        print t, " IndexError: list index out of range! ", data
                    except TypeError:
                        print t, " TypeError! ", get
        return tickers

    def Load_M30_data(self, ticker=None, last_bar=True):
        if last_bar is True:
            amount = "2d"
        else:
            amount = last_bar
        if ticker is None:
            tickers = self.get_all_tickers()
        else:
            tickers = ticker

        # Load
        if last_bar is True:
            for t in tickers:
                get = api_data(t, 1800, amount)
                try:
                    data = get[len(get)-1]
                    self._insert_quotes(t.encode('utf-8'), float(data[1]), int(data[0]), table='m30_history')
                except IndexError:
                    #print t, " IndexError: list index out of range! ", get
                    pass
                except TypeError:
                    print t, " TypeError! ", get
        else:
            for t in tickers:
                data = api_data(t, 1800, amount)
                for i in data:
                    try:
                        # print t, i[1], i[0]
                        self._insert_quotes(t, i[1], i[0], table='m30_history')
                    except IndexError:
                        print t, " IndexError: list index out of range! ", data
                        pass
                    except TypeError:
                        print t, " TypeError! ", get
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
