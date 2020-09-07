from classes import *

# Check EXCHANGE is Open ======================================================
if not exchange_is_open():
    exit(200)
    pass
# =============================================================================


print datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),' ',  #2017-10-14 13:05:00
_startTime = time.time()


with Loader() as l:
    l.Load_M5_data()


print "Running time: "+str(int(time.time() - _startTime))+" s."
