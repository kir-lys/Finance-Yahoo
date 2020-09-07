from classes import *

# Check EXCHANGE is Open ======================================================
if not exchange_is_open():
    #exit(200)
    pass
# =============================================================================

print datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),' '  #2017-10-14 13:05:00
_startTime = time.time()


with Loader() as l:
    print "Loading M5:"
    l.Load_M5_data(last_bar='1d')
    print "Loading M30:"
    l.Load_M30_data(last_bar='1d')


print "Running time: "+str(int(time.time() - _startTime))+" s."
