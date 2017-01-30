import urllib2
from bs4 import BeautifulSoup
import MySQLdb as mdb
import sys
import json
import urlparse
import csv
import proxy
from threading import Thread
from itertools import repeat
from connectionPool import PooledConnection,MySQLSavor,MysqlEngine
import time
import threading
import signal
import setting
import multiprocessing
import Queue

def change_proxy():
    myProxy = proxy.getProxy()
    return myProxy

def getData(url,symb):
	succeed = False
	needProxy = False
	#keep tying until succeed get data or exceed 3 times
	time = 0
	while succeed == False and time < 3:
		try:
			#check whether proxy is needed
			time += 1
			if needProxy == False:
				req = urllib2.Request(url,headers=hdr)
			else:
				req = urllib2.Request(url,headers=hdr, proxies = change_proxy())
			htmltext = urllib2.urlopen(req)
			#soup = BeautifulSoup(htmltext,'html.parser')
			data = json.load(htmltext) #json.load can deal with xhr type response
			return data[0],symb
		except urllib2.HTTPError,e:
			if e.code == 403:
				needProxy = True
			else:
				continue
	if time >= 3:
		return {},symb

def addDataToQueue(data,engine,symb):
	values = []
	first_ele = data["price"]
	for f in first_ele:
		#add symbol pair
		f.update({'symbol':symb})
		values.append(f)
	succeed = True
	engine.addTasks(values)


def signal_handler(signum, frame): 
    global is_exit
    is_exit = True 
    print "set is_exit"
    sys.exit(0)

def initDBManager():
	event = threading.Event() 
	#set signal for interrupt
	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)
	sql = "insert into stock (symbol,dateTime,value) VALUES (%(symbol)s,%(dateTime)s,%(value)s)"
	connstring="root#Jiangft1213#localhost#stock";
	value = []
	#initialize mysqlengine, connection pool has 10 connection, threads insert 50 tuples each time, max 3 threads for insertion
	engine = MysqlEngine(PooledConnection(10,connstring,"mysql"),50,sql,3,value,event)
	return engine,event

#target function of thread to get data
def pool_th(processPool,q,file):
	try:
		reader = csv.DictReader(file)
		cnt = 0 
		for row in reader:
			cnt+=1
			symbol = row['Symbol']
			newurl = urlparse.urljoin(base_url,symbol)
			newurl +='%3AUS?timeFrame=1_DAY'
			print newurl
			#T = Thread(target = getData,args = (newurl,symbol,engine))
			#T.start()
			q.put(processPool.apply_async(getData,(newurl,symbol)))
			if cnt == 5:
				break
	except Exception as e:
		print e
	finally:
		file.close()

#target function of thread to add received data to engine queue
def result_th(q,engine,event):
	while 1:
		a,symb = q.get().get()
		print a
		if a:
			addDataToQueue(a,engine,symb)
			#inform all the threads there is new data needed to be inserted into DB
			event.set()


def stopThreads():
	setting.is_exit = True

	#price_box = soup.find('div',attrs={'id':'qwidget_lastsale'})
	#print "Price of "+symb +" is : "+str(price_box.text.strip())
if __name__=="__main__":
	hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
	base_url = 'https://www.bloomberg.com/markets/api/bulk-time-series/price/'
	engine,event = initDBManager()
	threadlist = []
	setting.init()
	#read in symbol list
	f = open('companylist.csv','rb')
	results = []
	q = Queue.Queue()
	cpus = multiprocessing.cpu_count()
	print "number of cpus : "+str(cpus)
	#use processes pool, size of pool equal to number of cpus
	processPool = multiprocessing.Pool(processes = cpus)
	#use multi-threads to realize getting results while multi-processing
	t1=threading.Thread(target=pool_th,args=(processPool,q,f))
	t2=threading.Thread(target=result_th,args=(q,engine,event))
	#end t2 when main thread exit
	engine.threadsStart()
	t2.setDaemon(True)
	t1.start()
	t2.start()
	#start insertion threads
	
	t1.join()
	processPool.close()
	processPool.join()
	#prepare for stop program
	stopThreads()
	event.set()
	alive = True
	#wait untile all the threads exit
	while alive:
		alive = False
		for thread in engine.threadPool:
			alive = alive or thread.isAlive()
		if not alive and not t2.isAlive():
			break
	print "total inserted tuples : "+str(engine.getStoreNum())

