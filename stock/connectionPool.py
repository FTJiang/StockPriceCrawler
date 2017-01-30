import MySQLdb
import time
import string
import threading
from threading import Thread
#import redis
from Queue import Queue
import sys
import setting
 
#cond = threading.Condition()
def signal_handler(signum, frame): 
    setting.is_exit = True 
    print "set is_exit"
    sys.exit(0)

 #connection pool
class PooledConnection:
    def __init__(self, maxconnections, connstr,dbtype):
        self._pool = Queue(maxconnections) # create the queue for connection
        self.connstr = connstr #parameters for creating connection
        self.dbtype=dbtype  #type of database
        self.maxconnections=maxconnections  #set the maximum connections
        self.queue = Queue()     #queue used for storing connections

        #create connections
        try:
            for i in range(maxconnections):
                self.fillConnection(self.CreateConnection(connstr,dbtype))
        except Exception,e:
            raise Exception(e)

    #put connection into queue
    def fillConnection(self,conn):
        try:
            self._pool.put(conn)
 
        except Exception,e:
            raise Exception("fillConnection error:"+str(e))
    #return a connection
    def getConnection(self):
        try:
            return self._pool.get()
        except Exception,e:
            raise Exception("getConnection error:"+str(e))
 
    #close a connection
    def CloseConnection(self,conn):
        try:
            self._pool.get().close()
            self.fillConnection(self.CreateConnection(self.connstr,self.dbtype))
            #print "current connection pool : "+str(self._pool.qsize())
        except Exception,e:
            raise Exception("CloseConnection error:"+str(e))
 
    #create and return a connection
    def CreateConnection(self,connstr,dbtype):
        if dbtype=='xxx':
            pass
        elif dbtype=='mysql':
            try:
                db_conn = connstr.split("#");
                #conndb=MySQLdb.connect(db=conf.mydb,host=conf.dbip,user=conf.myuser,passwd=conf.mypasswd);
                conndb=MySQLdb.connect(user=db_conn[0],passwd=db_conn[1],host=db_conn[2],db=db_conn[3]);
                #conndb.clientinfo = 'datasync connection pool from datasync.py';
                #conndb.ping();
                return conndb
            except Exception, e:
                raise Exception('conn targetdb datasource Excepts,%s!!!(%s).'%(db_conn[2],str(e)))
                return None

#manage the threadpool
class MysqlEngine():
    def __init__(self,pool,buffSize,sql,threadsNum,values,event):
        self.storeNum = 0       #record number of tuples inserted into database
        self.storeLock = threading.Lock()   #lock for changing storeNum
        self.writeLock = threading.Lock()   #lock for inserting tuple
        self.buffSize = buffSize    #insert buffSize tuples into DB once
        self.sql = sql      #query statement
        self.threadsNum = threadsNum    #threads used to insert tuples
        self.threadPool = []        #used to store the name of threads
        self.values = values        #tuple or list of tuples for insertion
        self.queue = Queue()        #task pool used for storing tuples obtained from values
        #self.cond = cond
        self.event = event
        self.pool = pool   #connection pool
        #self.ThreadUsing = 0
        #self.stop = False
        self.initThreads()          
        self.initTaskQueue()

    def getThreadPool():
        return threadPool
         
    def addStoreNUm(self,num):
        self.storeLock.acquire()
        self.storeNum+=1
        self.storeLock.release()

    def initThreads(self):
        for i in range(self.threadsNum):
            self.threadPool.append(MySQLSavor(self,i,self.event))
        print self.threadPool

    def threadsStart(self):
        #threads will end when calling thread end
        for thread in self.threadPool:
            thread.setDaemon(True)
        for thread in self.threadPool:
            thread.start()

    #add values to task pool and start all the threads
    def initTaskQueue(self):
        self.addTasks(self.values)
        print str(self.queue.qsize())

    #store all the values into task pool
    def addTasks(self,values):
        #if cond.acquire():
        if values:
            if isinstance(values,tuple):
                    self.queue.put(values)
                    self.addStoreNUm(1)
            elif isinstance(values,list):
                for value in values:
                    if isinstance(value,dict):
                        self.queue.put(value)
                        self.addStoreNUm(1)
            #print str(self.queue.qsize())
            #cond.notifyAll()
            #cond.release()      

    def getStoreNum(self):
        return self.storeNum

    def getTask(self):
        return self.queue.get(True,1)
    
#thread class for inserting tuples into DB
class MySQLSavor(Thread):
    def __init__(self, engine,threads,event):
        Thread.__init__(self)
        self.engine = engine    #thread manager
        self.stop = False
        self.threadID = threads     #threadID
        #self.cond = cond     
        self.event = event      

    def run(self):
        #print "Thread "+str(self.threadID)+" start working"
        buff = []
        #get connection
        try:
            db = self.engine.pool.getConnection()
            #print "get a connection"+str(db)
            cur = db.cursor()
        except Exception as e:
            print e
        while True:
            value = None
            try:
                value = self.engine.getTask() #block = True, timeout 1s, will delete elements after getting
            except Exception as e:
                #print "error"
                if self.stop and self.engine.queue.empty():
                    if buff:
                        self._save(buff,db,cur)
                    break
            if value:
                buff.append(value)
                if len(buff)>=self.engine.buffSize:
                    self._save(buff,db,cur)
                    #self.engine.threadPool.pop(MySQLSavor.threads)
            else:
                if self.engine.queue.empty():
                    if buff:
                        self._save(buff,db,cur)
                    #print "Thread "+str(self.threadID)+" is suspended"
                    self.event.wait()
                    #print "value of is_exit : " + str(setting.is_exit)
                    if setting.is_exit:
                        #print "Thread "+str(self.threadID)+" finish working"
                        break
                   # print "Thread "+str(self.threadID)+" restart working"
                    continue
            time.sleep(0)
                    

    def stop(self):
        self.stop = True;

    #insert the tuples in buffer into DB
    def _save(self,buff,db,cur):
        try:
            self.engine.writeLock.acquire()  #acquire for lock
            print buff
            print "This is thread "+str(self.threadID)+" writing"
            try:
                cur.executemany(self.engine.sql,buff)
                db.commit()
            except Exception as e:
                print e
            self.engine.writeLock.release()
            self.engine.addStoreNUm(len(buff))
        finally:
            buff[:] = []


