import time
import sys
from jaque import *
import threading
import urllib2

curdata = {}
histdata = []

# Queue example script that does a basic HTTP GET against several URLs at a defined interval and writes a log reporting the HTTP status code, response size, and time taken per test

# Create the queue
WorkQueue = Queue(threads=5, name='WorkQueue')

# List of URLs to check
worklist = ['http://www.cnn.com', 'http://www.akamai.net', 'http://www.ebay.com', 'http://www.stackexchange.com', 'http://www.wired.com', 'http://www.arin.net', 'http://www.paypal.com', 'http://www.reddit.com', 'http://www.icann.org', 'http://imgur.com']

# Log file for monitoring output
logfile = './log.out'
shutdown = False

# This thread will read from the processed list and write the results to disk, also re-enqueues message for next execution
class ResultLogger(threading.Thread):
  def __init__(self, sleep_cycle=0.05):
    super(ResultLogger, self).__init__()
    self.sleep_cycle = sleep_cycle

  def run(self):
    while not shutdown:
      message = WorkQueue.pop_processed()
      outfile = open(logfile, 'a')
      if message is not None: 
        timestamp = time.strftime('%Y%m%d%H%M%S', time.gmtime(message.finishtime)) + '{0:.3f}'.format(message.finishtime % 1)[1:]
        myurl = message.payload
        if type(message.result) is dict:
          mycode = str(message.result['resp_code'])
          mysize = str(message.result['resp_size'])
          mytime = '{0:.3f}'.format(message.result['req_time'])
        else:
          mycode = str(message.result[1])
          mysize = 'n/a'
          mytime = 'n/a'
        myenqtime = str(int(message.enqueuetime)) + '{0:.3f}'.format(message.enqueuetime % 1)[1:]
        mydeqtime = str(int(message.dequeuetime)) + '{0:.3f}'.format(message.dequeuetime % 1)[1:]
        myfintime = str(int(message.finishtime)) + '{0:.3f}'.format(message.finishtime % 1)[1:]
        myattempts = str(message.attempts)
        outline = timestamp + ',' + myurl + ',' + mycode + ',' + mysize + ',' + mytime + ',' + myenqtime + ',' + mydeqtime + ',' + myfintime + ',' + myattempts + '\n'
        outfile.write(outline)
        outfile.close()
        newmessage = Message(message.payload, handler_func, retry=True, delay=30)
        WorkQueue.enqueue_message(newmessage)
      time.sleep(self.sleep_cycle)
    return 0

class DataLogger(threading.Thread):
  def __init__(self, max_keep=20, interval=30, stale=300):
    super(DataLogger, self).__init__()
    self.max_keep = max_keep
    self.interval = interval
    self.stale = 30

  def run(self):
    while not shutdown:
      if len(histdata) >= self.max_keep:
        histdata.pop(0)
      if len(curdata) > 0:
        histdata.append(dict(curdata))
      if len(histdata) > 0:
        print('URL')
        for url in curdata:
          outstr = url + '  '
          for i in range(len(histdata) -1, -1, -1):
            datatime = histdata[i][url][0]
            result = histdata[i][url][1]
            if result['resp_size'] > 0:
              outstr += 'UP'
            else:
              outstr += 'DN'
            if datatime - time.time() > self.stale:
              outstr += 's'
            outstr += '  '
          print(outstr)
      for i in range(0, self.interval):
        if shutdown:
          break
        else: 
          time.sleep(1)
    return 0

# Handler that will be invoked by the check messages, takes URL as input, returns a dict with url, response code, response size, and request time
def handler_func(url):
  result = {}
  result['url'] = url
  starttime = time.time()
  try:
    req = urllib2.urlopen(url, timeout=10)
    result['resp_code'] = req.getcode()
    result['resp_size'] = len(req.read())
    reqtime = time.time() - starttime
    result['req_time'] = reqtime
    curdata[url] = (time.time(), result)
  except:
    e = sys.exc_info() 
    result['resp_code'] = e
    result['resp_size'] = 0
    reqtime = time.time() - starttime
    result['req_time'] = reqtime
    curdata[url] = (time.time(), result)
    raise
  return result # result will be available on the message object (message.result)  


if __name__ == '__main__':
  # create log file
  outfile = open(logfile, 'w')
  outfile.write('Time,URL,RespCode,RespSize,ReqTime,EnqTime,DeqTime,FinTime,Attempts\n')
  outfile.close()

  for payload in worklist:
    message = Message(payload, handler_func, retry=True)
    WorkQueue.enqueue_message(message)


  # start result logger thread
  resultlogger = ResultLogger()
  resultlogger.start()

  datalogger = DataLogger()
  datalogger.start()

  while not shutdown:
    try:
      time.sleep(1)
    except KeyboardInterrupt:
      WorkQueue.shutdown = True
      shutdown = True
      print('Waiting for background threads to exit, this may take up to 30 seconds')
      sys.stdout.write('Threads Remaining: ')
      sys.stdout.flush()
      while threading.active_count() > 1:
        sys.stdout.write(str(threading.active_count()) + ' ')
        sys.stdout.flush()
        time.sleep(1)
      print('\nShutdown completed') 
