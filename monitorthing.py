import time
import sys
from jaque import *
import threading
import urllib2

# Queue example script that does a basic HTTP GET against several URLs at a defined interval and writes a log reporting the HTTP status code, response size, and time taken per test

# Create the queue
WorkQueue = Queue(threads=3, name='WorkQueue')

# List of URLs to check
worklist = ['http://www.cnn.com', 'http://www.akamai.net', 'http://www.ebay.com', 'http://www.stackexchange.com', 'http://www.wired.com', 'http://www.arin.net', 'http://www.paypal.com', 'http://www.reddit.com']

# Log file for monitoring output
logfile = './log.out'

# This thread will submit check messages into the queue 
class WorkSubmitter(threading.Thread):
  def __init__(self, check_interval=60):
    super(WorkSubmitter, self).__init__()
    self.check_interval = check_interval

  def run(self):
    while True:
      # create a message for each url to be checked and enqueue the message
      for payload in worklist:
        message = Message(payload, handler_func)
        WorkQueue.enqueue_message(message)
      time.sleep(self.check_interval)

# This thread will read from the processed list and write the results to disk
class ResultLogger(threading.Thread):
  def __init__(self, sleep_cycle=0.5):
    super(ResultLogger, self).__init__()
    self.sleep_cycle = sleep_cycle

  def run(self):
    while True:
      message = WorkQueue.pop_processed()
      if message is not None:
        outfile = open(logfile, 'a')
        timestamp = time.strftime('%Y%m%d%H%M%S', time.gmtime(time.time()))
        myurl = message.result['url']
        mycode = str(message.result['resp_code'])
        mysize = str(message.result['resp_size'])
        mytime = '{0:.2f}'.format(message.result['req_time'])
        myenqtime = str(message.enqueuetime)
        mydeqtime = str(message.dequeuetime)
        myfintime = str(message.finishtime)
        outline = timestamp + ',' + myurl + ',' + mycode + ',' + mysize + ',' + mytime + ',' + myenqtime + ',' + mydeqtime + ',' + myfintime + '\n'
        outfile.write(outline)
        outfile.close()
      time.sleep(self.sleep_cycle)

# Handler that will be invoked by the check messages, takes URL as input, returns a dict with url, response code, response size, and request time
def handler_func(url):
  result = {}
  result['url'] = url
  starttime = time.time()
  try:
    starttime = time.time()
    req = urllib2.urlopen(url)
    result['resp_code'] = req.getcode()
    result['resp_size'] = len(req.read())
  except:
    result['resp_code'] = sys.exc_info()[0] # shove the exception in the resp code if we had one
    result['resp_size'] = 0
  reqtime = time.time() - starttime
  result['req_time'] = reqtime
  return result # result will be available on the message object (message.result)  


if __name__ == '__main__':
  # create log file
  outfile = open(logfile, 'w')
  outfile.write('Time,URL,RespCode,RespSize,ReqTime,EnqTime,DeqTime,FinTime\n')
  outfile.close()

  # start submitter thread
  submitter = WorkSubmitter()
  submitter.start()

  # start result logger thread
  resultlogger = ResultLogger()
  resultlogger.start()
