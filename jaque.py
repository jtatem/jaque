import threading
import uuid
import time
import sys
import socket

# Jaque - JAt's QUEues asynchronous queue system

# Basic Usage -

# Create a Queue:

# MyQueue = Queue(threads=5, name='WorkQueue')

# Create a message handler that takes a message payload and returns a result, you can have many handlers:

# def myhandler(payload):
#   return do_something(payload)

# Create a message with payload and the name of the handler function that should process the payload:

# mymessage = Message(payload, myhandler)

# Get the UUID:

# myuuid = mymessage.uuid

# Enqueue the message:

# MyQueue.enqueue_message(mymessage)

# Messages are executed by worker threads in FIFO order.  Completed messages are moved to Queue.processed which can be accessed as a list or through functions to retrieve by UUID, handler, etc

# Collect the result. If a message failed, the result will contain the exception thrown:

# completedmessage = MyQueue.pop_processed_by_uuid(myuuid)
# result = completedmessage.result


# Whether to track stats.  Uses an additional thread.  Setting to False also disables EnableConsoleStats and EnableGraphite

EnableStats = True

# Used as an index of queues that have been created
QueueList = []

# Turns on a lot of verbose console output
EnableDebug = False 
EnableConsoleStats = False 

# We can send stats to Graphite on a per-queue basis
EnableGraphite = False 
GraphiteServer = 'graphite-server.example.com'
GraphitePort = 2003
GraphiteMetricStub = 'jaque.testing.' # can change this to be in alignment with your graphite naming structure


EnableConsoleStats = EnableConsoleStats and EnableStats
EnableGraphite = EnableGraphite and EnableStats

# Message object has several useful attributes to track its progress and status.  Every message gets a random UUID.  I don't know if uuid.uuid4() is
# collision safe.  Whatever.  Init requires a payload and handler.  Handler is the function that should be called to process the payload.  The payload
# will be passed to the handler function as the sole argument

# retry enables automatic retry on exceptions from the handler function.  max_attempts controls how many tries.  Only the final try (when the handler succeeds or max_attempts is reached) will be included when the message is placed in the processed queue

# discard_result controls whether the message with result are added to the processed queue after execution.  If the handler takes care of all desired work, setting this to True eliminates the need to monitor/manage the processed queue 

class Message(object):
  def __init__(self, payload, handler, retry=False, max_attempts=5, discard_result=False, delay=0):
    self.payload = payload
    self.handler = handler
    self.retry = retry
    self.max_attempts = max_attempts
    self.discard_result = discard_result
    self.delay = delay
    self.attempts = 0
    self.orig_enqueuetime = 0
    self.enqueuetime = 0
    self.dequeuetime = 0
    self.finishtime = 0
    self.successful = False
    self.isactive = False
    self.uuid = str(uuid.uuid4())
    self.result = None

  # Gets called by Queue.dequeue_message to actually run the message.  We store the result, or the exception if we fail

  def execute(self):
    self.dequeuetime = time.time()
    self.isactive = True
    self.attempts += 1
    try:
      self.result = self.handler(self.payload)
      self.successful = True
      self.finishtime = time.time()
      self.isactive = False
    except:
      e = sys.exc_info()
      self.result = e
      self.finishtime = time.time()
      self.isactive=False
      self.successful=False
    return self.result

# Queue object - 3 queues really; queued, active, processed.  Optional ability to set max thread count and "queue name" at init. Threads are all created
# at init, so changing Queue.threads after init won't change the thread count.  Queue name will be included on metrics output (debug, graphite). We also
# add ourselves to the QueueList index.

class Queue(object):
  def __init__(self, threads=10, name='UnnamedQueue'):
    self.queued = []
    self.processed = []
    self.active = []
    self.delayed = []
    self.paused = False
    self.threads = threads
    QueueList.append(self)
    self.stats = []
    self.threads = []
    self.name = name
    self.shutdown = False

    # stats attributes

    self.enqcounter = 0
    self.deqcounter = 0
    self.fincounter = 0
    self.retrycounter = 0

    self.qdepth = 0
    self.processed_qdepth = 0
    self.oldest = 0
    self.enqrate = 0
    self.deqrate = 0
    self.finrate = 0
    self.threadcount = 0

    # start stats thread, if stats enabled

    if EnableStats:
      statsthread = QueueStatsThread(self)
      statsthread.start()

    # start runner threads

    for i in range(0, threads):
      runner = QueueRunner(self, startactive=True)
      self.threads.append(runner)
      runner.start()

    # start delayed queue sweeper

    sweeper = DelaySweeper(self, interval=1)
    sweeper.start()

  def exit(self):
    self.shutdown = True

  # self explanatory

  def clear_counters(self):
    self.stats = []
    self.enqcounter = 0
    self.deqcounter = 0
    self.fincounter = 0

    self.enqrate = 0
    self.deqrate = 0
    self.finrate = 0

  # puts a message in the queue
  def enqueue_message(self, message):
    if EnableDebug:
      print('Enqueued message ' + message.uuid)
    if message.attempts == 0:
      message.orig_enqueuetime = time.time()
    if message.delay < 0:
      raise ValueError('Message delay cannot be negative')
    elif message.delay > 0:
      self.delayed.append(message)
      self.enqcounter += 1
    else:
      message.enqueuetime = time.time()    
      self.queued.append(message)
      self.enqcounter += 1

  # dequeues and executes a message
  def dequeue_message(self):
    try:
      message = self.queued.pop(0)
      self.deqcounter += 1
      self.active.append(message)
      if EnableDebug:
        print('Dequeued message ' + message.uuid)
      message.execute()
      self.active.remove(message)
      if message.successful:
        if not message.discard_result:
          self.processed.append(message)
        if EnableDebug:
          print('Successful execution of message ' + message.uuid)
      elif message.retry:
        if EnableDebug:
          print('Message ' + message.uuid + ' failed on attempt ' + str(message.attempts))
        if message.attempts < message.max_attempts:
          if EnableDebug:
            print('Message ' + message.uuid + ' re-enqueued for retry')
          self.enqueue_message(message)
          self.retrycounter += 1
        else:
          if EnableDebug:
            print('Message ' + message.uuid + ' has hit max retries and will not be attempted again')
          if not message.discard_result:
            self.processed.append(message)
      else:
        if EnableDebug:
          print('Message ' + message.uuid + ' failed and is not configured to retry')
        if not message.discard_result:
          self.processed.append(message)
      self.fincounter += 1
      if EnableDebug:
        print('Finished message ' + message.uuid)
    except IndexError: # sometimes we pass the queue length check to get a dequeue call, but the queue is already empty when we get here
      if EnableDebug: 
        print(time.asctime() + ' - Whoops, we went to dequeue but there wasn\'t anything to get')
  
  # delete a specific queued message by uuid
  def delete_queued_message(self, uuid_to_del):
    todel = [x for x in self.queued if x.uuid == uuid_to_del]
    for message in todel:
      try:
        self.queued.remove(message)
      except:
        pass

  # delete all queued messages
  def flush_queued_messages(self):
    self.queued = []

  # delete all processed messages
  def flush_processed_messages(self):
    self.processed = []

  def flush_delayed_messages(self):
    self.delayed = []

  # get a single processed message by uuid, returns None if not found, and we spit out an error if we get 2 matches cause that means uuid.uuid4() is not very collision safe
  def pop_processed_by_uuid(self, uuid_to_get):
    msglist = [x for x in self.processed if x.uuid == uuid_to_get]
    if len(msglist) == 0:
      processedmessage = None
    elif len(msglist) == 1:
      processedmessage = msglist[0]
      self.processed.remove(msglist[0])
    else:
      print('We may have had a UUID collision.  Ain\'t that something.  Details:')
      print('UUID\tEnqT\tDeqT\tFinT\tHandler\tSucceeded?')
      for msg in msglist:
        print(msg.uuid + '\t' + str(msg.enqueuetime) + '\t' + str(msg.dequeuetime) + '\t' + str(msg.finishtime) + '\t' + str(msg.handler) + '\t' + str(msg.successful))
      raise KeyError, "Multiple UUID matches found!"
      processedmessage = None 
    return processedmessage

  # get the message from the top of the processed queue, returns None if processed queue is empty
  def pop_processed(self):
    try:
      message = self.processed.pop(0)
    except:
      message = None
    return message

  # get the top n messages from the processed queue, returns an empty list if none avail, list of up to n otherwise
  def pop_processed_multi(self, count):
    result = []
    for i in range(0, count):
      try:
        result.append(self.processed.pop(0))
      except:
        pass
    return result

  # get the first processed message by handler, returns None if nothing available for that handler
  def pop_processed_by_handler(self, handler):
    for i in range(0, len(self.processed)):
      try:
        if self.processed[i].handler == handler:
          message = self.processed.pop(i)
          break
        else:
          message = None
      except:
        message = None
    return message

  # turn off queue processing
  def pause_processing(self):
    self.paused = True
    for t in self.threads:
      t.isactive = False

  # turn on queue processing
  def unpause_processing(self):
    self.paused = False
    for t in self.threads:
      t.isactive = True

# A Queue will spin up its QueueRunner threads automatically, this class shouldn't normally be invoked directly

class QueueRunner(threading.Thread):
  def __init__(self, queue, startactive=False):
    super(QueueRunner, self).__init__()
    self.queue = queue
    self.isactive = startactive
    self.runnerid = str(uuid.uuid4())

  def run(self):
    while not self.queue.shutdown:
      if self.isactive and len(self.queue.queued) > 0:
        if EnableDebug:
          print('Thread ' + self.runnerid + ' now dequeueing')
        self.queue.dequeue_message()
    if EnableDebug:
      print('Shutdown received by QueueRunner thread ' + self.runnerid)
    return 0

class DelaySweeper(threading.Thread):
  def __init__(self, queue, interval=1):
    super(DelaySweeper, self).__init__()
    self.queue = queue
    self.interval = interval

  def run(self):
    while not self.queue.shutdown:
      if len(self.queue.delayed) > 0:
        tosweep = []
        for message in self.queue.delayed:
          if message.orig_enqueuetime + message.delay <= time.time():
            tosweep.append(message)
        if len(tosweep) > 0:
          if EnableDebug:
            print('DelaySweeper found ' + str(len(tosweep)) + ' messages to move to the queue.')
          for message in tosweep:
            message.enqueuetime = time.time()
            self.queue.delayed.remove(message)
            self.queue.queued.append(message)
            if EnableDebug:
              print('DelaySweeper has swept message ' + message.uuid + ' to the queue.')
      time.sleep(self.interval)
    if EnableDebug:
      print('Shutdown received by DelaySweeper thread')
    return 0

# Every Queue gets a stats thread that keeps an eye on things.  Console and Graphite stats output handled here too.  Also starts with queue invocation and shouldn't be invoked directly
    
class QueueStatsThread(threading.Thread):
  def __init__(self, queue, maxstats=50, interval=30):
    super(QueueStatsThread, self).__init__()
    self.queue = queue
    self.maxstats = maxstats
    self.interval = interval 
    self.last_enq_count = 0
    self.last_deq_count = 0
    self.last_fin_count = 0
  def run(self):
    
    while not self.queue.shutdown:
      self.queue.qdepth = len(self.queue.queued)
      self.queue.processed_qdepth = len(self.queue.processed)
      self.queue.delayed_qdepth = len(self.queue.delayed)
      if len(self.queue.queued) > 0:
        try:
          self.queue.oldest = time.time() - self.queue.queued[0].enqueuetime
        except:
          self.queue.oldest = 0
      else:
        self.queue.oldest = 0
      self.queue.enqrate = (self.queue.enqcounter - self.last_enq_count) / self.interval
      self.queue.deqrate = (self.queue.deqcounter - self.last_deq_count) / self.interval
      self.queue.finrate = (self.queue.fincounter - self.last_fin_count) / self.interval      
      self.queue.threadcount = len(self.queue.active)

      self.last_enq_count = self.queue.enqcounter
      self.last_deq_count = self.queue.deqcounter
      self.last_fin_count = self.queue.fincounter

      curstats = {}
      curstats['qdepth'] = self.queue.qdepth
      curstats['processed_qdepth'] = self.queue.processed_qdepth
      curstats['delayed_qdepth'] = self.queue.delayed_qdepth
      curstats['oldest'] = self.queue.oldest
      curstats['enqrate'] = self.queue.enqrate
      curstats['deqrate'] = self.queue.deqrate
      curstats['finrate'] = self.queue.finrate
      curstats['threadcount'] = self.queue.threadcount
      curstats['enqcounter'] = self.queue.enqcounter
      curstats['deqcounter'] = self.queue.deqcounter
      curstats['fincounter'] = self.queue.fincounter
      curstats['retrycounter'] = self.queue.retrycounter

      # Keep up to [maxstats] historical stats things

      if len(self.queue.stats) < self.maxstats:
        self.queue.stats.append(curstats)
      else:
        self.queue.stats.pop(0)
        self.queue.stats.append(curstats)

      # Send data to graphite if we're configured to do so

      if EnableGraphite:
        curtime = str(int(time.time()))
        graphitestr = ''
        graphitestub = GraphiteMetricStub + self.queue.name + '.'

        graphitestr += graphitestub + 'qdepth ' + str(curstats['qdepth']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'processed_qdepth ' + str(curstats['processed_qdepth']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'delayed_qdepth ' + str(curstats['delayed_qdepth']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'oldest ' + str(curstats['oldest']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'enqrate ' + str(curstats['enqrate']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'deqrate ' + str(curstats['deqrate']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'finrate ' + str(curstats['finrate']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'threadcount ' + str(curstats['threadcount']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'enqcounter ' + str(curstats['enqcounter']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'deqcounter ' + str(curstats['deqcounter']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'fincounter ' + str(curstats['fincounter']) + ' ' + curtime + '\n'
        graphitestr += graphitestub + 'retrycounter ' + str(curstats['retrycounter']) + ' ' + curtime + '\n'

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((GraphiteServer, GraphitePort))
        totalsent = 0
        while totalsent < len(graphitestr):
          sent = s.send(graphitestr[totalsent:])
          if sent == 0:
            raise RuntimeError("socket connection broken")
          totalsent = totalsent + sent
        s.close()

      # Print stats on stdout if debug enabled

      if EnableDebug or EnableConsoleStats:
        outstr = time.asctime() + ' - QStats:' + self.queue.name + ':'
        outstr += str(curstats['qdepth']) + ','
        outstr += str(curstats['processed_qdepth']) + ','
        outstr += str(curstats['delayed_qdepth']) + ','
        outstr += '{0:.2f}'.format(curstats['oldest']) + ','
        outstr += str(curstats['enqrate']) + ','
        outstr += str(curstats['deqrate']) + ','
        outstr += str(curstats['finrate']) + ','
        outstr += str(curstats['threadcount']) + ','
        outstr += str(curstats['enqcounter']) + ','
        outstr += str(curstats['deqcounter']) + ','
        outstr += str(curstats['fincounter']) + ','
        outstr += str(curstats['retrycounter'])
        print(outstr)
      for i in range(0, self.interval):
        if self.queue.shutdown: 
          break
        else:
          time.sleep(1)
    if EnableDebug:
      print('Shutdown received by queue stats thread')
    return 0
