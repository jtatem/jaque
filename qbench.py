import time
import sys
from jaque import *
import threading
import urllib2
import random
import argparse

SleeperQueue = Queue(threads=100, name='SleeperQueue')

parser = argparse.ArgumentParser(description='Simple load generator / benchmark tool for Jaque')

parser.add_argument('-f', type=str, default='./rand.txt', help='Input file of message durations, default ./rand.txt')
parser.add_argument('-n', type=int, default=5000, help='Number of messages')

args = parser.parse_args()
filename = args.f
count = args.n

def handler_func(interval):
  time.sleep(interval)
  return 0

if __name__ == '__main__':
  print('Starting benchmark with ' + str(count) + ' sleepers using ' + filename + ' as input')
  start = time.time()
  f = open(filename, 'r')
  i = 0
  while i <= count:
    line = f.readline()
    n = float(line.rstrip())
    message = Message(n, handler_func, discard_result=True)
    SleeperQueue.enqueue_message(message)
    i += 1
    if i % 1000 == 0:
      print(str(i) + ' messages enqueued')
  end = time.time()
  print('Work enqueue completed in ' + str(end - start) + ' seconds')
  f.close()

  while len(SleeperQueue.queued) > 0:
    print(str(len(SleeperQueue.queued)) + ' queued entries remaining')
    time.sleep(1)

  print('Benchmark completed after ' + str(time.time() - start) + ' seconds')

  print('Shutting down queue threads')
  SleeperQueue.shutdown = True
  tc = threading.active_count()
  while tc > 1:
    tc = threading.active_count()
    print(str(tc) + ' threads remaining')
    time.sleep(1)
