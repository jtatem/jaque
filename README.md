# jaque
A simple async queue framework in Python.  Yeah there's others out there, even stuff in the standard libs.  I wrote this mostly as a learning experience, but I like it.

Basic Usage -

Create a Queue:

  MyQueue = Queue(threads=5, name='WorkQueue')

Create a message handler that takes a message payload and returns a result, you can have many handlers:

  def myhandler(payload):
  
    return do_something(payload)

Create a message with payload and the name of the handler function that should process the payload:

  mymessage = Message(payload, myhandler)

Get the UUID:

  myuuid = mymessage.uuid

Enqueue the message:

  MyQueue.enqueue_message(mymessage)

Messages are executed by worker threads in FIFO order.  Completed messages are moved to Queue.processed which can be accessed as a list or through functions to retrieve by UUID, handler, etc

Collect the result. If a message failed, the result will contain the exception thrown:

  completedmessage = MyQueue.pop_processed_by_uuid(myuuid)
  
  result = completedmessage.result
