import json

def producer():
  print ("Producer called" )
  for i in range(20):
    yield (json.dumps( { 'k' : i }) , json.dumps( { 'v' : i + 1 } ))


# append the message to the data pointer
def consumer(message, prefix=None , dataptr=[] ):
  key = message.key().decode('utf-8')
  value = json.loads( message.value().decode('utf-8') )
  print (f"CONSUMER : {prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
  dataptr.append(value)
