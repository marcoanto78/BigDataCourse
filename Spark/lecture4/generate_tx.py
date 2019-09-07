import random
import time
cnt=100000
#for x in range(cnt):
#  f=random.randint(1,100)
#  qty=random.randint(1,10)
#  amt=random.randint(1,100)
#  print str(x) + "," + str(f) + "," + str(qty) + "," + str(amt) + ",2019-02-02 12:12:12"

import socket
import threading

bind_ip = '0.0.0.0'
bind_port = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((bind_ip, bind_port))
server.listen(15)  # max backlog of connections

#print 'Listening on {}:{}'.format(bind_ip, bind_port)


def handle_client_connection(client_socket):
  x=1
  while True:
    f=random.randint(1,100)
    qty=random.randint(1,10)
    amt=random.randint(1,100)
    data=str(x) + "," + str(f) + "," + str(qty) + "," + str(amt) + ",2019-02-02 12:12:12\n"
    client_socket.send(data)
    x+=1
    time.sleep(1)

while True:
    client_sock, address = server.accept()
    print 'Accepted connection from {}:{}'.format(address[0], address[1])
    client_handler = threading.Thread(
        target=handle_client_connection,
        args=(client_sock,)  # without comma you'd get a... TypeError: handle_client_connection() argument after * must be a sequence, not _socketobject
    )
    client_handler.start()
