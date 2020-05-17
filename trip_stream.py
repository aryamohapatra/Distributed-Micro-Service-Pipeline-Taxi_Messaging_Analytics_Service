#!/usr/bin/env python
import stomp

def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.connect(login='system', passcode='manager', wait=True)
    filename = 'yellow_tripdata_2018-01_sorted.csv'
        
    file = open(filename, encoding="utf8") # Reading the file to publish to the ActiveMQ
    for line in file:
        publish(conn, line, '/queue/taxi-stream')
    publish(conn, str('exit'), '/queue/taxi-stream')
    conn.disconnect()

if __name__ == '__main__':
    main()
