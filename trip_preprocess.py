#!/usr/bin/env python
import time
import stomp
import csv
from dateutil.parser import parse
#Global variables
EXIT = False
places = dict()
details = []
d = {}

# We are using knowledge repository location to lookup and add location details
with open('taxi+_zone_lookup.csv') as csvfile:    
        reader = csv.DictReader(csvfile, delimiter=',')
        for record_contents in reader:
            d[record_contents['LocationID']] = (record_contents['Borough'],record_contents['Zone'])

# Create Connection
conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
conn.connect(login='system', passcode='manager', wait=True)

class MyListener(stomp.ConnectionListener):
        
    def publish1(self,conn, msg, destination):
        conn.send(body=msg, destination=destination)

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        global details
        if 'exit' in message:
            MyListener.publish1(self,conn, str('exit'), '/queue/taxi-data')
            global EXIT
            EXIT = True
        else:
            try:
                msg = message.replace('\n',"").split(",") # Splitting the incoming data 
                date = parse(msg[1]).date()               # Parse message into datetime format to get date and time
                time = parse(msg[1]).time()
                time_sub = time.strftime('%H:%M')[:2]     # Format date and time
                date_sub = date.strftime('%d-%m-%Y')
                location_id = msg[7]                      # Getting the pickup location from the input string 
                locname,zonename = d[location_id]         # Adding the Borough and Zone details using the lookup table
                details=[date_sub,time_sub,locname,zonename]
    
            except (IndexError,ValueError):
                print('Passing unwanted data')
                print(message)
   
            MyListener.publish1(self,conn, str(details), '/queue/taxi-data')

def main():
    global prev_when
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/queue/taxi-stream', id=1, ack='auto')
    while not EXIT:
        time.sleep(0.1)

    conn.disconnect()

if __name__ == '__main__':
    main()
