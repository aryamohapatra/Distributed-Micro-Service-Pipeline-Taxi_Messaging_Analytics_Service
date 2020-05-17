import stomp
import csv
import json

def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.connect(login='system', passcode='manager', wait=True)

    #Read the input stream with headers as a dictionary
    with open('Jan_2018_Motor_Vehicle_Collisions_-_Crashes_version.csv') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for record_contents in reader:
            #publish the record to the ActiveMQ accident_stream queue
            publish(conn, json.dumps(record_contents), '/queue/accident-stream')
    publish(conn, str('exit-acc-stream'), '/queue/accident-stream')
    
    #Disconnect at end of stream
    print("Job done - Exiting")
    conn.disconnect()

if __name__ == '__main__':
    main()
