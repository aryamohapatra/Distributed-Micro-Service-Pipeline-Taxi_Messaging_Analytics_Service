import time
from datetime import datetime
from dateutil.parser import parse
import stomp
import json
EXIT = False

class MyListener(stomp.ConnectionListener):
    


    def __init__(self):
        #State variables
        self.window_date = ""
        self.window_time = ""
        self.current = {}
        self.previous = {}
        #Connection variables
        self.pub_conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
        self.pub_conn.connect(login='system', passcode='manager', wait=True)

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        
        #Exit when exit instruction is received in the incoming message
        if 'exit-acc-stream' in message:
            print("Job done - Exiting")
            self.publish(self.pub_conn, str('exit-acc-hour'), '/queue/accident-hourly')        
            global EXIT
            EXIT = True
        else:
            #Load the incoming record as using json
            record = json.loads(message)
            #Parse and format the date and time
            rec_datetime = parse(record['CRASH DATE'] + " " + record['CRASH TIME'])
            rec_date = rec_datetime.date()
            time_slot = datetime.strftime(rec_datetime, '%H')
            formated_date = datetime.strftime(rec_datetime, '%m-%d-%Y')
            formated_time = datetime.strftime(rec_datetime, '%H:%M')

            #Maintain and check state
            if self.window_date == "" and self.window_time == "":
                self.window_date = rec_date
                self.window_time = time_slot
            if self.window_date == rec_date and self.window_time == time_slot:
                pass
            else:
                self.previous = self.current
                self.publish(self.pub_conn, json.dumps(self.current), '/queue/accident-hourly')
                self.current = {}
                self.window_date = rec_date
                self.window_time = time_slot

            #Preprocess and format the record
            #If location is blank mark as 'UNKNOWN"
            location = record["BOROUGH"].upper()
            if location == "":
                location="UNKNOWN"
            details = dict([("CRASH_TIME",formated_time),
                            ("LATITUDE",record["LATITUDE"]),
                            ("LONGITUDE",record["LONGITUDE"]),
                        ])
            rec_details = {record["COLLISION_ID"]:details,
                            'FREQUENCY' : 1}
            dict_loc = {location: rec_details}
            dict_time = {time_slot: dict_loc}
            if formated_date in self.current:
                if time_slot in self.current[formated_date]:
                    if location in self.current[formated_date][time_slot]:
                        self.current[formated_date][time_slot][location][record["COLLISION_ID"]] = details
                        #Carry out low-level counting as operator separtor for next operator analyzer
                        self.current[formated_date][time_slot][location]['FREQUENCY'] +=1
                    else:
                        self.current[formated_date][time_slot][location]= rec_details
                else:
                    self.current[formated_date][time_slot]= dict_loc
            else:
                self.current[formated_date] = dict_time       

    def publish(self, conn, msg, destination):
        conn.send(body=msg, destination=destination)


def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    #Publish the record to ActiveMQ accident_stream queue
    conn.subscribe(destination='/queue/accident-stream', id=1, ack='auto')
    
    #Disconnect on exit message
    while not EXIT:
        time.sleep(0.1)
    conn.disconnect()

if __name__ == '__main__':
    main()
