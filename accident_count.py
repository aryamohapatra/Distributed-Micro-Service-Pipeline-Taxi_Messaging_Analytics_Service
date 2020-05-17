import time
from datetime import datetime
from dateutil.parser import parse
import stomp
import json
EXIT = False

class MyListener(stomp.ConnectionListener):
    
    def __init__(self):
        #Instance state variables
        self.window_date = ""
        self.window_time = ""
        self.current = {}
        self.previous = {}
        self.stats = {}
        #Connection variables
        self.pub_conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
        self.pub_conn.connect(login='system', passcode='manager', wait=True)

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        #Exit when exit instruction is received in the incoming message
        if 'exit-acc-hour' in message:
            print("Job done - Exiting")
            self.publish(self.pub_conn, str('exit-acc-stats'), '/queue/for-analysis')        
            self.publish(self.pub_conn, str('exit-acc-stats'), '/queue/for-daily-analysis') 
            global EXIT
            EXIT = True
        else: 
            #Load incoming message, reset local state variable for date and time   
            record = json.loads(message)
            rec_date = ""
            time_slot = ""

            for in_date, value in record.items():
                #set local state variable to record date
                rec_date = in_date
                for in_time_slot, value in record[in_date].items():
                    #set local state variable to record time slot
                    time_slot = in_time_slot
                    #Compare instance state variable to local state variable for change in state
                    if self.window_date == "" and self.window_time == "":
                        self.window_date = rec_date
                        self.window_time = time_slot
                    if self.window_date == rec_date and self.window_time == time_slot:
                        pass
                    elif self.window_date == rec_date and self.window_time != time_slot:
                        self.current = {'accident': {(self.window_date,self.window_time):self.stats}}
                        #Publish hourly accident analysis
                        self.publish(self.pub_conn, str(self.current), '/queue/for-analysis')
                        self.window_time = time_slot
                    else:
                        self.current = {'accident': {(self.window_date,self.window_time):self.stats}}
                        day_stats = {'accident': {self.window_date:self.stats}}
                        #Publish hourly accident analysis
                        self.publish(self.pub_conn, str(self.current), '/queue/for-analysis')
                        #Publish daily accident analysis
                        self.publish(self.pub_conn, str(day_stats), '/queue/for-daily-analysis')
                        #Copy intermediate results to another variable to retain information till next window
                        self.previous = self.current
                        self.current = {}
                        self.stats = {}
                        self.window_date = rec_date
                        self.window_time = time_slot                    
                    #Calculate and update accident statitics 
                    for in_loc, value in record[in_date][in_time_slot].items():
                        rec_loc = in_loc
                        rec_frequency = record[in_date][in_time_slot][in_loc]['FREQUENCY']

                        #Statistics for the day
                        if rec_date in self.stats:
                            self.stats[rec_date] += rec_frequency
                        else:
                            self.stats[rec_date] = rec_frequency
                        #Statistics for the day and time period
                        if (rec_date, time_slot) in self.stats:
                            self.stats[(rec_date, time_slot)] += rec_frequency
                        else:
                            self.stats[(rec_date, time_slot)] = rec_frequency
                        #Statistics for the day and location
                        if (rec_date, rec_loc) in self.stats:
                            self.stats[(rec_date, rec_loc)] += rec_frequency
                        else:
                            self.stats[(rec_date, rec_loc)] = rec_frequency
                        #Statistics for the day, time and location
                        if (rec_date, rec_loc, time_slot) in self.stats:
                            self.stats[(rec_date, rec_loc, time_slot)] += rec_frequency
                        else:
                            self.stats[(rec_date, rec_loc, time_slot)] = rec_frequency

    def publish(self, conn, msg, destination):
        conn.send(body=msg, destination=destination)

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/queue/accident-hourly', id=1, ack='auto')
    
    #Disconnect on exit message    
    while not EXIT:
        time.sleep(0.1)
    conn.disconnect()

if __name__ == '__main__':
    main()
