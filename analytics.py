import time
from datetime import datetime
from dateutil.parser import parse
import stomp
import json
EXIT = False

class MyListener(stomp.ConnectionListener):
    
    def __init__(self):
        #Instance state variables
        self.hour,self.accident,self.location = {}, {}, {} 
        self.acc_date, self.acc_ts, self.hour_date, self.hour_ts, self.loc_date, self.loc_ts = "", "", "", "", "", ""
        self.exit_flag = False
        #Connection variables
        self.pub_conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
        self.pub_conn.connect(login='system', passcode='manager', wait=True)

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        #Set exit flag for end of hour analytics
        if 'exit-hour' in message:
            self.exit_flag = True
        #Exit on publishing all records
        if (self.exit_flag) and len(self.hour) == 0:
            print("Job done - Exiting")
            self.publish(self.pub_conn, str('exit-analytics'), '/queue/analytics')        
            global EXIT
            EXIT = True
        elif 'exit' in message:
            pass
        else: 
            #Construct dictionary of record from incoming string message   
            record = eval(message)
            stream_name = list(record.keys())[0]
            #Set the local state variables to record date and time
            if stream_name == 'accident':
                self.acc_date = list(record['accident'].keys())[0][0]
                self.acc_ts= list(record['accident'].keys())[0][1]
                self.accident[list(record['accident'].keys())[0]] = record['accident'][(self.acc_date, self.acc_ts)]
            if stream_name == 'hour':
                self.hour_date = list(record['hour'].keys())[0][0]
                self.hour_ts = list(record['hour'].keys())[0][1]
                self.hour[list(record['hour'].keys())[0]] = record['hour'][(self.hour_date, self.hour_ts)]
            if stream_name == 'loc':
                self.loc_date = list(record['loc'].keys())[0][0]
                self.loc_ts = list(record['loc'].keys())[0][1]
                self.location[list(record['loc'].keys())[0]] = record['loc'][(self.loc_date, self.loc_ts)]
            #Call the output method for printing real-time data to screen    
            self.output()

    #Method to bring real-time analytics to screen    
    def output(self):
        op_hour = dict.copy(self.hour)
        for rec in sorted(op_hour.keys()):
            rec_date = rec[0]
            rec_ts= rec[1]
            #Check if records are received from all streams for the date and time slot
            if self.check(rec_date, rec_ts):
                #Collate the records and print 
                print("\n*************-----------------------------------------------*************")
                print(f'Report for the Date: {rec_date} & Time Slot: {int(rec_ts)} - {int(rec_ts)+1}')
                print("*************-----------------------------------------------*************")
                print(f"Total Trips for this Hour: {self.hour[(rec_date, rec_ts)]['frequency'][rec_ts]}")
                peak_time=self.hour[(rec_date, rec_ts)]['peaktime']
                print(f"Peak Hour as on this Hour: {peak_time}")
                if (rec_date, rec_ts) in self.accident:
                    if (rec_date, rec_ts) in self.accident[(rec_date, rec_ts)]:
                        print(f"Accidents for the Hour: {self.accident[(rec_date, rec_ts)][(rec_date, rec_ts)]}")
                    else:
                        print(f"Accidents for the Hour: Nil")
                    if (rec_date, peak_time) in self.accident[(rec_date, rec_ts)]:
                        print(f"Accidents for the Peak Hour: {self.accident[(rec_date, rec_ts)][(rec_date, peak_time)]}")
                    else:
                        print(f"Accidents for the Peak Hour: Nil")
                    if rec_date in self.accident[(rec_date, rec_ts)]:
                        print(f"Total Accidents as on the Hour: {self.accident[(rec_date, rec_ts)][rec_date]}")
                    else:
                        print(f"Total Accidents as on the Hour: Nil" )
                if (rec_date, rec_ts) in self.location:
                    if 'PeakBorough' in self.location[(rec_date, rec_ts)]:
                        rec_borough = self.location[(rec_date, rec_ts)]['PeakBorough']
                        print(f"Busiest Borough of the Hour: {rec_borough}")
                        if (rec_date, rec_ts) in self.accident:
                            if (rec_date, rec_borough) in self.accident[(rec_date, rec_ts)]:
                                print(f"Accidents for the Borough: {self.accident[(rec_date, rec_ts)][(rec_date, rec_borough)]}")
                            else:
                                print(f"Accidents for the Borough: Nil")
                    if 'peakzone' in self.location[(rec_date, rec_ts)]:
                        print(f"Busiest Zone of the Hour: {self.location[(rec_date, rec_ts)]['peakzone']}")
                del self.hour[(rec_date, rec_ts)]
            else:
                break

    def check(self, rec_date, rec_ts):
        check = False

        #Check state of accident record
        if self.acc_date == '':
            check = False
        elif self.acc_date > rec_date: 
            check = True
        elif self.acc_date == rec_date and self.acc_ts >= rec_ts: 
            check = True
        #Check state of location record
        if  (check) and self.loc_date == '':
            check = False
        elif (check) and self.loc_date > rec_date: 
            check = True
        elif (check) and self.loc_date == rec_date and self.loc_ts >= rec_ts: 
            check = True
        else:
            check = False
        
        return check

    def publish(self, conn, msg, destination):
        conn.send(body=msg, destination=destination)

def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/queue/for-analysis', id=1, ack='auto')
    
    #Disconnect on exit message
    while not EXIT:
        time.sleep(0.1)
    conn.disconnect()

if __name__ == '__main__':
    main()
