import time
from datetime import datetime
from dateutil.parser import parse
import stomp
import json
EXIT = False

class MyListener(stomp.ConnectionListener):
    
    def __init__(self):
        #Instance state variables
        self.day,self.accident,self.location = {}, {}, {} 
        self.acc_date, self.day_date, self.loc_date = "", "", ""
        self.exit_flag = False
        #Connection variables
        self.pub_conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
        self.pub_conn.connect(login='system', passcode='manager', wait=True)


    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        #Set exit flag for end of day analytics
        if 'exit-day' in message:
            self.exit_flag = True
        #Exit on publishing all records
        if (self.exit_flag) and len(self.day) == 0:
            print("Job done - Exiting")
            self.publish(self.pub_conn, str('exit-day-analytics'), '/queue/day-analytics')        
            global EXIT
            EXIT = True
        elif 'exit' in message:
            pass
        else:   
            #Construct dictionary of record from incoming string message  
            record = eval(message)
            stream_name = list(record.keys())[0]
            #Set the local state variables to record date
            if stream_name == 'accident':
                self.acc_date = list(record['accident'].keys())[0]
                self.accident[self.acc_date] = record['accident'][self.acc_date]
            if stream_name == 'day':
                self.day_date = list(record['day'].keys())[0]
                self.day[self.day_date] = record['day'][self.day_date]
            if stream_name == 'loc':
                self.loc_date = list(record['loc'].keys())[0]
                self.location[self.loc_date] = record['loc'][self.loc_date]
            #Call the output method for printing real-time data to screen  
            self.output()

        
    def output(self):
        op_day = dict.copy(self.day)
        for rec in sorted(op_day.keys()):
            rec_date = rec
            #Check if records are received from all streams for the date
            if self.check(rec_date):
                #Collate the records and print 
                print("\n*************-----------------------------------------------*************")
                print(f'Report for the Date: {rec_date}')
                print("*************-----------------------------------------------*************")
                print(f"Total Trips for this Day: {self.day[rec_date]['frequency']}")
                peak_time=self.day[rec_date]['peaktime']
                print(f"Peak Hour for the Day: {peak_time}")
                if rec_date in self.accident:
                    if rec_date in self.accident[rec_date]:
                        print(f"Accidents for the Day: {self.accident[rec_date][rec_date]}")
                    else:
                        print(f"Accidents for the Day: Nil")
                    if (rec_date, peak_time) in self.accident[rec_date]:
                        print(f"Accidents for the Peak Hour: {self.accident[rec_date][(rec_date, peak_time)]}")
                    else:
                        print(f"Accidents for the Peak Hour: Nil")
                if rec_date in self.location:
                    if 'PeakBorough' in self.location[rec_date]:
                        rec_borough = self.location[rec_date]['PeakBorough']
                        print(f"Busiest Borough of the Day: {rec_borough}")
                        if rec_date in self.accident:
                            if (rec_date, rec_borough) in self.accident[rec_date]:
                                print(f"Accidents for the Borough: {self.accident[rec_date][(rec_date, rec_borough)]}")
                            else:
                                print(f"Accidents for the Borough: Nil")
                    if 'peakzone' in self.location[rec_date]:
                        print(f"Busiest Zone of the Day: {self.location[rec_date]['peakzone']}")
                    del self.location[rec_date]
                del self.day[rec_date]
            else:
                break

    def check(self, rec_date):
        check = False
        #Check state of accident record
        if self.acc_date == '':
            check = False
        elif self.acc_date >= rec_date: 
            check = True
        #Check state of location record
        if  (check) and self.loc_date == '':
            check = False
        elif (check) and self.loc_date >= rec_date: 
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
    conn.subscribe(destination='/queue/for-daily-analysis', id=1, ack='auto')
    
    while not EXIT:
        time.sleep(0.1)
    conn.disconnect()

if __name__ == '__main__':
    main()
