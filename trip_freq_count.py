#!/usr/bin/env python
import time
import stomp

#Defining global variabes
EXIT = False
hr_count = dict()
global day_count
global date_window
global hour_window
date_window = '00-00-0000' # Defining the daily and 
hour_window = ''           # hourly window state variables 
day_count = 0
conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
conn.connect(login='system', passcode='manager', wait=True)

class MyListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
            print('received an error "%s"' % message)
    
    def publish1(self, conn, msg, destination):
        conn.send(body=msg, destination=destination)
        
    def on_message(self, headers, message):
        global date_window
        global hour_window
        global day_count
        text = eval(message)
        try :
            if 'exit' in message: # When encounter a exit in the message we are publishing all deails till that point
                a  = max(hr_count, key=lambda k: hr_count[k])
                print('So finally-->',str({(date_window,hour_window):{'frequency':hr_count,'peaktime':a}}))
                print('Finally daily publish ',str({'day':{(date_window):{'frequency':day_count,'peaktime':a}}}))
                MyListener.publish1(self,conn, str({'day':{(date_window):{'frequency':day_count,'peaktime':a}}}), '/queue/for-daily-analysis')
                MyListener.publish1(self,conn, str({'hour': {(date_window,hour_window):{'frequency':hr_count,'peaktime':a}}}), '/queue/for-analysis')          
                MyListener.publish1(self,conn, str('exit-hour'), '/queue/for-analysis')
                MyListener.publish1(self,conn, str('exit-hour'), '/queue/for-daily-analysis')
                global EXIT
                EXIT = True
            else:
                if len(text)!=0:
                    if (text[1] != hour_window and hour_window != '') : #When the hour changes publishing the hourly analysis details
                        a  = max(hr_count, key=lambda k: hr_count[k]) 
                        print('Hours Changed')
                        print('sending...All details:',str({(date_window,hour_window):{'frequency':hr_count,'peaktime':a}}))
                        MyListener.publish1(self,conn, str({'hour':{(date_window,hour_window):{'frequency':hr_count,'peaktime':a}}}), '/queue/for-analysis')
    
                    if text[0] != date_window and date_window !='00-00-0000': #When the day changes we are publishing the daily analysis details
                        pfod  = max(hr_count, key=lambda k: hr_count[k])
                        print('Total count for the day',date_window)
                        print(hr_count)
                        print('daily count-->',day_count)
                        print('peak time of the day',pfod)
                        print('Finally daily publish ',str({'day':{(date_window):{'frequency':day_count,'peaktime':pfod}}}))
                        MyListener.publish1(self,conn, str({'day':{(date_window):{'frequency':day_count,'peaktime':pfod}}}), '/queue/for-daily-analysis')
                        day_count = 0
                        hr_count.clear()      # Resetting the details by flushing the older values.
                    if (text[1]) not in hr_count: # We are maintaining a state of daily and checking the frequency of trips for each hour
                        hr_count[text[1]] = 1
                    else : 
                        hr_count[text[1]]+=1
                    day_count += 1
                if len(text)!=0:
                    date_window = text[0]  # changing the value of window variables - day and hour
                    hour_window = text[1]
        except (IndexError,ValueError,TypeError):
            print('Ignoring any error')
            print(text)

def main():

    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/queue/taxi-data', id=1, ack='auto')
    while not EXIT:
        time.sleep(0.1)

    conn.disconnect()

if __name__ == '__main__':
    main()