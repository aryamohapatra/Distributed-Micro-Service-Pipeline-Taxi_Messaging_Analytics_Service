#!/usr/bin/env python
import time
import stomp

EXIT = False
#Defining necessary dictionaries for state management
hr_bor_count = dict()
hr_zone_count = dict()
dl_bor_count = dict()
dl_zone_count= dict()

#Creating window variables for day and window
global date_window 
global hour_window
#Intializing the window details
date_window = '00-00-0000'
hour_window = ''

conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
conn.connect(login='system', passcode='manager', wait=True)

class MyListener(stomp.ConnectionListener):
    
    def publish1(self,conn, msg, destination):
        conn.send(body=msg, destination=destination)
    
    def on_error(self, headers, message):
            print('received an error "%s"' % message)
    # This method publishes Hourly and daily analysis details
    def process_hr_day_count(self,dictionary_boro,dictionary_zone,day=0):                                                         
        if day==0:
            max_boroH  = max(dictionary_boro, key=lambda k: dictionary_boro[k]) # Finding the peak Borough 
            max_zoneH  = max(dictionary_zone, key=lambda k: dictionary_zone[k]) # Finding the peak Zone 
            dayB,hrB,Max_boroH = max_boroH
            dayZ,hrZ,Max_zoneH = max_zoneH 
            details = str({'loc':{(date_window,hour_window):{'PeakBorough':Max_boroH.upper(),'peakzone':Max_zoneH.upper()}}})
            print('sending',details)
            MyListener.publish1(self,conn, details, '/queue/for-analysis')
            hr_bor_count.clear() #Flushing the details of hourly window
            hr_zone_count.clear() 
        elif day==1:
            max_boroD  = max(dictionary_boro, key=lambda k: dictionary_boro[k])
            max_zoneD  = max(dictionary_zone, key=lambda k: dictionary_zone[k])
            dayB,Max_boroD = max_boroD
            dayZ,Max_zoneD = max_zoneD   
            detailsD = str({'loc':{(date_window):{'PeakBorough':Max_boroD.upper(),'peakzone':Max_zoneD.upper()}}})
            print('sending',detailsD)
            MyListener.publish1(self,conn, detailsD, '/queue/for-daily-analysis')
            dl_bor_count.clear() #Flushing the details of daily and hourly window after a day gets over
            dl_zone_count.clear()
            hr_bor_count.clear()
            hr_zone_count.clear()
        
    def on_message(self, headers, message):
        global date_window
        global hour_window
        text = eval(message)
        try :
            if 'exit' in message: #When we get exit in the steram we publish all the details till that point
                MyListener.process_hr_day_count(self,hr_bor_count,hr_zone_count,0)
                MyListener.process_hr_day_count(self,dl_bor_count,dl_zone_count,1)
                MyListener.publish1(self,conn, str('exit-hour-loc'), '/queue/for-analysis')
                MyListener.publish1(self,conn, str('exit-day-loc'), '/queue/for-daily-analysis')
                global EXIT
                EXIT = True
 
            if isinstance(text,list) :
                if len(text) !=0  :
                    if (text[1] != hour_window and hour_window !=''): # When hour changs, publish the details and remove the data
                        MyListener.process_hr_day_count(self,hr_bor_count,hr_zone_count,0)                     
                    if text[0] != date_window and date_window != '00-00-0000': # When Day changes, publish the details and remove the data
                         MyListener.process_hr_day_count(self,dl_bor_count,dl_zone_count,1)     
                    if (text[0],text[1],text[2]) not in hr_bor_count: # Count trips of Borough in hourly window 
                        hr_bor_count[(text[0],text[1],text[2])] = 1
                    else : 
                        hr_bor_count[(text[0],text[1],text[2])]+=1
                    if (text[0],text[1],text[3]) not in hr_zone_count: # Count trips of Zone in hourly window
                        hr_zone_count[(text[0],text[1],text[3])] = 1 
                    else : 
                        hr_zone_count[(text[0],text[1],text[3])]+=1
                    if (text[0],text[2]) not in dl_bor_count:  # Count trips of Borough in daily window 
                        dl_bor_count[(text[0],text[2])] = 1
                    else : 
                        dl_bor_count[(text[0],text[2])]+=1
                    if (text[0],text[3]) not in dl_zone_count: # Count trips of Borough in hourly window 
                        dl_zone_count[(text[0],text[3])] = 1
                    else : 
                        dl_zone_count[(text[0],text[3])]+=1
                  
            if isinstance(text,list) :
                if len(text) !=0  :
                    date_window = text[0]
                    hour_window = text[1]
        except:
            print('some error')

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

