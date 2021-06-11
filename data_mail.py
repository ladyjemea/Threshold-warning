import pandas as pd
import numpy as np
import subprocess
import time
import datetime as dtm
from datetime import timezone, datetime
import json
import os
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

crt_yr = dtm.datetime.utcnow().year
crt_month = dtm.datetime.utcnow().month
crt_day =dtm.datetime.utcnow().day
crt_time = dtm.datetime.utcnow().replace(microsecond=0)
crt_hour = crt_time.hour
crt_minute = crt_time.minute
crt_date = dtm.datetime(year=crt_yr, month=crt_month, day=crt_day, hour=crt_hour, minute=crt_minute)
end_range = crt_time

date = dtm.datetime.utcnow()

datestring_yr = dtm.datetime.strftime(date, '%Y')
datestring_mo= dtm.datetime.strftime(date, '%m')
datestring_da = dtm.datetime.strftime(date, '%d')



'''
#Tromsø
url_Tro = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=tro2a&year="+datestring_yr+"&month="+datestring_mo+"&day="+datestring_da+"&res=10sec&pwd=&format=html&comps=DHZ&getdata=+Get+data+"

dfTro = pd.read_csv(url_Tro, skiprows = 6)
dfTro = dfTro.astype(str)
dfTro = dfTro[dfTro.columns[0]].str.split(expand=True)

dfTro.columns = ['Date', 'Time', 'Dec', 'Tromso', 'Vert', 'Incl', 'Tot']
dfTro['timestamp'] = dfTro['Date'] + ' ' + dfTro['Time']
dfTro.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfTro = dfTro[dfTro.Tromso != '99999.9']
dfTro['timestamp'] = pd.to_datetime(dfTro['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfTro.set_index('timestamp', inplace=True)

dfTro.to_csv('data/hcom/tromsø.csv', index= True, sep= " ")




#Dombås
url_Dob = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=dob1a&year="+datestring_yr+"&month="+datestring_mo+"&day="+datestring_da+"&res=10sec&pwd=&format=html&comps=DHZ&getdata=+Get+data+"

dfDob = pd.read_csv(url_Dob, skiprows = 6)
dfDob = dfDob.astype(str)
dfDob = dfDob[dfDob.columns[0]].str.split(expand=True)

dfDob.columns = ['Date', 'Time', 'Dec', 'Dombas', 'Vert', 'Incl', 'Tot']
dfDob['timestamp'] = dfDob['Date'] + ' ' + dfDob['Time']
dfDob.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfDob = dfDob[dfDob.Dombas != '99999.9']
dfDob['timestamp'] = pd.to_datetime(dfDob['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfDob.set_index('timestamp', inplace=True)

dfDob.to_csv('data/hcom/dombås.csv', index= True, sep= " ")




#Svalbard
url_Sva = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=lyr2a&year="+datestring_yr+"&month="+datestring_mo+"&day="+datestring_da+"&res=10sec&pwd=&format=html&comps=DHZ&getdata=+Get+data+"

dfSva = pd.read_csv(url_Sva, skiprows = 6)
dfSva = dfSva.astype(str)
dfSva = dfSva[dfSva.columns[0]].str.split(expand=True)

dfSva.columns = ['Date', 'Time', 'Dec', 'Svalbard', 'Vert', 'Incl', 'Tot']
dfSva['timestamp'] = dfSva['Date'] + ' ' + dfSva['Time']
dfSva.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfSva = dfSva[dfSva.Svalbard != '99999.9']
dfSva['timestamp'] = pd.to_datetime(dfSva['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfSva.set_index('timestamp', inplace=True)

dfSva.to_csv('data/hcom/svalbard.csv', index= True, sep= " ")
'''



#dfTro = pd.read_csv("data/Hcom/tromsø.csv", sep=" ")
dfTro = pd.read_csv("../Combined_data/data/threshold_mag_tro.csv", sep=" ")
dfTro = dfTro.drop_duplicates(subset=['timestamp', 'Horiz_tro'])
dfTro.reset_index(drop=True, inplace=True)
dfTro['timestamp'] = pd.to_datetime(dfTro['timestamp'])
dfTro.set_index('timestamp', inplace=True)

#dfDob = pd.read_csv("data/Hcom/dombås.csv", sep=" ")
dfDob = pd.read_csv("../Combined_data/data/threshold_mag_dob.csv", sep=" ")
dfDob = dfDob.drop_duplicates(subset=['timestamp', 'Horiz_dob'])
dfDob.reset_index(drop=True, inplace=True)
dfDob['timestamp'] = pd.to_datetime(dfDob['timestamp'])
dfDob.set_index('timestamp', inplace=True)

#dfSva = pd.read_csv("data/Hcom/svalbard.csv", sep=" ")
dfSva = pd.read_csv("../Combined_data/data/threshold_mag_nal.csv", sep=" ")
dfSva = dfSva.drop_duplicates(subset=['timestamp', 'Horiz_nal'])
dfSva.reset_index(drop=True, inplace=True)
dfSva['timestamp'] = pd.to_datetime(dfSva['timestamp'])
dfSva.set_index('timestamp', inplace=True)

dfTro.columns = ['Tromsø']
dfDob.columns = ['Dombås']
dfSva.columns = ['Svalbard']




#deltaH
df_ai_tro = dfTro.resample('H')['Tromsø'].agg(['max', 'min'])
df_ai_tro['diff'] = abs(df_ai_tro['max'].sub(df_ai_tro['min'], axis = 0))
df_ai_tro['diff'] = df_ai_tro['diff'].shift(1)
df_ai_tro.drop(['max', 'min'], axis=1, inplace=True)
df_ai_tro.columns = ['Tromso']
df_ai_tro = df_ai_tro.tail(1)
df_ai_tro["datetime"] = crt_date
df_ai_tro['datetime'] = df_ai_tro['datetime'].astype(str)
df_ai_tro[['Date','Time']] = df_ai_tro.datetime.str.split(expand=True)


df_ai_dob = dfDob.resample('H')['Dombås'].agg(['max', 'min'])
df_ai_dob['diff'] = abs(df_ai_dob['max'].sub(df_ai_dob['min'], axis = 0))
df_ai_dob['diff'] = df_ai_dob['diff'].shift(1)
df_ai_dob.drop(['max', 'min'], axis=1, inplace=True)
df_ai_dob.columns = ['Dombas']
df_ai_dob = df_ai_dob.tail(1)
df_ai_dob["datetime"] = crt_date
df_ai_dob['datetime'] = df_ai_dob['datetime'].astype(str)
df_ai_dob[['Date','Time']] = df_ai_dob.datetime.str.split(expand=True)


df_ai_sva = dfSva.resample('H')['Svalbard'].agg(['max', 'min'])
df_ai_sva['diff'] = abs(df_ai_sva['max'].sub(df_ai_sva['min'], axis = 0))
df_ai_sva['diff'] = df_ai_sva['diff'].shift(1)
df_ai_sva.drop(['max', 'min'], axis=1, inplace=True)
df_ai_sva.columns = ['Svalbard']
df_ai_sva = df_ai_sva.tail(1)
df_ai_sva["datetime"] = crt_date
df_ai_sva['datetime'] = df_ai_sva['datetime'].astype(str)
df_ai_sva[['Date','Time']] = df_ai_sva.datetime.str.split(expand=True)
print(df_ai_sva)




#Mail function deltaH
def send_mail():
    sender_email = ""
    receiver_email = []
    password = ""

    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message["Bcc"] = ""  # Recommended for mass emails


    text = """\
    
    Message below the line:
    -------------------------------------------------

    GEOMAGNETIC ACTIVITY WARNING
    This is an automatically generated message.

    The threshold for extreme geomagnetic activity has been reached in the past hour.
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT 
    in """ + str(station) + """ at """ + str(val_datetime) + """.

    To follow up the event, see
    https://site.uit.no/spaceweather/data-and-products/real-time-geomagnetic-disturbances/

    and for more information on the thresholds,
    see https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/summary-and-forecast/

    For further support, contact person@gmail.com (available during normal working hours).

    NOSWE - The Norwegian Centre for Space Weather

    ---------------------------------------------------"""



    msg = MIMEText(text, "plain")
    message.attach(msg)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())




#Tromsø
tro_deltaH = df_ai_tro.iloc[0][0]
tro_deltaH = round(tro_deltaH, 1)

if (tro_deltaH >= 2000):
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Extreme'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            if tro_deltaH_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > tro_deltaH_2.iloc[0][1]:
                    if tro_deltaH > tro_deltaH_2.iloc[0][0]:
                        #send_mail()
                        data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        tro_deltaH_2 = pd.DataFrame(data)
                        tro_deltaH_2.to_csv('data/compare/tro_deltaH.csv', index = False, sep= " ")
            else:
                #send_mail()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH.csv', index = False, sep= " ") 
    g.close()




'''
Station = """ + str(station) + """
    Property = """ + str(prop) + """
    Threshold value = """ + str(threshold_value) + """
    Description = """ + str(name) + """
    Date = """ + str(val_date) + """
    Time = """ + str(val_time) + """
'''



#Dombås
dob_deltaH = df_ai_dob.iloc[0][0]
dob_deltaH = round(dob_deltaH, 1)

if (dob_deltaH >= 750):
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Extreme'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            if dob_deltaH_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > dob_deltaH_2.iloc[0][1]:
                    if dob_deltaH > dob_deltaH_2.iloc[0][0]:
                        #send_mail()
                        data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        dob_deltaH_2 = pd.DataFrame(data)
                        dob_deltaH_2.to_csv('data/compare/dob_deltaH.csv', index = False, sep= " ")
            else:
                #send_mail()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH.csv', index = False, sep= " ") 
    g.close()





#Svalbard
sva_deltaH = df_ai_sva.iloc[0][0]
sva_deltaH = round(sva_deltaH, 1)

if (sva_deltaH >= 2000):
    prop = 'Hourly Activity Index'
    threshold_value = sva_deltaH
    station = df_ai_sva.columns[0]
    name = 'Extreme'

    sva_index = df_ai_sva.index[0]
    val_datetime = df_ai_sva.iloc[0][1]
    val_date = df_ai_sva.iloc[0][2]
    val_time = df_ai_sva.iloc[0][3]


    with open('data/compare/sva_deltaH.csv', 'a')as g:
        if os.path.getsize('data/compare/sva_deltaH.csv') <= 0:
            df_ai_sva.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail()
        else:
            sva_deltaH_2 = pd.read_csv('data/compare/sva_deltaH.csv', sep = " ")
            sva_deltaH_2['timestamp'] = pd.to_datetime(sva_deltaH_2['timestamp'])
            sva_deltaH_2.set_index('timestamp', inplace=True)
            sva_deltaH_2 = sva_deltaH_2.round(decimals=1)
            sva_deltaH_2['datetime'] = pd.to_datetime(sva_deltaH_2['datetime'])

            if sva_deltaH_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > sva_deltaH_2.iloc[0][1]:
                    if sva_deltaH > sva_deltaH_2.iloc[0][0]:
                        #send_mail()
                        data = {'timestamp':[sva_index], 'Svalbard':[sva_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        sva_deltaH_2 = pd.DataFrame(data)
                        sva_deltaH_2.to_csv('data/compare/sva_deltaH.csv', index = False, sep= " ")
            else:
                #send_mail()
                data = {'timestamp':[sva_index], 'Svalbard':[sva_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                sva_deltaH_2 = pd.DataFrame(data)
                sva_deltaH_2.to_csv('data/compare/sva_deltaH.csv', index = False, sep= " ") 
    g.close()







#Mail function dH
def send_mail_dH():
    sender_email = ""
    receiver_email = []
    password = ""

    message = MIMEMultipart("alternative")
    message["Subject"] = "Geomagnetic Activity Alert - NOSWE"
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message["Bcc"] = ""  # Recommended for mass emails


    text = """\

    Geomagnetic activity alert

    This is an automated message.
    The threshold for extreme geomagnetic activity has been reached in the past hour.
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT/s 
    in """ + str(station) + """ at """ + str(val_datetime) + """.

    To follow up the event, see
    https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/real-time-gic-proxy/

    For further support, contact person@gmail.com (available during normal working hours).

    NOSWE - The Norwegian Centre for Space Weather
    
    ***Testing***"""


    msg = MIMEText(text, "plain")
    message.attach(msg)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())





#dH/dt
#Tromsø
df_dh_tro = dfTro.copy()
df_dh_tro['dH_dt'] = df_dh_tro.Tromsø.diff()
df_dh_tro.drop('Tromsø', axis=1, inplace=True)
df_dh_tro.columns = ['Tromso']
df_dh_tro["datetime"] = df_dh_tro.index
df_dh_tro["time_diff"] = (df_dh_tro['datetime'] - df_dh_tro['datetime'].shift())
df_dh_tro['seconds'] = df_dh_tro['time_diff'].dt.total_seconds()
df_dh_tro['Tromso'] = (df_dh_tro['Tromso']/df_dh_tro['seconds']).round(2).abs()
df_dh_tro.drop(['time_diff', 'seconds'], axis=1, inplace=True)
df_dh_tro = df_dh_tro.iloc[[-2]]


df_dh_tro['datetime'] = df_dh_tro['datetime'].astype(str)
df_dh_tro[['Date','Time']] = df_dh_tro.datetime.str.split(expand=True)
tro_dH = df_dh_tro.iloc[0][0]


if (tro_dH >= 20.0):
    prop = 'dH/dt' #property
    threshold_value = tro_dH #threshold
    station = df_dh_tro.columns[0] #station
    name = 'Extreme'

    tro_index = df_dh_tro.index[0]
    val_datetime = df_dh_tro.iloc[0][1]
    val_date = df_dh_tro.iloc[0][2] #date
    val_time = df_dh_tro.iloc[0][3] #time
    

    with open('data/compare/tro_dH.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_dH.csv') <= 0:
            df_dh_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dH()
        else:
            df_dh_tro_2 = pd.read_csv('data/compare/tro_dH.csv', sep = " ")
            df_dh_tro_2['timestamp'] = pd.to_datetime(df_dh_tro_2['timestamp'])
            df_dh_tro_2.set_index('timestamp', inplace=True)
            df_dh_tro_2['datetime'] = pd.to_datetime(df_dh_tro_2['datetime'])
            df_dh_tro_2 = df_dh_tro_2.round(decimals=2)
            if df_dh_tro_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > df_dh_tro_2.datetime[-1]:
                    tro_dH_old = round(df_dh_tro_2.iloc[0][0], 2)
                    if tro_dH > tro_dH_old:
                        #send_mail_dH()
                        data = {'timestamp':[tro_index], 'Tromso':[tro_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        df_dh_tro_2 = pd.DataFrame(data)
                        df_dh_tro_2.to_csv('data/compare/tro_dH.csv', index = False, sep= " ") 
            else:
                #send_mail_dH()
                data = {'timestamp':[tro_index], 'Tromso':[tro_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_tro_2 = pd.DataFrame(data)
                df_dh_tro_2.to_csv('data/compare/tro_dH.csv', index = False, sep= " ") 
    g.close()






#Dombås
df_dh_dob = dfDob.copy()
df_dh_dob['dH_dt'] = df_dh_dob.Dombås.diff()
df_dh_dob.drop('Dombås', axis=1, inplace=True)
df_dh_dob.columns = ['Dombas']
df_dh_dob["datetime"] = df_dh_dob.index
df_dh_dob["time_diff"] = (df_dh_dob['datetime'] - df_dh_dob['datetime'].shift())
df_dh_dob['seconds'] = df_dh_dob['time_diff'].dt.total_seconds()
df_dh_dob['Dombas'] = (df_dh_dob['Dombas']/df_dh_dob['seconds']).round(2).abs()
df_dh_dob.drop(['time_diff', 'seconds'], axis=1, inplace=True)
df_dh_dob = df_dh_dob.iloc[[-2]]


df_dh_dob['datetime'] = df_dh_dob['datetime'].astype(str)
df_dh_dob[['Date','Time']] = df_dh_dob.datetime.str.split(expand=True)
dob_dH = df_dh_dob.iloc[0][0]


if (dob_dH >= 20.0):
    prop = 'dH/dt' #property
    threshold_value = dob_dH #threshold
    station = df_dh_dob.columns[0] #station
    name = 'Extreme'

    dob_index = df_dh_dob.index[0]
    val_datetime = df_dh_dob.iloc[0][1]
    val_date = df_dh_dob.iloc[0][2] #date
    val_time = df_dh_dob.iloc[0][3] #time
    

    with open('data/compare/dob_dH.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_dH.csv') <= 0:
            df_dh_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dH()
        else:
            df_dh_dob_2 = pd.read_csv('data/compare/dob_dH.csv', sep = " ")
            df_dh_dob_2['timestamp'] = pd.to_datetime(df_dh_dob_2['timestamp'])
            df_dh_dob_2.set_index('timestamp', inplace=True)
            df_dh_dob_2['datetime'] = pd.to_datetime(df_dh_dob_2['datetime'])
            df_dh_dob_2 = df_dh_dob_2.round(decimals=2)
            if df_dh_dob_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > df_dh_dob_2.datetime[-1]:
                    dob_dH_old = round(df_dh_dob_2.iloc[0][0], 2)
                    if dob_dH > dob_dH_old:
                        #send_mail_dH()
                        data = {'timestamp':[dob_index], 'Dombas':[dob_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        df_dh_dob_2 = pd.DataFrame(data)
                        df_dh_dob_2.to_csv('data/compare/dob_dH.csv', index = False, sep= " ") 
            else:
                #send_mail_dH()
                data = {'timestamp':[dob_index], 'Dombas':[dob_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_dob_2 = pd.DataFrame(data)
                df_dh_dob_2.to_csv('data/compare/dob_dH.csv', index = False, sep= " ") 
    g.close()





#Svalbard
df_dh_sva = dfSva.copy()
df_dh_sva['dH_dt'] = df_dh_sva.Svalbard.diff()
df_dh_sva.drop('Svalbard', axis=1, inplace=True)
df_dh_sva.columns = ['Svalbard']
df_dh_sva["datetime"] = df_dh_sva.index
df_dh_sva["time_diff"] = (df_dh_sva['datetime'] - df_dh_sva['datetime'].shift())
df_dh_sva['seconds'] = df_dh_sva['time_diff'].dt.total_seconds()
df_dh_sva['Svalbard'] = (df_dh_sva['Svalbard']/df_dh_sva['seconds']).round(2).abs()
df_dh_sva.drop(['time_diff', 'seconds'], axis=1, inplace=True)
df_dh_sva = df_dh_sva.iloc[[-2]]


df_dh_sva['datetime'] = df_dh_sva['datetime'].astype(str)
df_dh_sva[['Date','Time']] = df_dh_sva.datetime.str.split(expand=True)
sva_dH = df_dh_sva.iloc[0][0]


if (sva_dH >= 20.0):
    prop = 'dH/dt' #property
    threshold_value = sva_dH #threshold
    station = df_dh_sva.columns[0] #station
    name = 'Extreme'

    sva_index = df_dh_sva.index[0]
    val_datetime = df_dh_sva.iloc[0][1]
    val_date = df_dh_sva.iloc[0][2] #date
    val_time = df_dh_sva.iloc[0][3] #time
    

    with open('data/compare/sva_dH.csv', 'a')as g:
        if os.path.getsize('data/compare/sva_dH.csv') <= 0:
            df_dh_sva.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dH()
        else:
            df_dh_sva_2 = pd.read_csv('data/compare/sva_dH.csv', sep = " ")
            df_dh_sva_2['timestamp'] = pd.to_datetime(df_dh_sva_2['timestamp'])
            df_dh_sva_2.set_index('timestamp', inplace=True)
            df_dh_sva_2['datetime'] = pd.to_datetime(df_dh_sva_2['datetime'])
            df_dh_sva_2 = df_dh_sva_2.round(decimals=2)
            if df_dh_sva_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > df_dh_sva_2.datetime[-1]:
                    sva_dH_old = round(df_dh_sva_2.iloc[0][0], 2)
                    if sva_dH > sva_dH_old:
                        #send_mail_dH()
                        data = {'timestamp':[sva_index], 'Svalbard':[sva_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                        df_dh_sva_2 = pd.DataFrame(data)
                        df_dh_sva_2.to_csv('data/compare/sva_dH.csv', index = False, sep= " ") 
            else:
                #send_mail_dH()
                data = {'timestamp':[sva_index], 'Svalbard':[sva_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_sva_2 = pd.DataFrame(data)
                df_dh_sva_2.to_csv('data/compare/sva_dH.csv', index = False, sep= " ") 
    g.close()








#xrays
'''
dfX = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json")
dfX = dfX[dfX['energy'].str.contains("0.1-0.8nm")]
dfX["timestamp"] = pd.to_datetime(dfX["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfX.drop(['time_tag', 'satellite', 'energy'], axis = 1, inplace = True)
dfX['flux'] =[float('{:.3g}'.format(x)) for x in dfX['flux']]
dfX.columns = ['xrays', 'timestamp']
dfX['xrays'] = dfX['xrays'].astype(float)
dfX['datetime'] = dfX['timestamp']
dfX.set_index('timestamp', inplace=True)
dfX = dfX.tail(1)
'''
dfX = pd.read_csv("../Combined_data/data/threshold_xrays.csv", sep=" ")
dfX['datetime'] = dfX['timestamp']
dfX.set_index('timestamp', inplace=True)
#dfX.drop(dfX.tail(1).index,inplace=True)
#dfX = dfX.tail(1)


#protons
'''
dfP = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/integral-protons-1-day.json")
dfP = dfP[dfP['energy'].str.contains(">=10 MeV")]
dfP["timestamp"] = pd.to_datetime(dfP["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfP.drop(['time_tag', 'satellite', 'energy'], axis = 1, inplace = True)
dfP['flux'] = dfP['flux'].astype(float).round(2)
dfP.columns = ['protons', 'timestamp']
dfP['protons'] = dfP['protons'].astype(float)
dfP['datetime'] = dfP['timestamp']
dfP.set_index('timestamp', inplace=True)
dfP = dfP.tail(1)
'''
dfP = pd.read_csv("../Combined_data/data/threshold_protons.csv", sep=" ")
dfP['datetime'] = dfP['timestamp']
dfP.set_index('timestamp', inplace=True)

#print(dfP)





#xrays
xray_val = dfX.iloc[0][0]
xray_val = '{:0.2e}'.format(xray_val)
xray_val = float(xray_val)


#dfX['datetime'] = dfX['datetime'].astype(str)
dfX = dfX.assign(datetime=lambda d: d['datetime'].astype(str))
dfX[['Date','Time']] = dfX.datetime.str.split(expand=True)


if (xray_val >= 1e-5) and (xray_val < 2e-3):
    prop = 'Radio blackouts' #property
    threshold_value  = xray_val #threshold value
    station = ''

    dfX_index = dfX.index[0]
    val_datetime = dfX.iloc[0][1]
    val_date = dfX.iloc[0][2] #date
    val_time = dfX.iloc[0][3] #time
    
   

    if (threshold_value >= 1e-5) and (threshold_value < 5e-5):
        name = 'Minor'
    elif (threshold_value >= 5e-5) and (threshold_value < 1e-4):
        name  = 'Moderate'
    elif (threshold_value >= 1e-4) and (threshold_value < 1e-3):
        name = 'Strong'
    elif (threshold_value >= 1e-3) and (threshold_value < 2e-3):
        name = 'Severe'
    elif (threshold_value >= 2e-3):
        name = 'Extreme'

    with open('data/compare/xrays.csv', 'a')as g:
        if os.path.getsize('data/compare/xrays.csv') <= 0:
            dfX.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail()
        else:
            dfX_2 = pd.read_csv('data/compare/xrays.csv', sep = " ")
            dfX_2['timestamp'] = pd.to_datetime(dfX_2['timestamp'])
            dfX_2.set_index('timestamp', inplace=True)
            dfX_2['xrays'] =[float('{:0.2e}'.format(x)) for x in dfX_2['xrays']]
            dfX_2['datetime'] = pd.to_datetime(dfX_2['datetime'])
            if dfX_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > dfX_2.datetime[-1]:
                    xray_val_old = dfX_2.iloc[0][0]
                    xray_val_old = '{:0.2e}'.format(xray_val_old)
                    xray_val_old = float(xray_val_old)
                    if xray_val > xray_val_old:
                        #send_mail()
                        data = {'timestamp':[dfX_index], 'xrays':[xray_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                        dfX_2 = pd.DataFrame(data)
                        dfX_2.to_csv('data/compare/xrays.csv', index = False, sep= " ") 
            else:
                #send_mail()
                data = {'timestamp':[dfX_index], 'xrays':[xray_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfX_2 = pd.DataFrame(data)
                dfX_2.to_csv('data/compare/xrays.csv', index = False, sep= " ") 
    g.close()






#protons
proton_val = dfP.iloc[0][0]
proton_val = round(proton_val, 2)
proton_val = float(proton_val)


dfP['datetime'] = dfP['datetime'].astype(str)
dfP[['Date','Time']] = dfP.datetime.str.split(expand=True)


if (proton_val >= 10) and (proton_val < 1e5):
    prop = 'Radiation storms' #property
    threshold_value  = proton_val #threshold value
    station = ''

    dfP_index = dfP.index[0]
    val_datetime = dfP.iloc[0][1]
    val_date = dfP.iloc[0][2] #date
    val_time = dfP.iloc[0][3] #time

   

    if (threshold_value >= 10) and (threshold_value < 1e2):
        name = 'Minor'
    elif (threshold_value >= 1e2) and (threshold_value < 1e3):
        name  = 'Moderate'
    elif (threshold_value >= 1e3) and (threshold_value < 1e4):
        name = 'Strong'
    elif (threshold_value >= 1e4) and (threshold_value < 1e5):
        name = 'Severe'
    elif (threshold_value >= 1e5):
        name = 'Extreme'

    with open('data/compare/protons.csv', 'a')as g:
        if os.path.getsize('data/compare/protons.csv') <= 0:
            dfP.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail()
        else:
            dfP_2 = pd.read_csv('data/compare/protons.csv', sep = " ")
            dfP_2['timestamp'] = pd.to_datetime(dfP_2['timestamp'])
            dfP_2.set_index('timestamp', inplace=True)
            dfP_2['protons'] = dfP_2['protons'].astype(float).round(2)
            dfP_2['datetime'] = pd.to_datetime(dfP_2['datetime'])
            if dfP_2.index[-1] == datetime.today().date():
                if dtm.datetime.utcnow().replace(microsecond=0) > dfP_2.datetime[-1]:
                    proton_val_old = round(dfP_2.iloc[0][0], 2)
                    proton_val_old = float(proton_val_old)
                    if proton_val > proton_val_old:
                        #send_mail()
                        data = {'timestamp':[dfP_index], 'protons':[proton_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                        dfP_2 = pd.DataFrame(data)
                        dfP_2.to_csv('data/compare/protons.csv', index = False, sep= " ") 
            else:
                #send_mail()
                data = {'timestamp':[dfP_index], 'protons':[proton_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfX_2 = pd.DataFrame(data)
                dfX_2.to_csv('data/compare/protons.csv', index = False, sep= " ") 
    g.close()