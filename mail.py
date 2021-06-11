import pandas as pd
import numpy as np
import subprocess
import time
import datetime as dtm
from datetime import timezone, datetime
import json
import os
import yaml
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
yaml.warnings({'YAMLLoadWarning': False})


crt_yr = dtm.datetime.utcnow().year
crt_month = dtm.datetime.utcnow().month
crt_day =dtm.datetime.utcnow().day
crt_time = dtm.datetime.utcnow().replace(microsecond=0)
crt_hour = crt_time.hour
crt_minute = crt_time.minute
crt_date = dtm.datetime(year=crt_yr, month=crt_month, day=crt_day, hour=crt_hour, minute=crt_minute)
crt_date_2 = dtm.datetime(year=crt_yr, month=crt_month, day=crt_day, hour=crt_hour, minute=0)
end_range = crt_time

date = dtm.datetime.utcnow()

datestring_yr = dtm.datetime.strftime(date, '%Y')
datestring_mo= dtm.datetime.strftime(date, '%m')
datestring_da = dtm.datetime.strftime(date, '%d')




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
dfTro = dfTro.drop_duplicates(subset=['timestamp', 'Tromso'])
dfTro["Tromso"] = pd.to_numeric(dfTro["Tromso"])
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
dfDob = dfDob.drop_duplicates(subset=['timestamp', 'Dombas'])
dfDob["Dombas"] = pd.to_numeric(dfDob["Dombas"])
dfDob.set_index('timestamp', inplace=True)

dfDob.to_csv('data/hcom/dombås.csv', index= True, sep= " ")





#Ny_Alesund
url_Nal = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=nal1a&year="+datestring_yr+"&month="+datestring_mo+"&day="+datestring_da+"&res=10sec&pwd=&format=html&comps=DHZ&getdata=+Get+data+"

dfNal = pd.read_csv(url_Nal, skiprows = 6)
dfNal = dfNal.astype(str)
dfNal = dfNal[dfNal.columns[0]].str.split(expand=True)

dfNal.columns = ['Date', 'Time', 'Dec', 'Ny_Alesund', 'Vert', 'Incl', 'Tot']
dfNal['timestamp'] = dfNal['Date'] + ' ' + dfNal['Time']
dfNal.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfNal = dfNal[dfNal.Ny_Alesund != '99999.9']
dfNal['timestamp'] = pd.to_datetime(dfNal['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfNal = dfNal.drop_duplicates(subset=['timestamp', 'Ny_Alesund'])
dfNal["Ny_Alesund"] = pd.to_numeric(dfNal["Ny_Alesund"])
dfNal.set_index('timestamp', inplace=True)

dfNal.to_csv('data/hcom/ny_alesund.csv', index= True, sep= " ")





#deltaH
df_ai_tro = dfTro.resample('H')['Tromso'].agg(['max', 'min'])
df_ai_tro['Tromso'] = abs(df_ai_tro['max'].sub(df_ai_tro['min'], axis = 0))
df_ai_tro['Tromso'] = df_ai_tro['Tromso'].shift(1)
df_ai_tro.drop(['max', 'min'], axis=1, inplace=True)
df_ai_tro = df_ai_tro.tail(1)
df_ai_tro["datetime"] = crt_date
df_ai_tro['datetime'] = df_ai_tro['datetime'].astype(str)
df_ai_tro[['Date','Time']] = df_ai_tro.datetime.str.split(expand=True)


df_ai_dob = dfDob.resample('H')['Dombas'].agg(['max', 'min'])
df_ai_dob['Dombas'] = abs(df_ai_dob['max'].sub(df_ai_dob['min'], axis = 0))
df_ai_dob['Dombas'] = df_ai_dob['Dombas'].shift(1)
df_ai_dob.drop(['max', 'min'], axis=1, inplace=True)
df_ai_dob = df_ai_dob.tail(1)
df_ai_dob["datetime"] = crt_date
df_ai_dob['datetime'] = df_ai_dob['datetime'].astype(str)
df_ai_dob[['Date','Time']] = df_ai_dob.datetime.str.split(expand=True)


df_ai_Nal = dfNal.resample('H')['Ny_Alesund'].agg(['max', 'min'])
df_ai_Nal['Ny_Alesund'] = abs(df_ai_Nal['max'].sub(df_ai_Nal['min'], axis = 0))
df_ai_Nal['Ny_Alesund'] = df_ai_Nal['Ny_Alesund'].shift(1)
df_ai_Nal.drop(['max', 'min'], axis=1, inplace=True)
df_ai_Nal = df_ai_Nal.tail(1)
df_ai_Nal["datetime"] = crt_date
df_ai_Nal['datetime'] = df_ai_Nal['datetime'].astype(str)
df_ai_Nal[['Date','Time']] = df_ai_Nal.datetime.str.split(expand=True)







#Mail function deltaH Tro
def send_mail_tro():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\
    
    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for """ + str(name) + """ geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/real-time-geomagnetic-disturbances/"> Realtime ground disturbance (deltaH)</a> <br><br>

    and for more information on the thresholds, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/summary-and-forecast/"> Geomagnetic summary and forecast </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>

    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""

    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#Tromsø
tro_deltaH = df_ai_tro.iloc[0][0]
tro_deltaH = round(tro_deltaH, 1)


if (tro_deltaH >= 281) and (tro_deltaH < 481):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Minor'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH_minor.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH_minor.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH_minor.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH_minor.csv', index = False, sep= " ")
    g.close()



if (tro_deltaH >= 481) and (tro_deltaH < 801):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Moderate'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH_moderate.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH_moderate.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH_moderate.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH_moderate.csv', index = False, sep= " ")
    g.close()



if (tro_deltaH >= 801) and (tro_deltaH < 1321):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Strong'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH_strong.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH_strong.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH_strong.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH_strong.csv', index = False, sep= " ")
    g.close()



if (tro_deltaH >= 1321) and (tro_deltaH < 2001):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Severe'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH_severe.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH_severe.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH_severe.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH_severe.csv', index = False, sep= " ")
    g.close()



if (tro_deltaH >= 2001):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Extreme'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/compare/tro_deltaH_extreme.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_deltaH_extreme.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/compare/tro_deltaH_extreme.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/compare/tro_deltaH_extreme.csv', index = False, sep= " ")
    g.close()





def send_mail_dob():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\
    
    <p>Message below the line: <br>
    -----------------------------------------------------------<br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for """ + str(name) + """ geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/real-time-geomagnetic-disturbances/"> Realtime ground disturbance (deltaH)</a> <br><br>

    and for more information on the thresholds, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/summary-and-forecast/"> Geomagnetic summary and forecast </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>

    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""



    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#Dombås
dob_deltaH = df_ai_dob.iloc[0][0]
dob_deltaH = round(dob_deltaH, 1)

if (dob_deltaH >= 106) and (dob_deltaH < 180):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Minor'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH_minor.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH_minor.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dob()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH_minor.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            minutes_diff = (crt_date - dob_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH_minor.csv', index = False, sep= " ") 
    g.close()



if (dob_deltaH >= 180) and (dob_deltaH > 300):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Moderate'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH_moderate.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH_moderate.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dob()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH_moderate.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            minutes_diff = (crt_date - dob_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH_moderate.csv', index = False, sep= " ") 
    g.close()



if (dob_deltaH >= 300) and (dob_deltaH < 495):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Strong'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH_strong.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH_strong.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dob()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH_strong.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            minutes_diff = (crt_date - dob_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH_strong.csv', index = False, sep= " ") 
    g.close()



if (dob_deltaH >= 495) and (dob_deltaH < 750):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Severe'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH_severe.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH_severe.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dob()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH_severe.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            minutes_diff = (crt_date - dob_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH_severe.csv', index = False, sep= " ") 
    g.close()



if (dob_deltaH >= 750):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = dob_deltaH
    station = "Dombås"  #df_ai_dob.columns[0]
    name = 'Extreme'

    dob_index = df_ai_dob.index[0]
    val_datetime = df_ai_dob.iloc[0][1]
    val_date = df_ai_dob.iloc[0][2]
    val_time = df_ai_dob.iloc[0][3]


    with open('data/compare/dob_deltaH_extreme.csv', 'a')as g:
        if os.path.getsize('data/compare/dob_deltaH_extreme.csv') <= 0:
            df_ai_dob.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dob()
        else:
            dob_deltaH_2 = pd.read_csv('data/compare/dob_deltaH_extreme.csv', sep = " ")
            dob_deltaH_2['timestamp'] = pd.to_datetime(dob_deltaH_2['timestamp'])
            dob_deltaH_2.set_index('timestamp', inplace=True)
            dob_deltaH_2 = dob_deltaH_2.round(decimals=1)
            dob_deltaH_2['datetime'] = pd.to_datetime(dob_deltaH_2['datetime'])

            minutes_diff = (crt_date - dob_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                dob_deltaH_2 = pd.DataFrame(data)
                dob_deltaH_2.to_csv('data/compare/dob_deltaH_extreme.csv', index = False, sep= " ") 
    g.close()






def send_mail_nal():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\
    
    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for """ + str(name) + """ geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see
    <a href = "https://site.uit.no/spaceweather/data-and-products/real-time-geomagnetic-disturbances/"> Realtime ground disturbance (deltaH)</a> <br><br>

    and for more information on the thresholds, see
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/summary-and-forecast/"> Geomagnetic summary and forecast </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>

    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""


    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#Ny_Alesund
nal_deltaH = df_ai_Nal.iloc[0][0]
nal_deltaH = round(nal_deltaH, 1)

if (nal_deltaH >= 281) and (nal_deltaH < 481):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = nal_deltaH
    station = df_ai_Nal.columns[0]
    name = 'Minor'

    nal_index = df_ai_Nal.index[0]
    val_datetime = df_ai_Nal.iloc[0][1]
    val_date = df_ai_Nal.iloc[0][2]
    val_time = df_ai_Nal.iloc[0][3]


    with open('data/compare/nal_deltaH_minor.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_deltaH_minor.csv') <= 0:
            df_ai_Nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_nal()
        else:
            nal_deltaH_2 = pd.read_csv('data/compare/nal_deltaH_minor.csv', sep = " ")
            nal_deltaH_2['timestamp'] = pd.to_datetime(nal_deltaH_2['timestamp'])
            nal_deltaH_2.set_index('timestamp', inplace=True)
            nal_deltaH_2 = nal_deltaH_2.round(decimals=1)
            nal_deltaH_2['datetime'] = pd.to_datetime(nal_deltaH_2['datetime'])

            minutes_diff = (crt_date - nal_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                nal_deltaH_2 = pd.DataFrame(data)
                nal_deltaH_2.to_csv('data/compare/nal_deltaH_minor.csv', index = False, sep= " ") 
    g.close()



if (nal_deltaH >= 481) and (nal_deltaH < 801):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = nal_deltaH
    station = df_ai_Nal.columns[0]
    name = 'Moderate'

    nal_index = df_ai_Nal.index[0]
    val_datetime = df_ai_Nal.iloc[0][1]
    val_date = df_ai_Nal.iloc[0][2]
    val_time = df_ai_Nal.iloc[0][3]


    with open('data/compare/nal_deltaH_moderate.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_deltaH_moderate.csv') <= 0:
            df_ai_Nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_nal()
        else:
            nal_deltaH_2 = pd.read_csv('data/compare/nal_deltaH_moderate.csv', sep = " ")
            nal_deltaH_2['timestamp'] = pd.to_datetime(nal_deltaH_2['timestamp'])
            nal_deltaH_2.set_index('timestamp', inplace=True)
            nal_deltaH_2 = nal_deltaH_2.round(decimals=1)
            nal_deltaH_2['datetime'] = pd.to_datetime(nal_deltaH_2['datetime'])

            minutes_diff = (crt_date - nal_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                nal_deltaH_2 = pd.DataFrame(data)
                nal_deltaH_2.to_csv('data/compare/nal_deltaH_moderate.csv', index = False, sep= " ") 
    g.close()



if (nal_deltaH >= 801) and (nal_deltaH < 1321):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = nal_deltaH
    station = df_ai_Nal.columns[0]
    name = 'Strong'

    nal_index = df_ai_Nal.index[0]
    val_datetime = df_ai_Nal.iloc[0][1]
    val_date = df_ai_Nal.iloc[0][2]
    val_time = df_ai_Nal.iloc[0][3]


    with open('data/compare/nal_deltaH_strong.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_deltaH_strong.csv') <= 0:
            df_ai_Nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_nal()
        else:
            nal_deltaH_2 = pd.read_csv('data/compare/nal_deltaH_strong.csv', sep = " ")
            nal_deltaH_2['timestamp'] = pd.to_datetime(nal_deltaH_2['timestamp'])
            nal_deltaH_2.set_index('timestamp', inplace=True)
            nal_deltaH_2 = nal_deltaH_2.round(decimals=1)
            nal_deltaH_2['datetime'] = pd.to_datetime(nal_deltaH_2['datetime'])

            minutes_diff = (crt_date - nal_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                nal_deltaH_2 = pd.DataFrame(data)
                nal_deltaH_2.to_csv('data/compare/nal_deltaH_strong.csv', index = False, sep= " ") 
    g.close()



if (nal_deltaH >= 1321) and (nal_deltaH < 2001):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = nal_deltaH
    station = df_ai_Nal.columns[0]
    name = 'Severe'

    nal_index = df_ai_Nal.index[0]
    val_datetime = df_ai_Nal.iloc[0][1]
    val_date = df_ai_Nal.iloc[0][2]
    val_time = df_ai_Nal.iloc[0][3]


    with open('data/compare/nal_deltaH_severe.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_deltaH_severe.csv') <= 0:
            df_ai_Nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_nal()
        else:
            nal_deltaH_2 = pd.read_csv('data/compare/nal_deltaH_severe.csv', sep = " ")
            nal_deltaH_2['timestamp'] = pd.to_datetime(nal_deltaH_2['timestamp'])
            nal_deltaH_2.set_index('timestamp', inplace=True)
            nal_deltaH_2 = nal_deltaH_2.round(decimals=1)
            nal_deltaH_2['datetime'] = pd.to_datetime(nal_deltaH_2['datetime'])

            minutes_diff = (crt_date - nal_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                nal_deltaH_2 = pd.DataFrame(data)
                nal_deltaH_2.to_csv('data/compare/nal_deltaH_severe.csv', index = False, sep= " ") 
    g.close()



if (nal_deltaH >= 2000):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = nal_deltaH
    station = df_ai_Nal.columns[0]
    name = 'Extreme'

    nal_index = df_ai_Nal.index[0]
    val_datetime = df_ai_Nal.iloc[0][1]
    val_date = df_ai_Nal.iloc[0][2]
    val_time = df_ai_Nal.iloc[0][3]


    with open('data/compare/nal_deltaH_extreme.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_deltaH_extreme.csv') <= 0:
            df_ai_Nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_nal()
        else:
            nal_deltaH_2 = pd.read_csv('data/compare/nal_deltaH_extreme.csv', sep = " ")
            nal_deltaH_2['timestamp'] = pd.to_datetime(nal_deltaH_2['timestamp'])
            nal_deltaH_2.set_index('timestamp', inplace=True)
            nal_deltaH_2 = nal_deltaH_2.round(decimals=1)
            nal_deltaH_2['datetime'] = pd.to_datetime(nal_deltaH_2['datetime'])

            minutes_diff = (crt_date - nal_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                nal_deltaH_2 = pd.DataFrame(data)
                nal_deltaH_2.to_csv('data/compare/nal_deltaH_extreme.csv', index = False, sep= " ") 
    g.close()

















#Mail function dH_tro
def send_mail_dH_tro():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\

    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for extreme geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT/s 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/real-time-gic-proxy/"> Realtime ground disturbance (dH/dt) </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>
    
    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""


    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#dH/dt
#Tromsø
df_dh_tro = dfTro.copy()
df_dh_tro['dH_dt'] = df_dh_tro.Tromso.diff()
df_dh_tro.drop('Tromso', axis=1, inplace=True)
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
    recipients = []
    prop = 'dH/dt' #property
    threshold_value = tro_dH #threshold
    station = 'Tromsø' #df_dh_tro.columns[0] (station)
    name = 'Extreme'

    tro_index = df_dh_tro.index[0]
    val_datetime = df_dh_tro.iloc[0][1]
    val_date = df_dh_tro.iloc[0][2] #date
    val_time = df_dh_tro.iloc[0][3] #time
    
    with open('data/compare/tro_dH.csv', 'a')as g:
        if os.path.getsize('data/compare/tro_dH.csv') <= 0:
            df_dh_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dH_tro()
        else:
            df_dh_tro_2 = pd.read_csv('data/compare/tro_dH.csv', sep = " ")
            df_dh_tro_2['timestamp'] = pd.to_datetime(df_dh_tro_2['timestamp'])
            df_dh_tro_2.set_index('timestamp', inplace=True)
            df_dh_tro_2 = df_dh_tro_2.round(decimals=2)
            df_dh_tro_2['datetime'] = pd.to_index(df_dh_tro_2['datetime'])

            minutes_diff = (crt_date - df_dh_tro_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dH_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_tro_2 = pd.DataFrame(data)
                df_dh_tro_2.to_csv('data/compare/tro_dH.csv', index = False, sep= " ") 
    g.close()






#Mail function dH_dob
def send_mail_dH_dob():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\

    <p>Message below the line: <br>
    -----------------------------------------------------------<br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for extreme geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT/s 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/real-time-gic-proxy/"> Realtime ground disturbance (dH/dt) </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>
    
    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""


    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()




#Dombås
df_dh_dob = dfDob.copy()
df_dh_dob['dH_dt'] = df_dh_dob.Dombas.diff()
df_dh_dob.drop('Dombas', axis=1, inplace=True)
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
    recipients = []
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
            #send_mail_dH_dob()
        else:
            df_dh_dob_2 = pd.read_csv('data/compare/dob_dH.csv', sep = " ")
            df_dh_dob_2['timestamp'] = pd.to_datetime(df_dh_dob_2['timestamp'])
            df_dh_dob_2.set_index('timestamp', inplace=True)
            df_dh_dob_2 = df_dh_dob_2.round(decimals=2)
            df_dh_dob_2['datetime'] = pd.to_datetime(df_dh_dob_2['datetime'])

            minutes_diff = (crt_date - df_dh_dob_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dH_dob()
                data = {'timestamp':[dob_index], 'Dombas':[dob_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_dob_2 = pd.DataFrame(data)
                df_dh_dob_2.to_csv('data/compare/dob_dH.csv', index = False, sep= " ") 
    g.close()






#Mail function dH_nal
def send_mail_dH_nal():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\

    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    GEOMAGNETIC ACTIVITY WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for extreme geomagnetic activity has been reached in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT/s 
    in """ + str(station) + """ at """ + str(val_datetime) + """ UTC. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/real-time-gic-proxy/"> Realtime ground disturbance (dH/dt) </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>
    
    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""


    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#Ny_Alesund
df_dh_nal = dfNal.copy()
df_dh_nal['dH_dt'] = df_dh_nal.Ny_Alesund.diff()
df_dh_nal.drop('Ny_Alesund', axis=1, inplace=True)
df_dh_nal.columns = ['Ny_Alesund']
df_dh_nal["datetime"] = df_dh_nal.index
df_dh_nal["time_diff"] = (df_dh_nal['datetime'] - df_dh_nal['datetime'].shift())
df_dh_nal['seconds'] = df_dh_nal['time_diff'].dt.total_seconds()
df_dh_nal['Ny_Alesund'] = (df_dh_nal['Ny_Alesund']/df_dh_nal['seconds']).round(2).abs()
df_dh_nal.drop(['time_diff', 'seconds'], axis=1, inplace=True)
df_dh_nal = df_dh_nal.iloc[[-2]]

df_dh_nal['datetime'] = df_dh_nal['datetime'].astype(str)
df_dh_nal[['Date','Time']] = df_dh_nal.datetime.str.split(expand=True)
nal_dH = df_dh_nal.iloc[0][0]


if (nal_dH >= 20.0):
    recipients = []
    prop = 'dH/dt' #property
    threshold_value = nal_dH #threshold
    station = df_dh_nal.columns[0] #station
    name = 'Extreme'

    nal_index = df_dh_nal.index[0]
    val_datetime = df_dh_nal.iloc[0][1]
    val_date = df_dh_nal.iloc[0][2] #date
    val_time = df_dh_nal.iloc[0][3] #time
    

    with open('data/compare/nal_dH.csv', 'a')as g:
        if os.path.getsize('data/compare/nal_dH.csv') <= 0:
            df_dh_nal.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_dH_nal()
        else:
            df_dh_nal_2 = pd.read_csv('data/compare/nal_dH.csv', sep = " ")
            df_dh_nal_2['timestamp'] = pd.to_datetime(df_dh_nal_2['timestamp'])
            df_dh_nal_2.set_index('timestamp', inplace=True)
            df_dh_nal_2 = df_dh_nal_2.round(decimals=2)
            df_dh_nal_2['datetime'] = pd.to_datetime(df_dh_nal_2['datetime'])

            minutes_diff = (crt_date - df_dh_nal_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_dH_nal()
                data = {'timestamp':[nal_index], 'Ny_Alesund':[nal_dH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                df_dh_nal_2 = pd.DataFrame(data)
                df_dh_nal_2.to_csv('data/compare/nal_dH.csv', index = False, sep= " ") 
    g.close()

















#xrays
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



#Mail function xrays 
def send_mail_xrays():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\

    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    RADIO BLACKOUT WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for """ + str(name) + """ radio signal disturbances has been reached. <br>
    The 0.1-0.8 nm X-ray flux measured by the GOES-16 satellite has exceeded """ + str(threshold_value) + """ W/m2 
    (NOAA """ + str(physical_measure) + """) at """ + str(dfX_index) + """ UTC. <br><br>

    For further information on the scales, see <br>
    <a href = "https://site.uit.no/spaceweather/radio-blackouts-and-radiation-storms/"> Radio blackouts and radiation storms </a> <br><br>

    To follow up the event, see “Event Analysis” at <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/basic-event-analysis/"> Basic event analysis </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>
    
    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""


    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()



#xrays
xray_val = dfX.iloc[0][0]
xray_val = '{:0.2e}'.format(xray_val)
xray_val = float(xray_val)


dfX['datetime'] = dfX['datetime'].astype(str)
dfX[['Date','Time']] = dfX.datetime.str.split(expand=True)

if (xray_val >= 1e-3) and (xray_val <2e-3):
    recipients = []
    prop = 'Radio blackouts' #property
    threshold_value  = xray_val #threshold value
    name = 'Severe'
    physical_measure = 'R4'

    dfX_index = dfX.index[0]
    val_datetime = dfX.iloc[0][1]
    val_date = dfX.iloc[0][2] #date
    val_time = dfX.iloc[0][3] #time
    

    with open('data/compare/xrays_severe.csv', 'a')as g:
        if os.path.getsize('data/compare/xrays_severe.csv') <= 0:
            dfX.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_xrays()
        else:
            dfX_2 = pd.read_csv('data/compare/xrays_severe.csv', sep = " ")
            dfX_2['timestamp'] = pd.to_datetime(dfX_2['timestamp'])
            dfX_2.set_index('timestamp', inplace=True)
            dfX_2['xrays'] =[float('{:0.2e}'.format(x)) for x in dfX_2['xrays']]
            dfX_2['datetime'] = pd.to_datetime(dfX_2['datetime'])

            minutes_diff = (crt_date - dfX_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_xrays()
                data = {'timestamp':[dfX_index], 'xrays':[xray_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfX_2 = pd.DataFrame(data)
                dfX_2.to_csv('data/compare/xrays_severe.csv', index = False, sep= " ") 

    g.close()




if (xray_val >= 2e-3):
    recipients = []
    prop = 'Radio blackouts' #property
    threshold_value  = xray_val #threshold value
    name = 'Extreme'
    physical_measure = 'R5'

    dfX_index = dfX.index[0]
    val_datetime = dfX.iloc[0][1]
    val_date = dfX.iloc[0][2] #date
    val_time = dfX.iloc[0][3] #time
    

    with open('data/compare/xrays_extreme.csv', 'a')as g:
        if os.path.getsize('data/compare/xrays_extreme.csv') <= 0:
            dfX.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_xrays()
        else:
            dfX_2 = pd.read_csv('data/compare/xrays_extreme.csv', sep = " ")
            dfX_2['timestamp'] = pd.to_datetime(dfX_2['timestamp'])
            dfX_2.set_index('timestamp', inplace=True)
            dfX_2['xrays'] =[float('{:0.2e}'.format(x)) for x in dfX_2['xrays']]
            dfX_2['datetime'] = pd.to_datetime(dfX_2['datetime'])

            minutes_diff = (crt_date - dfX_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_xrays()
                data = {'timestamp':[dfX_index], 'xrays':[xray_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfX_2 = pd.DataFrame(data)
                dfX_2.to_csv('data/compare/xrays_extreme.csv', index = False, sep= " ") 

    g.close()






#protons
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




#Mail function protons 
def send_mail_protons():
    conf = yaml.safe_load(open('credentials.yml'))
    sender_email = conf['user']['email']
    password = conf['user']['password']
    to_addr = ""
    receiver_email = recipients
    message = MIMEMultipart("alternative")
    message["Subject"] = "GEOMAGNETIC ACTIVITY WARNING"
    message["From"] = sender_email
    message["To"] = to_addr
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails


    text = """\

    <p>Message below the line: <br>
    ----------------------------------------------------------- <br><br>

    RADIATION STORM WARNING <br>
    This is an automatically generated message. <br><br>

    The threshold for """ + str(name) + """ radio signal disturbances has been reached. <br>
    The flux level of ≥ 10 MeV particles measured by the GOES-16 satellite has exceeded """ + str(threshold_value) + """ pfu 
    (NOAA """ + str(physical_measure) + """) at """ + str(dfP_index) + """ UTC. <br><br>

    For further information on the scales, see <br>
    <a href = "https://site.uit.no/spaceweather/radio-blackouts-and-radiation-storms/"> Radio blackouts and radiation storms </a> <br><br>

    To follow up the event, see “Event Analysis” at <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/basic-event-analysis/"> Basic event analysis </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>
    
    -----------------------------------------------------------</p>"""

    html = """<a href="link">Unsubscribe from Geomagnetic activity warning</a>"""

    msg = MIMEText(text+html, "plain")
    msg1 = MIMEText(text+html, "html")
    message.attach(msg)
    message.attach(msg1)

    #send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(message)
        #server.sendmail(sender_email, [to_addr] + receiver_email, message.as_string())
        server.quit()




#protons
proton_val = dfP.iloc[0][0]
proton_val = round(proton_val, 2)
proton_val = float(proton_val)


dfP['datetime'] = dfP['datetime'].astype(str)
dfP[['Date','Time']] = dfP.datetime.str.split(expand=True)


if (proton_val >= 1e3) and (proton_val < 1e4):
    recipients = [] 
    prop = 'Radiation storms' #property
    threshold_value  = proton_val #threshold value
    name = 'Strong'
    physical_measure = 'S3'

    dfP_index = dfP.index[0]
    val_datetime = dfP.iloc[0][1]
    val_date = dfP.iloc[0][2] #date
    val_time = dfP.iloc[0][3] #time



    with open('data/compare/protons_strong.csv', 'a')as g:
        if os.path.getsize('data/compare/protons_strong.csv') <= 0:
            dfP.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_protons()
        else:
            dfP_2 = pd.read_csv('data/compare/protons_strong.csv', sep = " ")
            dfP_2['timestamp'] = pd.to_datetime(dfP_2['timestamp'])
            dfP_2.set_index('timestamp', inplace=True)
            dfP_2['protons'] = dfP_2['protons'].astype(float).round(2)
            dfP_2['datetime'] = pd.to_datetime(dfP_2['datetime'])

            minutes_diff = (crt_date - dfP_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_protons()
                data = {'timestamp':[dfP_index], 'protons':[proton_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfP_2 = pd.DataFrame(data)
                dfP_2.to_csv('data/compare/protons_strong.csv', index = False, sep= " ")

    g.close()



if (proton_val >= 1e4) and (proton_val < 1e5):
    recipients = [] 
    prop = 'Radiation storms' #property
    threshold_value  = proton_val #threshold value
    name = 'Severe'
    physical_measure = 'S4'

    dfP_index = dfP.index[0]
    val_datetime = dfP.iloc[0][1]
    val_date = dfP.iloc[0][2] #date
    val_time = dfP.iloc[0][3] #time


    with open('data/compare/protons_severe.csv', 'a')as g:
        if os.path.getsize('data/compare/protons_severe.csv') <= 0:
            dfP.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_protons()
        else:
            dfP_2 = pd.read_csv('data/compare/protons_severe.csv', sep = " ")
            dfP_2['timestamp'] = pd.to_datetime(dfP_2['timestamp'])
            dfP_2.set_index('timestamp', inplace=True)
            dfP_2['protons'] = dfP_2['protons'].astype(float).round(2)
            dfP_2['datetime'] = pd.to_datetime(dfP_2['datetime'])

            minutes_diff = (crt_date - dfP_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_protons()
                data = {'timestamp':[dfP_index], 'protons':[proton_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfP_2 = pd.DataFrame(data)
                dfP_2.to_csv('data/compare/protons_severe.csv', index = False, sep= " ")

    g.close()



if (proton_val >= 1e5):
    recipients = [] 
    prop = 'Radiation storms' #property
    threshold_value  = proton_val #threshold value
    name = 'Extreme'
    physical_measure = 'S5'

    dfP_index = dfP.index[0]
    val_datetime = dfP.iloc[0][1]
    val_date = dfP.iloc[0][2] #date
    val_time = dfP.iloc[0][3] #time


    with open('data/compare/protons_extreme.csv', 'a')as g:
        if os.path.getsize('data/compare/protons_extreme.csv') <= 0:
            dfP.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            #send_mail_protons()
        else:
            dfP_2 = pd.read_csv('data/compare/protons_extreme.csv', sep = " ")
            dfP_2['timestamp'] = pd.to_datetime(dfP_2['timestamp'])
            dfP_2.set_index('timestamp', inplace=True)
            dfP_2['protons'] = dfP_2['protons'].astype(float).round(2)
            dfP_2['datetime'] = pd.to_datetime(dfP_2['datetime'])

            minutes_diff = (crt_date - dfP_2.index[-1]).total_seconds() / 60.0
            if minutes_diff >= 180:
                #send_mail_protons()
                data = {'timestamp':[dfP_index], 'protons':[proton_val], 'datetime':[val_datetime],'Date':[val_date], 'Time':[val_time]}
                dfP_2 = pd.DataFrame(data)
                dfP_2.to_csv('data/compare/protons_extreme.csv', index = False, sep= " ")

    g.close()