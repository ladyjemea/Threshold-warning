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

start_time = dtm.datetime(year=crt_yr, month=crt_month, day=crt_day, hour=0, minute=0)
end_time = start_time + dtm.timedelta(minutes=60)

date = dtm.datetime.utcnow()
datetime = crt_date

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

#dfTro.to_csv('data/hcom/tromsø_test_csv.csv', index= True, sep= " ")

#tro_deltaH
last = dfTro.iloc[[-2]]
end = last.index[0]
start = end - dtm.timedelta(minutes=60)
dfTro = dfTro.loc[start:end]
max_value = dfTro['Tromso'].max()
min_value = dfTro['Tromso'].min()
data = {'timestamp':[datetime], 'Max':[max_value], 'Min':[min_value]}
df_ai_tro = pd.DataFrame(data)
df_ai_tro.set_index('timestamp', inplace=True)
df_ai_tro['Tromso'] = abs(df_ai_tro['Max'] - df_ai_tro['Min'])
df_ai_tro.drop(['Max', 'Min'], axis=1, inplace=True)
df_ai_tro["datetime"] = crt_date
df_ai_tro['datetime'] = df_ai_tro['datetime'].astype(str)
df_ai_tro[['Date','Time']] = df_ai_tro.datetime.str.split(expand=True)



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

    '''
    with open("contacts_file.csv") as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for name, email, grade in reader:
        print(f"Sending email to {name}")
        # Send email here
    '''


    text = """\
    <p>GEOMAGNETIC ACTIVITY WARNING """ + str.upper(station) + """ <br><br>

    The threshold for """ + str(name) + """ geomagnetic activity has been reached in """ + str(station) + """ in the past hour. <br>
    The real-time ground disturbance has reached a level of """ + str(threshold_value) + """nT 
    at """ + str(val_datetime) + """ UTC. <br><br>

    This is an automatically generated message. <br><br>

    To follow up the event, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/real-time-geomagnetic-disturbances/"> Realtime ground disturbance (deltaH)</a> <br><br>

    and for more information on the thresholds, see <br>
    <a href = "https://site.uit.no/spaceweather/data-and-products/geomagnetic-conditions/summary-and-forecast/"> Geomagnetic summary and forecast </a> <br><br>

    For further support, contact <a href = "mailto:person@gmail.com"> Name </a> (available during normal working hours). <br><br>

    NOSWE - The Norwegian Centre for Space Weather <br><br>

    -----------------------------------------------------------</p>"""

    html = """To unsubscribe or change your subscription, click <a href="link">here</a>"""

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
val_datetime = df_ai_tro.iloc[0][1]

if (tro_deltaH >= 2):
    recipients = []
    prop = 'Hourly Activity Index'
    threshold_value = tro_deltaH
    station = "Tromsø" #df_ai_tro.columns[0]
    name = 'Extreme'

    tro_index = df_ai_tro.index[0]
    val_datetime = df_ai_tro.iloc[0][1]
    val_date = df_ai_tro.iloc[0][2]
    val_time = df_ai_tro.iloc[0][3]


    with open('data/tro_deltaH_extreme.csv', 'a')as g:
        if os.path.getsize('data/tro_deltaH_extreme.csv') <= 0:
            df_ai_tro.to_csv(g, header=g.tell()==0, index =True, sep = " ")
            send_mail_tro()
        else:
            tro_deltaH_2 = pd.read_csv('data/tro_deltaH_extreme.csv', sep = " ")
            tro_deltaH_2['timestamp'] = pd.to_datetime(tro_deltaH_2['timestamp'])
            tro_deltaH_2.set_index('timestamp', inplace=True)
            tro_deltaH_2 = tro_deltaH_2.round(decimals=1)
            tro_deltaH_2['datetime'] = pd.to_datetime(tro_deltaH_2['datetime'])

            minutes_diff = (crt_date - tro_deltaH_2.datetime[-1]).total_seconds() / 60.0
            if minutes_diff >= 1:
                send_mail_tro()
                data = {'timestamp':[tro_index], 'Tromso':[tro_deltaH], 'datetime':[val_datetime], 'Date':[val_date], 'Time':[val_time]}
                tro_deltaH_2 = pd.DataFrame(data)
                tro_deltaH_2.to_csv('data/tro_deltaH_extreme.csv', index = False, sep= " ")
    g.close()