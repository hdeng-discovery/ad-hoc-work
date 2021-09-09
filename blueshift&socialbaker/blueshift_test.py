# import airflow
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.sensors.sql_sensor import SqlSensor

from datetime import date, timedelta, datetime

import os
import time
import pandas as pd
import numpy as np
import boto3
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path
import subprocess
import json
from boto3.s3.transfer import S3Transfer
from pytz import timezone

creds=json.load(open('/home/centos/Airflow_Dags/credentials/redshift_credentials.json'))
redshift_cred=creds['rs_cred']
conn1=psycopg2.connect(dbname=redshift_cred['dbase'],
                        host=redshift_cred['host'],
                        port=redshift_cred['port'],
                        user=redshift_cred['user'],
                        password=redshift_cred['pwd'])
cur=conn1.cursor()
conn1.autocommit=True

engine_url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(redshift_cred['user'],
                                                        redshift_cred['pwd'],
                                                        redshift_cred['host'],
                                                        redshift_cred['port'],
                                                        redshift_cred['dbase'])
conn2 = create_engine(engine_url).connect()

session = boto3.Session(profile_name='dplus-prod')
s3 = session.client('s3')
bucket='bsft-customers'
if len(str(date.today().day))==1:
    prefix_1='discoveryplus.com/events/{}/{}/{}/'.format(date.today().year,date.today().month,'0'+str(date.today().day))
    prefix_2='discoveryplus.com/events/{}/{}/{}/'.format(date.today().year,date.today().month,'0'+str(date.today().day-1))
else:
    prefix_1='discoveryplus.com/events/{}/{}/{}/'.format(date.today().year,date.today().month,date.today().day)
    prefix_2='discoveryplus.com/events/{}/{}/{}/'.format(date.today().year,date.today().month,date.today().day-1)
df= pd.DataFrame()
current_timestamp = datetime.now(timezone('EST')) 

paginator = s3.get_paginator('list_objects')
if datetime.today().hour!=0:
    for result in paginator.paginate(Bucket = bucket, Prefix=prefix_1):
        for file in result.get('Contents', []):
            file_name=file.get('Key')
            if (int(file_name.split('.')[-3].split('-')[-2][-6:]) >= 14 & \
                (int(file_name.split('.')[-3].split('-')[-2][-6:]) < 15)):
                dest_name='/home/centos/blueshift/blueshift_hourly_csvs/'+file_name.split('/')[-1]
                s3.download_file(bucket, file_name, dest_name)
                # print(dest_name)
                chunks=pd.read_json(dest_name,compression='gzip',lines = True, chunksize=1)
                for chunk in chunks:
                    print(chunk.event.head(10))
                    df = pd.concat([df,chunk[chunk.event=='announcement - email submission']],\
                                   sort=False,ignore_index=True)
else:
    for result in paginator.paginate(Bucket = bucket, Prefix=prefix_2):
        for file in result.get('Contents', []):
            file_name=file.get('Key')
            if int(file_name.split('.')[-3].split('-')[-2][-6:]) >= 230000:
                dest_name='/home/centos/blueshift/blueshift_hourly_csvs/'+file_name.split('/')[-1]
                s3.download_file(bucket, file_name, dest_name)
                chunks=pd.read_json(dest_name,compression='gzip',lines = True, chunksize=1)
                for chunk in chunks:
                    df = pd.concat([df,chunk[chunk.event=='announcement - email submission']],\
                                   sort=False,ignore_index=True)

# if df.shape[0]>0:
#     print(df.shape[0])
#     df=df.groupby(['email']).timestamp.min().to_frame().reset_index()
#     red=pd.read_sql('select distinct email from digital_analytics_dev.blueshift_ap_signup_raw',conn2)
#     df=df[~df.email.isin(red.email.tolist())][['email','timestamp']]
#     df['unknown']=''
#     df['first_name']=''
#     df.rename(columns={"timestamp": "joined_date"},inplace=True)
#     df[['unknown','email','first_name','joined_date']].to_sql('blueshift_ap_signup_raw', conn2, index = False ,if_exists = 'append', \
#           schema = 'digital_analytics_dev', chunksize = 1000)
# query1 = '''
#         insert into digital_analytics.blueshift_ap_signup_hourly
#         select 
#             to_timestamp('{0}', 'yyyy-mm-dd hh24:mi:ss'),
#             count(distinct email) daily_cumulative_email_count
#         from digital_analytics_dev.blueshift_ap_signup_raw
#         where email not like '%@discovery.com%' and 
#         email not like '%@motortrend.com%' and
#         date(DATEADD('hour',-5,cast(joined_date as timestamp)))=trunc(convert_timezone('America/New_York',getdate()))
#         group by 1
#         '''.format(current_timestamp)
# cur.execute(query1)
# if cur.rowcount==0:
#     query2 = '''
#         insert into digital_analytics.blueshift_ap_signup_hourly
#         values (to_timestamp('{0}', 'yyyy-mm-dd hh24:mi:ss'), 0)
#         '''.format(current_timestamp)
#     cur.execute(query2)
# if len(os.listdir('/home/centos/blueshift/blueshift_hourly_csvs/'))>0:
#     subprocess.run("cd /home/centos/blueshift/blueshift_hourly_csvs && rm -r *", shell=True)
# conn2.close()
# cur.close()
# conn1.close()

