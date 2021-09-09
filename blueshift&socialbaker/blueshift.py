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

creds=json.load(open('/home/centos/Airflow_Dags/credentials/redshift_credentials.json'))
redshift_cred=creds['rs_cred']
conn=psycopg2.connect(dbname=redshift_cred['dbase'],
                        host=redshift_cred['host'],
                        port=redshift_cred['port'],
                        user=redshift_cred['user'],
                        password=redshift_cred['pwd'])
cur=conn.cursor()
conn.autocommit=True

bucket='bsft-customers'
# if date.today()>=date(2020,10,20):
prefix='discoveryplus.com/segment_export/{}'.format(date.today().year)
session = boto3.Session(profile_name='dplus-prod')
# else:
#     prefix='sandbox.discoveryplus.com/segment_export/2020'
#     session = boto3.Session(profile_name='dplus-sandbox')
s3 = session.client('s3')
paginator = s3.get_paginator('list_objects')
cnt=0
for result in paginator.paginate(Bucket = bucket, Prefix=prefix):
    for file in result.get('Contents', []):
        file_name=file.get('Key')
        dest_name='/home/centos/blueshift/blueshift_csvs/'+file_name.split('/')[-1]
        if 'Daily_Email_Signups' in file_name and not Path(dest_name).exists():
            print(file_name.split('/')[-1])
            s3.download_file(bucket, file_name, dest_name)
            client = boto3.client('s3')
            transfer = S3Transfer(client)
            filepath = 'hdeng/blueshift/'
            bucket_name = 'dci-prod-dataanalytics-teams-datastrategy-us-east-1'
            transfer.upload_file(dest_name, bucket_name,filepath+file_name.split('/')[-1])
            cur.execute('''
            copy digital_analytics_dev.blueshift_ap_signup_raw 
            from 's3://dci-prod-dataanalytics-teams-datastrategy-us-east-1/hdeng/blueshift/{0}' 
            iam_role 'arn:aws:iam::246607762912:role/prod-us-analytics-redshift-teams'
            csv
            EMPTYASNULL
            IGNOREHEADER 1;
            '''.format(file_name.split('/')[-1]))
            cur.execute('''
            insert into digital_analytics.blueshift_ap_signup
            select distinct
                count(distinct email),
                date(DATEADD('hour',-5,cast(joined_date as timestamp))) joined_date
            from digital_analytics_dev.blueshift_ap_signup_raw
            where email not like '%@discovery.com%' and 
            email not like '%@motortrend.com%' and
            date(DATEADD('hour',-5,cast(joined_date as timestamp))) = date(dateadd('day',-1,current_date))
            group by 2
            ''')
            if cur.rowcount==0:
                cur.execute('''
                    insert into digital_analytics.blueshift_ap_signup
                    select 
                        0,
                        trunc(dateadd(day,-1,convert_timezone('America/New_York',getdate())))
                    ''')
            cnt+=1
if cnt==0:
    cur.execute('''
                    insert into digital_analytics.blueshift_ap_signup
                    select 
                        0,
                        trunc(dateadd(day,-1,convert_timezone('America/New_York',getdate())))
                    ''')
conn.commit()
cur.close()
conn.close()


