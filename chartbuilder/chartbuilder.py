import json
import boto3
from datetime import datetime, timedelta
import pandas as pd



START_DATE = datetime.now() 
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client("s3")

def get_days(days_to_process):
    date_list = [] 
    for n in range(days_to_process):
        date_list.append(datetime.strftime((START_DATE - timedelta(n)), '%m-%d-%Y'))
    return date_list

def fmt_response(status_code, body):  
    return {
        'statusCode': status_code,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type,Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Origin": "*", 
            "Access-Control-Allow-Methods": "OPTIONS,POST" 
        },
        'body':  body
    }

def process_city(df, city):
    print(city)

def process_global(df):
    print('global')
    
def handler(event, context):
    print(pd.__version__)

    # Aggregate N days of stats as df
    days = get_days(2)
    
    print('Processing days ... ' + str(days))
    
    df = pd.DataFrame()
    for date in days:
        object_key = date + "/aggregated-daily.json"
        file_content = s3_client.get_object(
            Bucket='daily-statistics', Key=object_key)
        df = pd.concat([df, pd.read_json(file_content['Body'])])
        print(df)
        
        

    # loop through cities
    dyanamodb = boto3.resource('dynamodb')
    all_stations = dyanamodb.Table('Stations')
    stations = all_stations.scan()
    cities = [] # Get cities to process from DynamoDB Stations table.
    cities.extend(stations.get("Items", []))

    for city in cities:
        print('Processing: ' + city['City'].replace(' ', '') + ' ...')

        # Pass df and city to city builder
        # process_city(df, city)
        
    print('Processing: global ...')     
    #process_global(df)    
    return fmt_response(200, 'processed')


handler('','')