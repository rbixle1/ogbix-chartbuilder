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

def process_dataframe(df):
    print(df)
    df['song']= df['song'].str.title()
    df['artist']= df['artist'].str.title()
    local_df = df.groupby(['key', 'song', 'artist']).sum(['count']).reset_index()
    local_df = local_df.sort_values(by=['count'], ascending=False).head(100)

    print(local_df)

    
def handler(event, context):
    print(pd.__version__)

    # Aggregate N days of stats as df
    days = get_days(5)
    
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
        city_name = city['City'].replace(' ', '')
        print('Processing: ' + city_name + ' ...')

        # Pass df and city to city builder
        process_dataframe(df.loc[df['city'] == city_name])
        
    print('Processing: global ...')     
    process_dataframe(df)    
    return fmt_response(200, 'processed')


handler('','')