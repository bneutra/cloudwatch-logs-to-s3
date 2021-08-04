# lambda that performs a daily insights query and writes tabular results
# to s3 as a csv file
import json
import boto3
from datetime import datetime, timedelta
import sys
import time

# We poll for a completed query, then write to s3. Just set a 15m/max timeout on the lambda.
# Unless you're running thousands of insights queries a day, it's not expensive
POLL_S = 1


# handler calls main to provide ability to run standalone or in lambda
def lambda_handler(event, context):
    main(event, context)
    return {
        'statusCode': 200,
        'body': json.dumps('cloudwatch query done')
    }


def write_to_s3(region, bucket, key, data):
    s3_client = boto3.client('s3', region)
    # upload to s3
    print(f'writing to s3 {bucket}/{key}')
    s3_client.put_object(Body=data, Bucket=bucket, Key=key, ContentType='text/plain')


def main(event, context):
    report = event.get('report')
    hours = 24
    if report == 'daily':
        # run this lambda daily (i.e. next day, UTC) to report on the previous day (UTC)
        yesterday = datetime.utcnow().date() - timedelta(hours=24)
        year =  yesterday.strftime("%Y")
        month =  yesterday.strftime("%m")
        day =  yesterday.strftime("%d")
    # TODO: implement report == 'monthly' i.e. set the start and end datetime for a
    # given/provided event['month']
    else:
        # user specified, e.g. for backfilling days
        day = event.get('day')
        year = event['year']
        month = event['month']
    region = event['region']
    bucket = event['bucket']
    key_path = event['key_path']
    query_label = event['query_label']
    query = event['query']

    # query start range is based on beginning of the day, UTC
    # so we might run this at 10pm ET on Apr 1, to get the data for day Apr 1 UTC
    start_date = f'{year}-{month}-{day}'
    start_time = '00:00:00.000+00:00' # UTC
   
    log_group = event.get('log_group')
    dt = datetime.fromisoformat(start_date + 'T' + start_time) 
    print(f'{start_date}: {log_group} query:\n{query}')
    key = f'{key_path}/{year}/{month}/{start_date}_utc_day_{query_label}.txt'
    data = do_query(region, log_group, dt, hours, query)
    write_to_s3(region, bucket, key, data)


def do_query(region, log_group, dt, hours, query):
    client = boto3.client('logs', region)
    start_query_response = client.start_query(
        logGroupName=log_group,
        startTime=int(dt.timestamp()),
        endTime=int((dt + timedelta(hours=hours)).timestamp()),
        queryString=query,
    )
    
    query_id = start_query_response['queryId']
    response = None

    while response == None or response['status'] == 'Running':
        print('Waiting for query to complete ...')
        time.sleep(POLL_S)
        response = client.get_query_results(queryId=query_id)
        
    print(response.get('statistics'))
    results = response.get('results')
    if not results:
        print("No results returned. exit")
        return False

    # Note: this assumes that the first record has all the fields!
    # use functions like coalesce() to ensure that no field is missing
    # i.e. every record has all expected fields
    header = ''
    header_len = len(results[0])
    for field in results[0]:
        header+= field.get('field') + ','
    header = header.rstrip(',')
    print(header)
    final_data = header  
    for arr in results:
        line = ''
        if len(arr) != header_len:
            print(f'WARNING: expected {header_len} fields, but found {len(arr)}')
        for field in arr:
            line += field.get('value') + ','
        line = line.rstrip(',')
        final_data = final_data + '\n' + line

    return final_data


