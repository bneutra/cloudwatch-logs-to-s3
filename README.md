# cloudwatch-logs-s3-lambda

This is python lambda script intended to issue cloudwatch logs insights queries, capture the tabular output, and write it as a .csv to S3.

The function could be triggered e.g. by cloudwatch cron event that includes an event json such as:
```
{
      "report": "daily",
      "log_group": "/aws/ecs/fargate/myapplogs",
      "region": "us-east-1",
      "bucket": "my-bucket",
      "key_path": "daily_insights_reports",
      "query_label": "by_url_status_code",
      "query": "fields @message | stats count(*) as requests, by url, status"
}
```

You could also create a function with the query baked in, and set up a lambda to call that

```
import insights_to_s3

def lambda_handler(event, context):
    main(event, context)
    return {
        'statusCode': 200,
        'body': json.dumps('cloudwatch query done')
    }

def main(event, context):
    event['query'] = "some big query"
    insights_to_s3.main(event, context)

```

With the above parameters ("daily" report), the script will look at the current time in UTC and perform the query on the previous day, 24 hour period (UTC).

You can also specify a date using 'year', 'month', 'day', and 'report': None e.g. to use this script to backfill data by calling:
```
insights_to_s3.main(event, None)
```

