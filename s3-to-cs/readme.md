Need serverless-sqs-alarms-plugin and external s3 plugin:

    npm install serverless-sqs-alarms-plugin 
    npm install serverless-external-s3-event

To deploy:

    ./deploy.sh


https://sbstjn.com/serverless-sqs-worker-with-aws-lambda.html

This builds a scalable job that moves data from s3 to cloud storage.  It is broken into several components.

marshaller
    - Listens for S3 notifications and adds key/bucket to SQS queue

configure_scale
    - Listens for notifications from sqs queue and updates config dyamodb table with scale number
   
scale
    - checks the 'scale' argument in the dynamodb table, and fires off X workers
    - Called by CloudWatch every 5 minutes
    
worker
    - Reads from queue, picks up file from S3, moves into cloud watch, then invokes BQ Load
    - If errors, writes to DynamoDB table
    
load_bigquery
    - Given a filepath, calls table load on bigquery, places results into an sqs job queue

job_check
    - pops message off job queue, and checks google if the job is done.
        - if done & key/bucket are present, delete file from cloud storage
        - if error, log to dynamo table
        - else put back in queue
    - called by CW every 5 minutes, works until there are 10 seconds left
