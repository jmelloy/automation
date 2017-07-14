Lambda tasks to do various automation jobs within the donuts infrastructure.

This uses serverless and python 3.6:
 > npm install -g serverless
 > mkvirtualenv aws-automation -p `which python3`
 > pip install -r requirements.txt

Each individual service should manage their own dependencies with a requirements.txt file.
If you don't install them globally it's easier to find problems as you test.

Services:

bq-load:
    Loads data from Google Cloud Storage into BigQuery

http-downloader:
    Downloads URLs and uploads to S3

s3-to-cs:
    Moves data from s3 to cloud storage

scale:
    Manages how many concurrent lambda tasks should be run for everything else