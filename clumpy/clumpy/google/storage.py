import logging

from googleapiclient import discovery, http
from oauth2client.client import GoogleCredentials

logger = logging.getLogger(__name__)

credentials = GoogleCredentials.get_application_default()
service = discovery.build('storage', 'v1', credentials=credentials)


def put_object(file, bucket, key, mimetype="application/octet-stream", check_size=True, gs=None):
    """Uploads a file to google cloud storage.  Retries once automatically if 503 returned"""
    gs = gs or service("storage", scope="devstorage.read_write")

    logger.info("[upload_cloud_store] Uploading %s to %s" % (key, bucket))

    if check_size:
        exists, data = cs_get_object_info(bucket, key)
        # I'd like to use md5 for this but not sure how google's calculating it
        #  u'md5Hash': u'n2j1RoJz0ewlq7khTTCdwg==', ??? maybe base64?
        if exists and data["size"] == check_size:
            logger.info("[upload_cloud_store] Skipping upload for %s" % (key, ))
            return data

    upload = http.MediaIoBaseUpload(file, mimetype=mimetype, resumable=True)

    resp = gs.objects().insert(
        bucket=bucket,
        name=key,
        media_body=upload
    ).execute()

    logger.debug(resp)

    return resp


def get_object(bucket, key, gs=None, tracer=logger.info, filename=None):
    import io
    gs = gs or service("storage", scope="devstorage.read_write")

    # https://cloud.google.com/storage/docs/json_api/v1/objects/get

    # Get Metadata
    req = gs.objects().get(
        bucket=bucket,
        object=key)
    resp = req.execute()

    # Get Payload Data
    req = gs.objects().get_media(
        bucket=bucket,
        object=key)

    # The BytesIO object may be replaced with any io.Base instance.
    if not filename:
        fh = io.BytesIO()
    else:
        fh = open(filename, "wb")

    downloader = http.MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            tracer('Download %d%%.' % int(status.progress() * 100))
        tracer('Download Complete!')

    if filename:
        fh.close()
        fh = open(filename)
    else:
        fh.seek(0)

    return fh


def delete_object(bucket, key, gs=None):
    gs = gs or service("storage", scope="devstorage.read_write")

    logger.info("Deleting %s from %s" % (bucket, key))

    req = gs.objects().delete(
        bucket=bucket,
        object=key
    )

    resp = req.execute()
    # Success returns empty string: ''
    return resp


def get_object_info(bucket, key, gs=None):
    gs = gs or service("storage", scope="devstorage.read_write")
    req = gs.objects().get(
        bucket=bucket,
        object=key)
    try:
        resp = req.execute()
        resp["size"] = int(resp["size"])
        return True, resp
    except http.HttpError as he:
        if "404" in str(he):
            return False, {}
        else:
            logger.error(he)
            raise


def get_bucket_metadata(bucket):
    """Retrieves metadata about the given bucket."""

    # Make a request to buckets.get to retrieve a list of objects in the
    # specified bucket.
    return service.buckets().get(bucket=bucket).execute()


def list_bucket(bucket):
    """Returns a list of metadata of the objects within the given bucket."""

    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return)

    all_objects = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        all_objects.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)
    return all_objects