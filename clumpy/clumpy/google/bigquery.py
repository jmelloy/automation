import logging
import time

from clumpy.google import service
from clumpy.google.exceptions import BigQueryException

logger = logging.getLogger()

bq_service = service("bigquery", v="v2")
default_project_id = "whois-980"


def get_table_schema(table_name, datasetId, service=bq_service, projectId=default_project_id):
    response = service.tables().get(
        projectId=projectId,
        datasetId=datasetId,
        tableId=table_name
    ).execute()

    return response["schema"]["fields"]


def get_list_of_tables(datasetId, service=bq_service, projectId=default_project_id):

    response = service.tables().list(
        projectId=projectId,
        datasetId=datasetId,

    ).execute()

    while response and response.get("tables", []):
        for t in response["tables"]:
            yield t["tableReference"]["tableId"]

        pageToken = response.get("nextPageToken", None)
        response = None
        if pageToken:
            response = service.tables().list(
                projectId=projectId,
                datasetId=datasetId,
                pageToken=pageToken
            ).execute()


def delete_table(table_name, service=bq_service, projectId=default_project_id, datasetId=None):
    response = service.tables().delete(
        projectId=projectId,
        datasetId=datasetId,
        tableId=table_name
    ).execute()

    return response


def query_sync(sql, datasetId=None, service=bq_service, projectId=default_project_id, timeout=60000, num_retries=3,
               return_dict=True, header=False, dryrun=False, useQueryCache=True, destination=None):
    """Synchronously execute a query against BigQuery and return the results as a rowset, similar to data.query"""
    query_data = {
        'query': sql,
        'timeoutMs': timeout,
        'dryRun': dryrun,
        'useQueryCache': useQueryCache,
        "allowLargeResults": True,
    }

    if destination:
        query_data["destinationTable"] = {
            "datasetId": datasetId,
            "projectId": projectId,
            "tableId": destination
        }

    response = service.jobs().query(
        projectId=projectId,
        body=query_data).execute(num_retries=num_retries)

    if not response:
        raise Exception('got no response from query')

    total_bytes = int(response.get("totalBytesProcessed", 0))
    total_cost = total_bytes / pow(2.0, 40) * 5
    logger.info("{} GB (${})".format(total_bytes / 1024.0 / 1024.0 / 1024.0, total_cost))

    if response.get(u"cacheHit"):
        logger.info("Returning cached data")

    if dryrun:
        response[u"totalCost"] = total_cost
        yield response
        return

    jobId = response["jobReference"]["jobId"]
    logger.info("jobId: {}".format(jobId))

    rows = response.get("totalRows")
    if rows is None:
        raise Exception("Got malformed schema response")

    logger.info("Got {} total rows".format(rows))

    columns = [n["name"] for n in response["schema"]["fields"]]
    if header and not return_dict:
        yield columns

    if response.get("totalRows", 0) == 0:
        return

    i = 0
    while response and response.get("rows"):
        logger.info("{} rows in this batch".format(len(response["rows"])))
        for i, row in enumerate(response["rows"]):
            vals = [f["v"] for f in row["f"]]
            if return_dict:
                yield dict(zip(columns, vals))
            else:
                yield vals

        token = response.get("pageToken")
        response = None
        if token:
            logger.info("token: {}".format(token))
            response = response.jobs().getQueryResults(projectId=projectId, jobId=jobId, pageToken=token).execute()


def query_async(sql, destinationTable, destinationDataSetId, projectId=default_project_id, service=bq_service, **kwargs):
    """Asynchronously run a query against BigQuery and return the job ID"""
    body = {
        "configuration": {
            "query": {
                "query": sql,
                "destinationTable": {
                    "projectId": projectId,
                    "datasetId": destinationDataSetId,
                    "tableId": destinationTable
                },
                "allowLargeResults": True,
                "useQueryCache": True
            }
        }
    }

    body["configuration"]["query"].update(kwargs)

    response = service.jobs().insert(
        projectId=projectId,
        body=body
    ).execute()

    return response


def query_async_with_results(sql, projectId=default_project_id, service=bq_service, return_dict=True, header=False, **kwargs):
    """Asynchronously run a query against BigQuery, wait for job to finish, generate results."""
    body = {
        "configuration": {
            "query": {
                "query": sql,
                "useQueryCache": True
            }
        }
    }

    body["configuration"]["query"].update(kwargs)

    response = service.jobs().insert(
        projectId=projectId,
        body=body
    ).execute()

    # wait for job to complete.
    job_state = response.get("status", {}).get("state", '')
    jobId = response.get("jobReference", {}).get("jobId", '')

    while job_state == "RUNNING":
        time.sleep(1)
        response = job_status(jobId, projectId, service)
        job_state = response["status"]["state"]

    if job_state == "DONE":
        response = service.jobs().getQueryResults(
            projectId=projectId,
            jobId=jobId,
            startIndex=0).execute()

    if not response:
        raise Exception("Got no query response")

    total_bytes = int(response.get("totalBytesProcessed", 0))
    total_cost = total_bytes / pow(2.0, 40) * 5
    logger.info("{} GB (${})".format(total_bytes / 1024.0 / 1024.0 / 1024.0, total_cost))
    if response.get(u"cacheHit"):
        logger.info("Returning cached data")

    logger.info("jobId: {}".format(jobId))

    rows = response.get("totalRows")
    if rows is None:
        raise Exception("Got malformed schema response")

    logger.info("Got {} total rows".format(rows))

    columns = [n["name"] for n in response["schema"]["fields"]]
    if header and not return_dict:
        yield columns

    if response.get("totalRows", 0) == 0:
        return

    i = 0
    while response and response.get("rows"):
        logger.info("{} rows in this batch".format(len(response["rows"])))
        for i, row in enumerate(response["rows"]):
            vals = [f["v"] for f in row["f"]]
            if return_dict:
                yield dict(zip(columns, vals))
            else:
                yield vals

        token = response.get("pageToken")
        response = None
        if token:
            logger.info("token: {}".format(token))
            response = service.jobs().getQueryResults(projectId=projectId, jobId=jobId, pageToken=token).execute()


def job_status(jobId, projectId=default_project_id, service=bq_service):
    """ Checks the status of a bigquery job, such as returned by bq_export, load, or asynchronous query """
    response = service.jobs().get(projectId=projectId, jobId=jobId).execute()
    return response


def export(table, datasetId, destination, projectId=default_project_id, service=bq_service):
    """ Exports a table from big query to Google cloud store
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.extract

    :param table:
    :param destination: the fully qualified google cloudstore URI: gs://facebook_lists/blah.csv.gz
    :param projectId: The big query project (whois-980)
    :param datasetId: BigQuery dataset ID (whois2015)
    :param gs: a clumpy service account
    :param tracer:
    :return: job configuration response:
    """
    body = {
        "configuration": {
            "extract": {
                "sourceTable": {
                    "projectId": projectId,
                    "datasetId": datasetId,
                    "tableId": table
                },
                "destinationUris": [
                    destination
                ],
                "destinationFormat": "CSV",
                "compression": "GZIP"
            }
        }
    }

    return service.jobs().insert(projectId=projectId, body=body).execute()


def load(file_path, destination_table, destination_dataset, project_id=default_project_id, fields=[], service=bq_service, **kwargs):
    """Loads a table from cloud storage into BigQuery

    Allowed parameters here:
    https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load

     :param file_path: The fully qualified google cloud storage name (gs://bucket/blah). Wildcards and lists allowed
     :param destination_table: table to put the data into
     :param destination_dataset: destination dataset
     :param project_id: Project ID for the data
     :param fields: Array of field names
     :param service: Google service to use
     :param kwargs: Anything else to be passed through to google. (skipLeadingRows, writeDisposition, etc)

     :returns job_id: The google Job ID
    """
    if type(file_path) != list:
        file_path = [file_path]

    properties = service._resourceDesc["schemas"]["JobConfigurationLoad"]["properties"]
    for k in kwargs:
        if k not in properties:
            raise BigQueryException("Got unexpected argument %s, options: %s" % (k, ",".join(properties)))

    field_names = []
    for f in fields:
        r = f.split(":")
        field_type = 'STRING'
        if len(r) > 1:
            field_type = r[1]
        field_names.append({"name": r[0], "type": field_type})

    # https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load

    body = {
        "configuration": {  # [Required] Describes the job configuration.
            "load": {  # [Pick one] Configures a load job.
                "destinationTable": {  # [Required] The destination table to load the data into.
                    "projectId": project_id,
                    # [Required] The ID of the project containing this table.
                    "tableId": destination_table,
                    # [Required] The ID of the table. The ID must contain only letters (a-z, A-Z),
                    # numbers (0-9), or underscores (_). The maximum length is 1,024 characters.
                    "datasetId": destination_dataset,
                    # [Required] The ID of the dataset containing this table.
                },
                "sourceUris":
                    # [Required] The list of fully-qualified URIs that point to your
                    # data in Google Cloud Storage. Each URI can contain one '*' wildcard character
                    # and it must come after the 'bucket' name.
                    file_path,
                "schema": {
                    # [Optional] The schema for the destination table. The schema can be omitted if
                    # the destination table already exists or if the schema can be inferred from the loaded data.
                    "fields": field_names,
                },
            },
        },
    }

    body["configuration"]["load"].update(kwargs)

    response = service.jobs().insert(
        projectId=project_id,
        body=body
    ).execute()

    logger.info("[load] Started job %s" % response["jobReference"]["jobId"])

    return response["jobReference"]["jobId"]

