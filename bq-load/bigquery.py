import datetime


def field_mapping(key, cs_bucket, file_type="zone"):
    ret = {}
    if file_type == "zone":

        ret, filename = key.split("/")
        dt = datetime.datetime.strptime(ret, "%Y-%m-%d")
        tld = filename.split(".")[0]

        path = f"gs://{cs_bucket}/{key}"
        datasetId = "zone"
        destination = "%s_%s" % (tld.replace("-", "_"), dt.strftime("%Y%m%d"))

        ret = {"path": path, "datasetId": datasetId, "destination": destination,
               "bucket": cs_bucket, "key": key}

        # This only works with com "raw" files
        if ".raw." in filename:  # COM, ORG, premiumdrop
            ret["fields"] = ["hostname", "type", "value"]
            ret["fieldDelimiter"] = ' '
            ret["ignoreUnknownValues"] = True
            ret["skipLeadingRows"] = 35
            ret["maxBadRecords"] = 10000
        else: # CZDS
            ret["fields"] = ["hostname", "ttl:integer", "in", "type", "value"]
            ret["fieldDelimiter"] = '\t'
            ret["ignoreUnknownValues"] = True

    return ret
