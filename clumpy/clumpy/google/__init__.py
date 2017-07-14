from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import httplib2
import os

GOOGLEAPI_AUTH_URL = 'https://www.googleapis.com/auth/'

DEBUG = False
http_path = os.path.join("/tmp/", "responses")
if DEBUG and not os.path.exists(http_path):
    os.makedirs(http_path)

class HttpRecord(httplib2.Http):
    def request(self, uri, *args, **kwargs):

        resp, content = super().request(uri, *args, **kwargs)

        filename = os.path.join(http_path, f'{datetime.datetime.now()}-{uri.replace("/", "-")}-{kwargs.get("method")}')

        with open(filename, "wb") as f:
            f.write(content)
        print("%s written to %s" % (uri, filename))
        return resp, content


def credentials(name, *, scope=None, json=None):
    if json is None:
        json = os.path.join(os.path.dirname(__file__), "secret.json")

    if scope is None:
        scope = GOOGLEAPI_AUTH_URL + name
    elif '://' in scope:
        assert GOOGLEAPI_AUTH_URL in scope, scope
    else:
        scope = GOOGLEAPI_AUTH_URL + scope

    cred = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
    assert cred.invalid is False

    return cred


def service(name, v=None, scope=None, json=None):
    if json is None:
        json = os.path.join(os.path.dirname(__file__), "secret.json")

    v = v or ('v2' if name in ('drive', 'bigquery') else 'v1')
    cred = credentials(name, scope=scope, json=json)

    http = httplib2.Http()
    if DEBUG:
        http = HttpRecord()

    if 'gspread' == name:
        import gspread
        # credentials.refresh(http)  # gspread doesn't auto-refresh access
        return gspread.authorize(cred)

    return build(name, v, http=cred.authorize(http))
