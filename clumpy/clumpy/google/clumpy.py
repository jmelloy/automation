import oauth2client
import oauth2client.client
import oauth2client.file
import os
import httplib2
import gspread
import gspread.httpsession
from util import parse

import logging
logger = logging.getLogger()


class __ReadOnlyTokenStorage(oauth2client.file.Storage):
    """ Overrides locked_put so credentials cannot be written to disk. """

    def locked_put(self, credentials):
        """ Does nothing. """
        pass

SERVICES = {}


def service(name, secrets='clumpy.json', v=None, token=None, refresh=False, reauthorize=None, scope=None, force=False, 
            tracer=None):
    """
    Return a service object for a Google API; pass as `gs` param to Clumpy API's.
    https://developers.google.com/gmail/api/quickstart/quickstart-python
    To re-authorize pass in True for the `reauthorize` parameter. This will
    cause the function to output a URL to visit which will generate a new
    authorization code. Pass the new code into the `reauthorize` parameter and
    the token file will be overwritten with the new authorization info.
    https://developers.google.com/api-client-library/python/guide/aaa_oauth
    """
    secrets = '{"installed": {"auth_uri": "https://accounts.google.com/o/oauth2/auth", "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "oob"], "client_email": "", "client_id": "315340634847-o93lk11t92lm3svtaf23cpdrodlffei6.apps.googleusercontent.com", "token_uri": "https://accounts.google.com/o/oauth2/token", "client_secret": "Tt8wHOFX_RVYtkqzA1ueH2PC", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": ""}}'
    tracer = logger.info
    token = '{"_module": "oauth2client.client", "token_expiry": "2015-05-08T00:10:34Z", "access_token": "ya29.bAEIcHAwdj1EyaKOL_QWnRgtp9Ccj5gPyG1U1s7-ClIeJ0jnsem2DbGrMybI3yu4U642KFB4TUX7Jg", "token_uri": "https://accounts.google.com/o/oauth2/token", "invalid": false, "token_response": {"access_token": "ya29.bAEIcHAwdj1EyaKOL_QWnRgtp9Ccj5gPyG1U1s7-ClIeJ0jnsem2DbGrMybI3yu4U642KFB4TUX7Jg", "token_type": "Bearer", "expires_in": 3600}, "client_id": "315340634847-o93lk11t92lm3svtaf23cpdrodlffei6.apps.googleusercontent.com", "id_token": null, "client_secret": "Tt8wHOFX_RVYtkqzA1ueH2PC", "revoke_uri": "https://accounts.google.com/o/oauth2/revoke", "_class": "OAuth2Credentials", "refresh_token": "1/CT11_YaQs_nhgs-UXr6xDb3gitxUjHvUDId1hUeumUs", "user_agent": null}'
    GOOGLEAPI_AUTH_URL = 'https://www.googleapis.com/auth/'

    global SERVICES
    svckey = "%s:%s" % (name, token)
    if svckey in SERVICES and not refresh:
        return SERVICES[svckey]

    v = v or ('v2' if name == 'drive' else 'v1')

    if token is None:
        if 'gmail' == name:
            token = 'clumpy@donuts.co.token'
        else:
            token = 'clumpy.%s.token' % (name,)

    # currently for tracer/information purposes only
    if '.token' in token:
        __title = token[:token.rfind('.token')]
    else:
        __title = token

    if scope is None:
        scope = GOOGLEAPI_AUTH_URL + name
        if 'gmail' == name:
            scope += '.modify'
        elif 'gspread' == name:
            scope = 'https://spreadsheets.google.com/feeds'
    elif '://' in scope:
        if not force:
            assert GOOGLEAPI_AUTH_URL in scope, scope
    else:
        scope = GOOGLEAPI_AUTH_URL + scope

    if '/' not in secrets:
        secrets = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), secrets)

    if '/' not in token:
        token = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), token)

    # if os.path.exists(token + '.token.gpg'):
    #     token += '.token.gpg'
    # elif os.path.exists(token + '.token'):
    #     token += '.token'

    http = httplib2.Http()

    if not reauthorize:  # and not os.path.exists(token):  # load directly as JSON
        credentials = oauth2client.client.Credentials.new_from_json(token)
    elif token[-4:] == '.gpg':  # encrypted token
        assert not reauthorize  # cannot reauthorize with encrypted token
        lines = []
        run('gpg --batch -qd "%s"' % (token,), echo=lines, tracer=tracer)
        credentials = oauth2client.client.Credentials.new_from_json(''.join(lines))
    elif reauthorize:  # create/refresh token data
        import json
        storage = oauth2client.file.Storage(token)

        secrets = json.load(open(secrets))
        client_id = secrets[u'installed'][u'client_id']
        client_secret = secrets[u'installed'][u'client_secret']

        flow = oauth2client.client.OAuth2WebServerFlow(
            client_id=client_id, client_secret=client_secret, scope=scope,
            redirect_uri='http://localhost')

        if type(reauthorize) not in (str,):
            return flow.step1_get_authorize_url()

        code = parse.flat(reauthorize)
        credentials = flow.step2_exchange(code=code, http=http)
        storage.put(credentials)
    else:
        storage = __ReadOnlyTokenStorage(token)
        credentials = storage.get()

    assert credentials
    assert credentials.invalid is False

    if 'gspread' == name:
        import gspread
        credentials.refresh(http)  # gspread doesn't auto-refresh access
        SERVICES[svckey] = gspread.authorize(credentials)
    else:
        from apiclient.discovery import build
        SERVICES[svckey] = build(name, v, http=credentials.authorize(http))

    SERVICES[svckey].__title = __title  # used by title()

    return SERVICES[svckey]


def sheet(ws, load=False, refresh=False, update_range=None,
          update_data=None, tracer=None, gs=None):
    """ Returns a worksheet from the specified Google Sheet (ws) or optionally
    return all rows (load). Another option is to specify a range of cells to
    update.
    :ws: Worksheet.
    :load: Either `True` or a list of headers to load.
    :refresh: Refreshes the Gspread Client connection.
    :update_range:
    :update_data:
    :tracer: Tracing function.
    :gs: Google Service (gspread client).
    """

    tracer = logger.info

    retry_on_error = True if gs is None else False
    assert not (gs and refresh)  # cannot refresh connection w/ explicit service object

    try:
        if type(ws) in (str, unicode):
            name = ws

            gs = gs or service('gspread', refresh=refresh)
            print('got service back and about to try opening a sheet')
            if 'https://' == name[:8]:
                sh = gs.open_by_url(name)
                ws = sh.sheet1
            # if no worksheet name is specified, then just get the first one
            elif '/' not in name:
                sh = gs.open(name)
                ws = sh.sheet1
            else:
                sname, wname = name.split('/')
                sh = gs.open(sname)

                # find the worksheet index by name
                try:
                    ws = sh.worksheet(wname)
                except gspread.WorksheetNotFound:
                    names = parse.index([ws.title for ws in sh.worksheets()])
                    wi = parse.fuzzy_match(wname, names, leftmost=True)
                    if wi is None:
                        raise
                    ws = sh.get_worksheet(wi)

        # update a block of cells
        if update_range:
            cells = ws.range(update_range)
            for j, cell in enumerate(cells):
                if update_data[j] is not None:
                    cell.value = update_data[j]
            ws.update_cells(cells)

        if load:
            rows = ws.get_all_values()
            if load is True:
                return rows

            # load specific columns by name
            keys = load
            prows = []
            hrow = None
            for row in rows:
                if hrow is None:
                    hrow = parse.index(row)
                    continue
                prows.append(parse.fuzzy_get_multi(row, keys, hrow))
            return prows

    except gspread.httpsession.HTTPError as he:
        if refresh or not retry_on_error:
            raise
        tracer("WARNING: HTTP %d. Retrying spreadsheet ..." % (he.code,))
        return sheet(ws, refresh=True, load=load, update_range=update_range,
                     update_data=update_data, tracer=tracer, gs=None)

    except Exception as exc:
        if refresh or not retry_on_error:
            raise
        tracer("WARNING: %s. Retrying spreadsheet ..." % (exc,))
        return sheet(ws, refresh=True, load=load, update_range=update_range,
                     update_data=update_data, tracer=tracer, gs=None)

    return ws

# for row in sheet(
#         'https://docs.google.com/a/donuts.co/spreadsheets/d/1yibYxkc0nyf5zQ7-_vm85GAwj0P-Oj11BFE7XzRvrcs/edit?usp=sharing',
#         load=True
#         ):
#     print row
