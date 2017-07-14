import bs4
import requests


def table_query(url, keys=None, username=None, password=None):
    """Returns columns in the first table in an html document"""

    if username:
        r = requests.get(url, auth=(username, password))
    else:
        r = requests.get(url)

    if r.status_code != 200:
        raise Exception("Got http error %s", r.status_code)

    soup = bs4.BeautifulSoup(r.content)

    if not soup or not soup.table:
        raise Exception("Could not find table in data")

    column_row = soup.table.tr
    columns = [t.text for t in column_row.findAll("th")]
    if not columns:
        columns = [t.text for t in column_row.findAll("td")]

    if not columns:
        raise Exception("First row does not contain columns")

    if keys:
        key_index = {el: i for i, el in enumerate(columns) if el in keys}

        check = set(keys) - set(key_index)
        if check:
            raise Exception("Columns not found: %s" % ", ".join(check))
    else:
        key_index = {el: i for i, el in enumerate(columns)}

    rows = soup.table.findAll("tr")[1:]
    indices = set(key_index.values())
    for row in rows:
        r = [r.text for i, r in enumerate(row.findAll("td")) if i in indices]
        yield r
