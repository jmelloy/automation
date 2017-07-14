import gspread
import gspread.httpsession
from clumpy.google import service
import logging

logger = logging.getLogger()


def sheet(ws, load=False, update_range=None, update_data=None, gs=None):
    ''' Returns a worksheet from the specified Google Sheet (ws) or optionally
    return all rows (load). Another option is to specify a range of cells to
    update.

    :ws: Worksheet.
    :load: Either `True` or a list of headers to load.
    :update_range:
    :update_data:
    :tracer: Tracing function.
    :gs: Google Service (gspread client).
    '''

    retry_on_error = True if gs is None else False

    try:
        if type(ws) == str:
            name = ws

            gs = gs or service('gspread')

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
                    names = [ws.title for ws in sh.worksheets()]
                    ws = sh.get_worksheet(names.index(wname))

        # update a block of cells
        if update_range:
            cells = ws.range(update_range)
            for j, cell in enumerate(cells):
                if update_data[j] is not None:
                    cell.value = update_data[j]
            ws.update_cells(cells)

        if load:
            rows = ws.get_all_values()
            if load is True: # Load will either be a bool or a column list
                return rows

            # load specific columns by name
            keys = load
            filtered_rows = []

            columns = rows[0]

            key_index = {el:i for i,el in enumerate(columns) if el in keys}

            check = set(keys) - set(key_index)
            if check:
                raise Exception("Columns not found: %s"  % ", ".join(check))

            indices = set(key_index.values())
            for row in rows:
                r = [r for i, r in enumerate(row) if i in indices]
                filtered_rows.append(r)
            return filtered_rows

    except gspread.httpsession.HTTPError as he:
        if not retry_on_error:
            raise
        logger.warning("HTTP %d. Retrying spreadsheet ..." % (he.code,))
        return sheet(ws, load=load, update_range=update_range,
                     update_data=update_data, gs=None)

    except Exception as exc:
        if not retry_on_error:
            raise
        logger.warning("%s. Retrying spreadsheet ..." % (exc.message,))
        return sheet(ws, load=load, update_range=update_range,
                     update_data=update_data, gs=None)

    return ws

