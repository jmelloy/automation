import json
import datetime
from decimal import Decimal
from uuid import UUID

class UTCOffset (datetime.tzinfo):
    def __init__(self, offset=0, name="UTC"):
        self.offset = datetime.timedelta(seconds=offset)
        self.name = name or self.__class__.__name__

    def utcoffset(self, dt):
        return self.offset

    def tzname(self, dt):
        return self.name

    def dst(self, dt):
        return None


utc = UTCOffset()

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date) or isinstance(obj, datetime.datetime):
            return obj.isoformat()

        elif isinstance(obj, Decimal):
            return "%.3f" % obj
        elif isinstance(obj, UUID):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)

def my_decoder(obj):
    if '__type__' in obj:
        if obj['__type__'] == '__datetime__':
            dt = datetime.datetime.fromtimestamp(obj['epoc'])
            return dt.replace(tzinfo=utc)
        elif obj['__type__'] == '__date__':
            dt = datetime.date.fromtimestamp(obj['epoc'])
            return dt
    return obj

# Encoder function
def dumps(obj):
    return json.dumps(obj, cls=MyEncoder)

# Decoder function
def loads(obj):
    return json.loads(str(obj), object_hook=my_decoder)
