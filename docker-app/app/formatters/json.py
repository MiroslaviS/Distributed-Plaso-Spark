
from formatters.interface import Formatter
import json


class JsonFormatter(Formatter):
    def format(self, event):
        json_event = json.dumps(event,
                                default=self._json_dump,
                                indent=4,
                                sort_keys=True)

        return json_event

    def _json_dump(self, obj):
        if not hasattr(obj, '__dict__'):
            return str(obj)

        result = obj.__dict__

        return result

