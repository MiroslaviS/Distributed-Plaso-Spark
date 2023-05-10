""" Component formatting events into JSON output """
from formatters.interface import Formatter
from formatters.manager import FormatterManager
import json


class JsonFormatter(Formatter):
    """ Formatter for formating Events into JSON format"""
    NAME = 'json'

    def format(self, event):
        """ Format the given event as json
        Params:
            event (EventData): Extracted event for formatting
        Returns:
             str: String representing JSON formatted event
        """
        json_event = json.dumps(event,
                                default=self._json_dump,
                                indent=4,
                                sort_keys=True)

        return json_event

    def _json_dump(self, obj):
        """ Convert object to dictionary/string format
        Params:
            obj (Object): Converting object to dict/str
        Returns:
            dict|str: Dictionary representing the object or string representing object
        """
        if not hasattr(obj, '__dict__'):
            return str(obj)

        result = obj.__dict__

        return result


FormatterManager.register(JsonFormatter)
