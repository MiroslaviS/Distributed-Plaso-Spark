
class FormatterManager:
    registered_formatters = []

    @classmethod
    def register(cls, formatter):
        cls.registered_formatters.append(formatter)

    @classmethod
    def get_formatter(cls, formatter_name):
        formatter_object = None

        for formatter in cls.registered_formatters:
            if formatter.NAME == formatter_name:
                formatter_object = formatter()

        return formatter_object