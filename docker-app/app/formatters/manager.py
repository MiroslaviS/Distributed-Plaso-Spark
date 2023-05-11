""" Component for registering and managing all available formatters  """


class FormatterManager:
    """ Manager for formatters """
    registered_formatters = []

    @classmethod
    def register(cls, formatter):
        """ Register formatter class
        Params:
            Formatter: Class that needs to be register for usage
        """
        cls.registered_formatters.append(formatter)

    @classmethod
    def get_formatter(cls, formatter_name):
        """ Get formatter class and create formatter object
        Params:
            formatter_name (str): Name of formatter class
        Returns:
            Formatter: Initialized requested formatter object
        """
        formatter_object = None

        for formatter in cls.registered_formatters:
            if formatter.NAME == formatter_name:
                formatter_object = formatter()

        return formatter_object
