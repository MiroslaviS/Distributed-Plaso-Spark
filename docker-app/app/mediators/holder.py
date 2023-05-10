"""
    Component for holding necessary Plaso parser mediator data for Spark Jobs
"""

class MediatorHolder:
    """ Holder for ParserMediator data """
    def __init__(self, knowledge_base, configuration, collection_filter):
        """
            Initialize mediator object
        Params:
            knowledge_base (KnowledgeBase): Plaso extraction knowledge base configurations
            configuration (ProcessingConfiguration): Configuration for extraction process
            collection_filter (str): Filter for parsers collection
        """
        self.knowledge_base = knowledge_base
        self.configuration = configuration
        self.collection_filter = collection_filter
