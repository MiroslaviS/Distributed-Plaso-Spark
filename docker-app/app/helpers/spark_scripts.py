""" Script used in Spark jobs during extraction """

from dfvfs.resolver import resolver as path_resolver
from mediators import mediator as spark_mediator

from plaso.engine import extractors
from plaso.engine import worker


def create_file_entry_rdd(path_spec):
    """ Creates HDFSFileEntry objects from HDFSPathSpec
    Params:
        path_spec (HDFSPathSpec): HDFS path specification
    Returns:
        HDFSFileEntry: HDFS File Entry created from path specification
    """
    file_entry = path_resolver.Resolver.OpenFileEntry(path_spec)

    return file_entry


def create_event_sources(path_spec):
    """ Creates event source from path specification
    Params:
        path_spec (HDFSPathSpec): HDFS path specification for file
    Returns:
        FileEntryEventSource: Created event source from path specification
    """
    from plaso.containers import event_sources
    from dfvfs.lib.definitions import TYPE_INDICATOR_HDFS

    event_source = event_sources.FileEntryEventSource(
        file_entry_type=TYPE_INDICATOR_HDFS,
        path_spec=path_spec
    )

    return event_source


def create_data_stream_event(path_spec):
    """ Creates data streams event from path specification
    Params:
        path_spec (HDFSPathSpec): HDFS path specification for file
    Returns:
        DataStreamEventSource: Data stream from path specification
    """
    from plaso.containers import events
    data_stream_event = events.EventDataStream()
    data_stream_event.path_spec = path_spec

    return data_stream_event


def get_signature_parser(signature_rdd, broadcast_config_parser):
    """
        Determines possible parsers for file by calculated file signature
    Params:
        signature_rdd (HDFSFileEntry): File Entry for HDFS file
        broadcast_config_parser (ProcessingConfiguration): Parsers configurations
    Returns:
        (FileEntry, [str]): List of parser names for FileEntry
    """
    file_entry = signature_rdd
    parser_filter_expression = broadcast_config_parser.value

    # Get File object from file_entry
    file_object = file_entry.GetFileObject()

    # Calculate parser names for file_object
    extractor = extractors.EventDataExtractor(parser_filter_expression=parser_filter_expression)

    parser_names = extractor._GetSignatureMatchParserNames(file_object)

    return file_entry, parser_names


def expand_file_parsers(file_signature):
    """
        Creates tuples of the same file entry but all it's parsers.
        Add filestat parser for parsing file statistics if not present
        in parser list
    Params:
        file_signature ((FileEntry, [str])): File entry with all possible parsers
    Returns:
        [(parser, HDFSPathSpec)]: List of all parsers for file entry
    """
    file_entry, parsers = file_signature
    file_parsers = []

    if isinstance(parsers, set):
        parsers = list(parsers)

    # Add filestat parser if not presented in
    # parsers after creating signature
    if 'filestat' not in parsers:
        parsers.append('filestat')

    for parser in parsers:
        file_parsers.append((parser, file_entry.path_spec))

    return file_parsers


def parse(parsing_rdd, mediator_data_broadcast):
    """
        Parse file entry with given parser. Creates Parser object
        and Spark ParserMediator and starts the extraction on given
        file with Parser
    Params:
        parsing_rdd ((str, HDFSPathSpec)): Name of parser which needs to be used for file specified by path specification
        mediator_data_broadcast (MediatorHolder): Data for creating ParserMediator for extraction process
    Returns:
        [EventData]: List of extracted event data
    """
    from plaso.parsers import manager as parsers_manager

    parser_name, path_spec = parsing_rdd
    file_entry = path_resolver.Resolver.OpenFileEntry(path_spec)

    parsers = parsers_manager.ParsersManager.GetParserObjects()
    parser = parsers.get(parser_name)

    file_object = None
    mediator_data = mediator_data_broadcast.value
    mediator = spark_mediator.ParserMediator(mediator_data)
    mediator.SetFileEntry(file_entry)

    from plaso.parsers import interface as parsers_interface

    try:
        if isinstance(parser, parsers_interface.FileEntryParser):
            parser.Parse(mediator)
        elif isinstance(parser, parsers_interface.FileObjectParser):
            file_object = file_entry.GetFileObject()

            parser.Parse(mediator, file_object)
    except:
        # Do nothing on exception. Exceptions are raised when nonsig parser tries to parser
        # not supported file entry.
        pass
    finally:
        if file_object:
            file_object._Close()

    events = []
    events.extend(mediator.event_queue)
    events.extend(mediator.warning_queue)
    events.extend(mediator.recovery_queue)
    print("Parsing File object" + path_spec.location + " EVENTS: " + str(len(events)))

    return events
