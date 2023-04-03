
from dfvfs.resolver import resolver as path_resolver
from dfvfshadoop.hdfs_file_system import HDFSFileSystem
from dfvfs.resolver.context import Context
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
from pyarrow import fs
import os
from plaso.parsers import mediator as parsers_mediator
from mediators import mediator as spark_mediator

from plaso.engine import extractors
from plaso.engine import worker


def create_file_entry_rdd(path_spec):
    file_entry = path_resolver.Resolver.OpenFileEntry(path_spec)

    return file_entry

def check_if_metadata(file_rdd):
    file_entry, parser_filter = file_rdd
    extraction_worker = worker.EventExtractionWorker(parser_filter_expression=parser_filter)

    return file_entry, extraction_worker._IsMetadataFile(file_entry)


def create_event_sources(path_spec):
    from plaso.containers import event_sources
    from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS

    event_source = event_sources.FileEntryEventSource(
        file_entry_type=TYPE_INDICATOR_HDFS,
        path_spec=path_spec
    )

    return event_source

def create_data_stream_event(path_spec):
    from plaso.containers import events
    data_stream_event = events.EventDataStream()
    data_stream_event.path_spec = path_spec

    return data_stream_event

def get_signature_parser(signature_rdd):
    """

    :param signature_rdd:
    :return: file_entry, parser_names
    """
    file_entry, parser_filter_expression = signature_rdd

    # Get File object from file_entry
    file_object = file_entry.GetFileObject()

    # Calculate parser names for file_object
    extractor = extractors.EventDataExtractor(parser_filter_expression=parser_filter_expression)

    parser_names = extractor._GetSignatureMatchParserNames(file_object)

    return file_entry, parser_names


def expand_file_parsers(file_signature):
    """

    :param file_signature:
    :return: [(path_spec, parser)]
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
        file_parsers.append((file_entry.path_spec, parser))

    return file_parsers


def parse(parsing_rdd):
    """

    :param parsing_rdd:
    :return: (event_queue, warning_queue, recovery_queue) : ([EventData], [EventWarning], [WarningRecovery])
    """
    from plaso.parsers import manager as parsers_manager

    path_spec, parser_name, mediator_data = parsing_rdd
    file_entry = path_resolver.Resolver.OpenFileEntry(path_spec)

    parsers = parsers_manager.ParsersManager.GetParserObjects()
    parser = parsers.get(parser_name)

    file_object = file_entry.GetFileObject()

    mediator = spark_mediator.ParserMediator(mediator_data)
    mediator.SetFileEntry(file_entry)

    from plaso.parsers import interface as parsers_interface

    try:
        if isinstance(parser, parsers_interface.FileEntryParser):
            parser.Parse(mediator)
        elif isinstance(parser, parsers_interface.FileObjectParser):
            parser.Parse(mediator, file_object)
    except:
        # Do nothing on exception. Exceptions are raised when nonsig parser tries to parser
        # not supported file entry.
        pass
    finally:
        file_object._Close()

    return mediator.event_queue, mediator.warning_queue, mediator.recovery_queue
    # return "[" + parser.NAME + "]" + path_spec.location, len(mediator.event_queue), len(mediator.warning_queue), len(mediator.recovery_queue)
