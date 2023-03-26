
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


def get_signature_parser(signature_rdd):
    file_entry, parser_filter_expression = signature_rdd

    # Get File object from file_entry
    file_object = file_entry.GetFileObject()

    # Calculate parser names for file_object
    extractor = extractors.EventExtractor(parser_filter_expression=parser_filter_expression)

    parser_names = extractor._GetSignatureMatchParserNames(file_object)

    return file_entry, parser_names


def json_dumper(obj):
    if not hasattr(obj, '__dict__'):
        return str(obj)

    result = obj.__dict__

    return result


def expand_file_parsers(file_signature):
    file_entry, parsers = file_signature
    file_parsers = []


    # Add filestat parser if not presented in
    # parsers after creating signature
    if 'filestat' not in parsers:
        parsers.append('filestat')

    for parser in parsers:
        file_parsers.append((file_entry.path_spec, parser))

    return file_parsers


def parse(parsing_rdd):
    from plaso.parsers import manager as parsers_manager

    path_spec, parser_name = parsing_rdd
    file_entry = path_resolver.Resolver.OpenFileEntry(path_spec)

    parsers = parsers_manager.ParsersManager.GetParserObjects()
    parser = parsers.get(parser_name)

    file_object = file_entry.GetFileObject()
    mediator = spark_mediator.ParserMediator()
    mediator.SetFileEntry(file_entry)

    from plaso.parsers import interface as parsers_interface

    try:
        if isinstance(parser, parsers_interface.FileEntryParser):
            parser.Parse(mediator)
        elif isinstance(parser, parsers_interface.FileObjectParser):
            parser.Parse(mediator, file_object)
    except:
        return None
    finally:
        file_object._Close()

    return mediator.event_queue
    # return "[" + parser.NAME + "]" + path_spec.location, len(mediator.event_queue), len(mediator.warning_queue), len(mediator.recovery_queue)
