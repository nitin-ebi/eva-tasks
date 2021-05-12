import os.path
import subprocess

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)


def export_data(mongo_source, accession_export_directory, mongoexport_args=None):
    mongoexport_args = " ".join([f"--{arg} {val}"
                                 for arg, val in mongoexport_args.items()]) if mongoexport_args else ""
    mongoexport_command = f"mongoexport --uri {mongo_source.uri_with_db_name}  --out {accession_export_directory} {mongoexport_args}" + \
                          mongo_source._get_optional_secrets_file_stdin()
    try:
        run_command_with_output("mongoexport", mongoexport_command, log_error_stream_to_output=True)
    except subprocess.CalledProcessError as ex:
        raise Exception("mongoexport failed! HINT: Did you forget to provide a secrets file for authentication?")


def import_data(mongo_dest, coll_file_loc, mongoimport_args=None):
    mongoimport_args = " ".join([f"--{arg} {val}"
                                 for arg, val in mongoimport_args.items()]) if mongoimport_args else ""
    mongoimport_command = f"mongoimport --uri {mongo_dest.uri_with_db_name}  --file {coll_file_loc} {mongoimport_args}" + \
                          mongo_dest._get_optional_secrets_file_stdin()
    try:
        run_command_with_output("mongoimport", mongoimport_command, log_error_stream_to_output=True)
    except subprocess.CalledProcessError as ex:
        raise Exception("mongoexport failed! HINT: Did you forget to provide a secrets file for authentication?")


def mongo_import_from_dir(mongo_dest, export_dir):
    mongo_args = {
        "mode": "upsert",
        "jsonArray": ""
    }
    db_list = os.listdir(export_dir)

    for db in db_list:
        invalidate_and_set_db(mongo_dest, db)
        coll_list = os.listdir(os.path.join(export_dir, db))
        for coll in coll_list:
            import_data(mongo_dest, os.path.join(export_dir, db, coll), mongo_args)


def invalidate_and_set_db(mongo_instance, db):
    if 'uri_with_db_name' in mongo_instance.__dict__:
        del mongo_instance.__dict__['uri_with_db_name']
    mongo_instance.db_name = db


def write_query_to_file(query, query_file_dir, file_name):
    if query_file_dir and os.path.exists(query_file_dir):
        query_file = os.path.join(query_file_dir, file_name)
        logger.info(f"Creating query file in the location : {query_file}")
    else:
        query_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), file_name)
        logger.info(f"No query file path provided. Creating query file in the default location : {query_file}")
    try:
        with open(query_file, 'w') as open_file:
            open_file.write(query)
    except Exception:
        logger.error(f"Could not write query to the file: {query_file}")
    else:
        logger.info(f"Query written successfully to file: {query_file}")

    return query_file
