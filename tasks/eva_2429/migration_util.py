import os.path

from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)


def mongo_import_from_dir(mongo_dest, export_dir):
    mongo_args = {
        "mode": "upsert"
    }
    db_list = os.listdir(export_dir)

    for db in db_list:
        invalidate_and_set_db(mongo_dest, db)
        db_dir = os.path.join(export_dir, db)
        all_coll_dir = os.listdir(db_dir)
        for coll in all_coll_dir:
            logger.info(f'Importing data for db ({db} - collection ({coll})')
            coll_dir = os.path.join(db_dir, coll)
            files_list = os.listdir(coll_dir)
            for file in files_list:
                mongo_args.update({"collection": coll})
                mongo_dest.import_data(mongo_dest, os.path.join(coll_dir, file), mongo_args)


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
    except IOError:
        logger.error(f"Could not write query to the file: {query_file}")
    else:
        logger.info(f"Query written successfully to file: {query_file}")

    return query_file
