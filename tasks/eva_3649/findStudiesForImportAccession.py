import argparse
import logging
import os
from datetime import datetime

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

logger = log_cfg.get_logger(__name__)
log_cfg.add_stdout_handler()
log_cfg.set_log_level(logging.INFO)


def prepare_final_file_with_studies(study_dir):
    final_studies_list = set()
    try:
        for filename in os.listdir(study_dir):
            if filename.endswith(".txt"):
                with open(os.path.join(study_dir, filename), 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        trimmed_line = line.strip()
                        if trimmed_line:
                            final_studies_list.add(trimmed_line)
    except Exception as e:
        logger.error(f"Error reading files: {e}")

    logger.info(f"Final Studies List: {final_studies_list}")

    try:
        final_file_path = os.path.join(study_dir, f"finalList_{datetime.now()}.txt")
        with open(final_file_path, 'w') as file:
            for study in final_studies_list:
                file.write(f"{study}\n")
        print("List has been written to the file successfully.")
    except Exception as e:
        logger.error(f"An error occurred while writing the file: {e}")


def get_studies_for_accession_import_run(private_settings_file, study_dir):
    with get_mongo_connection_handle("production_processing", private_settings_file) as mongo_conn:
        try:
            databases = mongo_conn.list_database_names()

            for db_name in databases:
                db_file_path = os.path.join(study_dir, f"{db_name}.txt")
                if "dryrun" in db_name or "test" in db_name or os.path.exists(db_file_path):
                    continue

                open(db_file_path, 'w').close()

                logger.info(f"Starting search for studies in DB: {db_name}")
                database = mongo_conn[db_name]

                collections = database.list_collection_names()
                if "variants_2_0" in collections:
                    logger.info("Found collection Variants_2_0 in DB")
                    collection = database["variants_2_0"]

                    pipeline = [
                        {"$match": {"ids": {"$not": {"$regex": "^ss.+$"}}}},
                        {"$unwind": "$files"},
                        {"$group": {"_id": db_name, "sids": {"$addToSet": "$files.sid"}}}
                    ]
                    result = collection.aggregate(pipeline, allowDiskUse=True)

                    cursor = result
                    if cursor:
                        logger.info(f"Studies found in database: {db_name}")
                        for doc in cursor:
                            studies_list = doc.get("sids", [])
                            logger.info(f"Studies List: {studies_list}")
                            try:
                                with open(db_file_path, 'w') as file:
                                    for study in studies_list:
                                        file.write(f"{study}\n")
                                print("List has been written to the file successfully.")
                            except Exception as e:
                                logger.error(f"An error occurred while writing the file {db_file_path}: {e}")
                    else:
                        logger.info(f"No studies found in database: {db_name}")

        except Exception as e:
            logger.error(f"An error occurred while getting studies : {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find and delete RS_SPLIT operations that were split into the same RS',
                                     add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--study-dir", help="ex: /path/to/dir where deleted documents will be logged", required=True)
    parser.add_argument('--prepare-final-list', help='Prepare the final combined list', action='store_true',
                        default=False)
    args = parser.parse_args()

    if args.prepare_final_list:
        prepare_final_file_with_studies(args.study_dir)
    else:
        get_studies_for_accession_import_run(args.private_config_xml_file, args.study_dir)
        prepare_final_file_with_studies(args.study_dir)
