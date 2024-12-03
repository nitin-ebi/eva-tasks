import argparse
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.config_utils import get_mongo_uri_for_eva_profile
from pymongo import MongoClient

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def delete_variants(mongo_source, study, collection_name):
    collection = mongo_source.mongo_handle[mongo_source.db_name][collection_name]
    x = collection.delete_many({'study': study})
    deletion_count = x.deleted_count
    logger.info(f"""{deletion_count} documents deleted from collection {collection_name}""")


def main():
    parser = argparse.ArgumentParser(
        description='Delete document associated with specific study accession', add_help=False)
    parser.add_argument("--maven_xml_file",
                        help="Maven configuration file where connection to mongodb can be found", required=True)
    parser.add_argument("--study", help="Study accession for which all ssids will be removed",
                        required=True)
    args = parser.parse_args()
    mongo_uri = get_mongo_uri_for_eva_profile('development', args.maven_xml_file)
    mongo_source = MongoClient(mongo_uri)
    delete_variants(mongo_source, args.study, 'submittedVariantEntity')


if __name__ == "__main__":
    main()
