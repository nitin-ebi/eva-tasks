import argparse
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.config_utils import get_mongo_uri_for_eva_profile, get_properties_from_xml_file
from pymongo import MongoClient

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def delete_variants(mongo_client, study, db_name, collection_name):
    collection = mongo_client[db_name][collection_name]
    x = collection.delete_many({'study': study})
    deletion_count = x.deleted_count
    logger.info(f"""{deletion_count} documents deleted from collection {collection_name} for study {study}""")


def main():
    parser = argparse.ArgumentParser(
        description='Delete document associated with specific study accession', add_help=False)
    parser.add_argument("--maven_xml_file",
                        help="Maven configuration file where connection to mongodb can be found", required=True)
    parser.add_argument("--study", help="Study accession for which all ssids will be removed",
                        required=True)
    args = parser.parse_args()
    maven_profile = 'development'
    mongo_uri = get_mongo_uri_for_eva_profile(maven_profile, args.maven_xml_file)
    mongo_client = MongoClient(mongo_uri)
    db_name = get_properties_from_xml_file(maven_profile, args.maven_xml_file)['eva.accession.mongo.database']
    delete_variants(mongo_client, args.study, db_name, 'submittedVariantEntity')


if __name__ == "__main__":
    main()
