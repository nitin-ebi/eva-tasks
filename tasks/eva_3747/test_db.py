import argparse
import os
from datetime import datetime
from functools import cached_property

import requests
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query, execute_query
from lxml import etree
from retry import retry


logger = logging_config.get_logger(__name__)
logging_config.add_stderr_handler()

@retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
def get_species():
    response = requests.get('https://wwwdev.ebi.ac.uk/eva/webservices/rest/v1/meta/species/list')
    response.raise_for_status()
    response_json = response.json()
    return response_json.get('response')[0].get('result')


def test_databases(databases_names):
    all_species = get_species()
    db_name_2_species = {}
    for species_dict in all_species:
        db_name_2_species[f'eva_{species_dict["taxonomyCode"]}_{species_dict["assemblyCode"]}'] = species_dict
    for database_name in databases_names:
        if database_name in db_name_2_species:
            # logger.info(f'{database_name} is supported')
            pass
        else:
            logger.info(f'{database_name} is not supported')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test if the variant warehouse database name can be found in the species API')
    parser.add_argument("--databases",  nargs='*', default=None, help="list of database to fix", required=True)

    args = parser.parse_args()
    test_databases(args.databases)
