import argparse
import json
import logging
import os

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

logger = log_cfg.get_logger(__name__)
log_cfg.set_log_level(logging.INFO)

logger = log_cfg.get_logger(__name__)
log_cfg.add_stdout_handler()

CVE_OPS_COLLECTIONS = ["clusteredVariantOperationEntity", "dbsnpClusteredVariantOperationEntity"]
SVE_OPS_COLLECTIONS = ["submittedVariantOperationEntity", "dbsnpSubmittedVariantOperationEntity"]


def get_list_of_assemblies_to_check(private_config_xml_file):
    asm_list = list()
    query = f"select distinct assembly_id from evapro.supported_assembly_tracker;"
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as pg_conn:
        for assembly in get_all_results_for_query(pg_conn, query):
            asm_list.append(assembly[0])

    return asm_list


def find_and_delete_split_operations(private_config_xml_file, working_dir):
    asm_list = get_list_of_assemblies_to_check(private_config_xml_file)
    logger.info(f'List of asm to search: {asm_list}')

    cve_docs = []
    sve_docs = []
    cve_ops_ids = set()
    cve_ops_accs = set()
    sve_ops_ids = set()

    with get_mongo_connection_handle("production_processing", private_config_xml_file) as mongo_conn:
        # get cve ops ids
        for collection in CVE_OPS_COLLECTIONS:
            cve_coll = mongo_conn['eva_accession_sharded'][collection]
            cve_query = {
                'inactiveObjects.asm': {'$in': asm_list},
                'eventType': 'RS_SPLIT',
                '$expr': {'$eq': ['$accession', '$splitInto']}
            }
            cursor = cve_coll.find(cve_query, no_cursor_timeout=True)

            for cve_ops_doc in cursor:
                cve_docs.append(cve_ops_doc)
                cve_ops_ids.add(cve_ops_doc['_id'])
                cve_ops_accs.add(cve_ops_doc['accession'])

        logger.info(f'Number of CVE documents: {len(cve_docs)}')
        # Save CVE documents to file
        cve_docs_file = os.path.join(working_dir, "cve_docs.json")
        with open(cve_docs_file, 'w') as f:
            json.dump(cve_docs, f, default=str)

        # get sve ops ids
        for collection in SVE_OPS_COLLECTIONS:
            sve_coll = mongo_conn['eva_accession_sharded'][collection]
            sve_query = {
                'inactiveObjects.rs': {'$in': list(cve_ops_accs)}
            }
            cursor = sve_coll.find(sve_query, no_cursor_timeout=True)

            for sve_ops_doc in cursor:
                rs = f"rs{sve_ops_doc['inactiveObjects'][0]['rs']}"
                reason = f'SS was associated with the split RS {rs} that was split from {rs} after remapping.'
                if sve_ops_doc['reason'] == reason:
                    sve_docs.append(sve_ops_doc)
                    sve_ops_ids.add(sve_ops_doc['_id'])

        logger.info(f'Number of SVE documents: {len(sve_docs)}')
        # Save SVE documents to file
        sve_docs_file = os.path.join(working_dir, "sve_docs.json")
        with open(sve_docs_file, 'w') as f:
            json.dump(sve_docs, f, default=str)

        # delete sve operations
        for collection in SVE_OPS_COLLECTIONS:
            sve_coll = mongo_conn['eva_accession_sharded'][collection]
            sve_del_query = {'_id': {'$in': list(sve_ops_ids)}}
            result = sve_coll.delete_many(sve_del_query)
            logger.info(f'Deleted {result.deleted_count} SVE documents from {collection}')

        # delete cve operations
        for collection in CVE_OPS_COLLECTIONS:
            cve_coll = mongo_conn['eva_accession_sharded'][collection]
            cve_del_query = {'_id': {'$in': list(cve_ops_ids)}}
            result = cve_coll.delete_many(cve_del_query)
            logger.info(f'Deleted {result.deleted_count} CVE documents from {collection}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find and delete RS_SPLIT operations that were split into the same RS',
                                     add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--working-dir", help="ex: /path/to/dir where ", required=True)
    args = parser.parse_args()

    find_and_delete_split_operations(args.private_config_xml_file, args.working_dir)
