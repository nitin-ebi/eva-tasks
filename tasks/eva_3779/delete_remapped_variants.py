import logging
import sys
from argparse import ArgumentParser
from itertools import islice

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from pymongo import ReadPreference

logger = log_cfg.get_logger(__name__)


assembly_to_delete = 'GCA_002263795.4'
db_name = 'eva_accession_sharded_EVA3779'


def chunked_iterable(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk

def delete_variants_no_search(collections, find_filter, private_config_xml_file, profile, chunk_size = 1000):
    with get_mongo_connection_handle(profile, private_config_xml_file, read_preference = ReadPreference.SECONDARY_PREFERRED, write_concern=0) as mongo_conn:
        for source_collection in collections:
            collection_obj = mongo_conn[db_name][source_collection]
            cursor = collection_obj.find(find_filter)
            chunk_num = 1
            total_deletion = 0
            for variants_chunk in chunked_iterable(cursor, chunk_size):
                logger.info(f"Processing chunk {chunk_num} : {chunk_num * chunk_size} in {source_collection}")
                chunk_num += 1
                hash_to_delete = [variant.get('_id') for variant in variants_chunk]
                collection_obj.delete_many({'_id': {'$in': list(hash_to_delete)}})
                total_deletion += chunk_size
                logger.info(f"Deleted {chunk_size} (Total {total_deletion}) documents from {source_collection}.")
                if total_deletion>1000000:
                    break

def delete_variants2(private_config_xml_file, profile, chunk_size = 1000):
    submitted_collections = ['submittedVariantEntity', 'dbsnpSubmittedVariantEntity']
    cluster_collections = ['clusteredVariantEntity', 'dbsnpClusteredVariantEntity']
    operation_collections = ['submittedVariantOperationEntity', 'dbsnpSubmittedVariantOperationEntity']
    sve_find_filter = {"seq": assembly_to_delete, 'remappedFrom': {'$exists': 1}}
    cve_find_filter = {"asm": assembly_to_delete}
    svoe_find_filter = {'inactiveObjects.seq': assembly_to_delete}

    delete_variants_no_search(submitted_collections, sve_find_filter, private_config_xml_file, profile, chunk_size = chunk_size)
    delete_variants_no_search(cluster_collections, cve_find_filter, private_config_xml_file, profile, chunk_size = chunk_size)
    delete_variants_no_search(operation_collections, svoe_find_filter, private_config_xml_file, profile, chunk_size = chunk_size)


def delete_variants(private_config_xml_file, profile, source_collection, chunk_size = 1000):
    with get_mongo_connection_handle(profile, private_config_xml_file) as mongo_conn:
        sve_find_filter = {"seq": assembly_to_delete, 'remappedFrom':{'$exists':1}}
        operation_collections = ['submittedVariantOperationEntity', 'dbsnpSubmittedVariantOperationEntity']
        cluster_collections = ['clusteredVariantEntity', 'dbsnpClusteredVariantEntity']
        submitted_collections = ['submittedVariantEntity', 'dbsnpSubmittedVariantEntity']
        sve_coll = mongo_conn[db_name][source_collection]
        cursor = sve_coll.find(sve_find_filter)

        chunk_num = 1
        for variants_chunk in chunked_iterable(cursor, chunk_size):
            logger.info(f"Processing chunk {chunk_num} : {chunk_num * chunk_size}")
            chunk_num += 1
            hash_to_delete = [variant.get('_id') for variant in variants_chunk]
            sve_accessions = [variant.get('accession') for variant in variants_chunk]
            cve_accessions = set([variant.get('rs') for variant in variants_chunk if variant.get('rs')])

            # delete SVE
            delete_result = sve_coll.delete_many({'_id': {'$in': list(hash_to_delete)}})
            logger.info(f"Deleted {delete_result.deleted_count} documents from {source_collection}.")

            # Delete submitted variant operation if they exists
            for collection in operation_collections:
                tmp_svoe_coll = mongo_conn[db_name][collection]
                hash_to_delete = [op.get('_id') for op in tmp_svoe_coll.find(
                    {'accession': {'$in': sve_accessions}, 'inactiveObjects.seq': assembly_to_delete}, {'_id': 1}
                )]
                if hash_to_delete:
                    delete_result = tmp_svoe_coll.delete_many({'_id': {'$in': hash_to_delete}})
                    logger.info(f"Deleted {delete_result.deleted_count} documents from {collection}.")

            # delete clustered variants if they are orphan
            cve_accession_to_keep = set()
            for collection in submitted_collections:
                tmp_sve_coll = mongo_conn[db_name][collection]
                variants_for_these_rs = list(tmp_sve_coll.find({'rs': {'$in': list(cve_accessions)}, 'seq': assembly_to_delete}))
                cve_accession_to_keep.update(set([variant.get('rs') for variant in variants_for_these_rs] ))
            cve_accessions_to_delete = cve_accessions - cve_accession_to_keep
            for collection in cluster_collections:
                tmp_cve_coll = mongo_conn[db_name][collection]
                hash_to_delete = [cluster.get('_id') for cluster in tmp_cve_coll.find(
                    {'accession': {'$in': list(cve_accessions_to_delete)}, 'asm':assembly_to_delete},
                    {'_id': 1})]
                if hash_to_delete:
                    delete_result = tmp_cve_coll.delete_many({'_id': {'$in': hash_to_delete}})
                    logger.info(f"Deleted {delete_result.deleted_count} documents from {collection}.")

def main():
    arg_parse = ArgumentParser(description='Delete variants remapped to  RS')
    arg_parse.add_argument('--private-config-xml-file',
                           help="Maven configuration file where connection to mongodb can be found", required=True)
    arg_parse.add_argument('--profile', help='e.g. production, development or local', required=True)

    args = arg_parse.parse_args()
    log_cfg.add_stdout_handler(level=logging.INFO)
    # delete variant in the remapped assembly
    delete_variants2(args.private_config_xml_file, args.profile)

    return 0


if __name__ == '__main__':
    sys.exit(main())
