import sys
from argparse import ArgumentParser
from itertools import islice

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

logger = log_cfg.get_logger(__name__)


def chunked_iterable(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


def delete_variants(private_config_xml_file, profile, rs_ids_file):
    with get_mongo_connection_handle(profile, private_config_xml_file) as mongo_conn:
        db_name = 'eva_accession_sharded'

        chunk_num = 1
        for chunk in chunked_iterable(rs_ids_file, 1000):
            logger.info(f"Processing chunk {chunk_num}")
            chunk_num += 1

            # delete CVE
            cve_coll = mongo_conn[db_name]['clusteredVariantEntity']
            delete_result = cve_coll.delete_many({"accession": {"$in": chunk}, "asm": "GCA_000188115.3"})
            logger.info(f"Deleted {delete_result.deleted_count} documents from clusteredVariantEntity.")

            # remove RS in SVE
            sve_coll = mongo_conn[db_name]['submittedVariantEntity']
            update_result = sve_coll.update_many(
                {"rs": {"$in": chunk}, "seq": "GCA_000188115.3", "study": "PRJEB82793"},
                {"$unset": {"rs": ""}}
            )
            logger.info(
                f"Updated {update_result.modified_count} documents in submittedVariantEntity (removed rs field).")

            # remove backpropagated RS in SVE
            update_result = sve_coll.update_many(
                {"backPropRS": {"$in": chunk}, "seq": "GCA_000188115.2", "study": "PRJEB82793"},
                {"$unset": {"backPropRS": ""}}
            )
            logger.info(
                f"Updated {update_result.modified_count} documents in submittedVariantEntity (removed backPropRS field).")


def get_rs_ids_from_file(rs_ids_file):
    rs_ids_list = []
    with open(rs_ids_file) as rs_ids_file:
        for line in rs_ids_file.readlines():
            rs_id = line.split('\t')[0].strip()
            rs_ids_list.append(int(rs_id))

    return rs_ids_list


def main():
    arg_parse = ArgumentParser(description='Delete duplicate RS')
    arg_parse.add_argument('--private-config-xml-file',
                           help="Maven configuration file where connection to mongodb can be found", required=True)
    arg_parse.add_argument('--profile', help='e.g. production, development or local', required=True)
    arg_parse.add_argument('--rs-ids-file', help='file that contains duplicate rs ids', required=True)

    args = arg_parse.parse_args()

    # read ids from file an put it into a list
    rs_ids_list = get_rs_ids_from_file(args.rs_ids_file)

    # delete duplicate RS
    delete_variants(args.private_config_xml_file, args.profile, rs_ids_list)

    return 0


if __name__ == '__main__':
    sys.exit(main())
