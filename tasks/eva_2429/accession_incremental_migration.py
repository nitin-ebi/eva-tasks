import os.path

import psycopg2
import psycopg2.extras
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

from migration_util import export_data
from migration_util import write_query_to_file

logger = logging_config.get_logger(__name__)

project_key = "projectAccession"
assembly_key = "assemblyAccession"
accession_query_file_name = "accession_query.txt"
accession_db = "eva_accession_sharded"
accession_collection = "submittedVariantEntity"


def find_accession_studies_eligible_for_migration(private_config_xml_file, migration_start_time, migration_end_time):
    # with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("production", private_config_xml_file),
    #                      user="evaaccjt") as metadata_connection_handle:
    with psycopg2.connect("postgresql://pgsql-hxvm7-010.ebi.ac.uk:5432/vrnvaraccjtpro",
                          user="evaaccjt", password="FAISMOzB7WS4") as metadata_connection_handle:
        query_string = f"select bjep.job_execution_id, bjep.key_name, bjep.string_val, bje.start_time \
                     from batch_job_execution bje join batch_job_execution_params bjep \
                     on bje.job_execution_id=bjep.job_execution_id \
                     where bjep.key_name in ('{project_key}', '{assembly_key}')  \
                     and bje.start_time > '{migration_start_time}' and bje.start_time < '{migration_end_time}' \
                     order by bjep.job_execution_id desc , bjep.key_name"

    query_result = get_all_results_for_query(metadata_connection_handle, query_string)
    logger.info(f"\nStudies eligible for migration : {query_result}")

    job_parameter_combine = {}
    for job_id, key_name, key_value, start_time in query_result:
        job_parameter_combine.setdefault(job_id, {}).update({key_name: key_value})

    study_seq_set = set()
    for key, val in job_parameter_combine.items():
        study_seq_set.add((val[project_key], val[assembly_key]))

    return study_seq_set


def export_accession_data(mongo_source, study_seq_set, export_dir, query_file_dir):
    mongo_source.db_name = accession_db
    accession_query = create_accession_query(study_seq_set)
    query_file_path = write_query_to_file(accession_query, query_file_dir, accession_query_file_name)
    mongo_args = {
        "collection": accession_collection,
        "queryFile": query_file_path,
        "jsonArray": ""
    }

    logger.info(
        f"Starting mongo export process for accessioning database: mongo_source ({mongo_source.mongo_handle.address[0]}) and mongo_args ({mongo_args})")
    accession_export_file = os.path.join(export_dir, accession_db, f"{accession_collection}")

    export_data(mongo_source, accession_export_file, mongo_args)


def create_accession_query(study_seq_set):
    # query_template: {$or:[{study:"PRJEB1234",seq:"GC000012.4"},{study:"PRJEB4567",seq:"GC000045.3"},...]}
    query_beg = "{$or:["
    study_string = ",".join("{study:\"" + x[0] + "\",seq:\"" + x[1] + "\"}" for x in study_seq_set)
    query_end = "]}"
    accession_query = query_beg + study_string + query_end
    logger.info(f"query created for accession migration : {accession_query}")

    return accession_query


def accession_export(mongo_source, private_config_xml_file, export_dir, query_file_dir, start_time, end_time):
    study_seq_set = find_accession_studies_eligible_for_migration(private_config_xml_file, start_time, end_time)
    export_accession_data(mongo_source, study_seq_set, export_dir, query_file_dir)
