import argparse
import logging
import os
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle, resolve_variant_warehouse_db_name
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

logger = log_cfg.get_logger(__name__)
log_cfg.add_stdout_handler()
log_cfg.set_log_level(logging.INFO)


def run_nextflow(working_dir, params, project_id, accession_report_file, db_name):
    workflow_name = f"import_accession_{project_id}_{os.path.basename(accession_report_file).replace('.', '_')}"

    # create nextflow work dir - remove if already exists
    nextflow_work_dir = os.path.join(working_dir, project_id, workflow_name)
    if os.path.exists(nextflow_work_dir):
        shutil.rmtree(nextflow_work_dir)
    os.makedirs(nextflow_work_dir)

    try:
        command_utils.run_command_with_output(
            f'Nextflow {workflow_name} process',
            ' '.join((
                'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                params['nextflow_path'], params['nextflow_script'],
                '-name', workflow_name,
                '-work-dir', nextflow_work_dir,
                '--java_app', params['java_app'],
                '--acc_import_job_props', params['acc_import_job_props'],
                '--accession_report', accession_report_file,
                '--db_name', db_name,
                '--logs_dir', os.path.join(working_dir, project_id)
            ))
        )
    except subprocess.CalledProcessError as e:
        logger.error(f'Nextflow {workflow_name} pipeline failed: results might not be complete.')
        raise e


def get_db_name(params, project_id, accession_report_file):
    with get_metadata_connection_handle("production_processing", params['private_settings_xml_file']) as pg_conn:
        query = f"""
            select distinct asm.assembly_accession, t.taxonomy_id
            from project_taxonomy pt 
            join taxonomy t on t.taxonomy_id = pt.taxonomy_id 
            join project_analysis pa on pa.project_accession = pt.project_accession 
            join analysis a on pa.analysis_accession = a.analysis_accession 
            join assembly asm on asm.assembly_accession = a.vcf_reference_accession 
            join analysis_file af on af.analysis_accession = pa.analysis_accession 
            join file f on f.file_id = af.file_id 
            where pt.project_accession = '{project_id}' 
            and f.filename = '{os.path.basename(accession_report_file).replace(".accessioned.vcf", ".vcf")}'
            and f.file_type IN ('vcf', 'VCF', 'vcf_aggregate', 'VCF_AGGREGATE')
            """
        res = list(get_all_results_for_query(pg_conn, query))
        if res:
            return resolve_variant_warehouse_db_name(pg_conn, res[0][0], res[0][1])
        else:
            return None


def run_import_accession_job_for_project(working_dir, params, project_id):
    logger.info(f"Starting processing project: {project_id}")

    project_path = os.path.join(params['project_dir'], project_id)
    if not os.path.exists(project_path):
        logger.error(f"Could not find the Project {project_id}. Please make sure the project {project_path} exists")
        return

    accession_report_files = [
        os.path.join(project_path, '60_eva_public', f)
        for f in os.listdir(os.path.join(project_path, '60_eva_public'))
        if ".accessioned.vcf" in f
    ]
    if not accession_report_files:
        logger.error(f"No accession report files found for project {project_id} in {project_path}")
    else:
        for accession_report_file in accession_report_files:
            db_name = get_db_name(params, project_id, accession_report_file)
            if db_name:
                run_nextflow(working_dir, params, project_id, accession_report_file, db_name)
            else:
                logger.error(
                    f'Could not find DB name for project {project_id} and accession_report_file: {accession_report_file}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='', add_help=False)
    parser.add_argument("--working-dir", help="/path/to/dir where all the logs and other files will be stored",
                        required=True)
    parser.add_argument("--params-file", help="/path/to/params/file containing path of nextflow, java app etc.",
                        required=True)
    parser.add_argument("--project-list", help="List of projects space-separated e.g. PRJEB123 PRJEB456",
                        required=True, nargs='+')
    args = parser.parse_args()

    os.makedirs(args.working_dir, exist_ok=True)

    with open(args.params_file, 'r') as file:
        params = yaml.safe_load(file)

    for project_id in args.project_list:
        run_import_accession_job_for_project(args.working_dir, params, project_id.strip())
