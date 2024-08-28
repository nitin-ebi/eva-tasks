import argparse
import csv
import logging
import os
import shutil
import subprocess
from ftplib import FTP

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle, resolve_variant_warehouse_db_name
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query
from retry import retry

logger = log_cfg.get_logger(__name__)
log_cfg.add_stdout_handler()
log_cfg.set_log_level(logging.INFO)


def run_nextflow(working_dir, params, project_id, acc_file_db_name_csv):
    workflow_name = f"import_accession_{project_id}"

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
                '--acc_file_db_name_csv', acc_file_db_name_csv,
                '--logs_dir', os.path.join(working_dir, project_id)
            ))
        )
    except subprocess.CalledProcessError as e:
        raise Exception(f'Nextflow {workflow_name} pipeline failed: results might not be complete.')


def get_tax_asm_details(params, project_id, formatted_file_names):
    file_names = ",".join(f"'{f}'" for f in formatted_file_names)
    with get_metadata_connection_handle("production_processing", params['private_settings_xml_file']) as pg_conn:
        query = f"""
            select distinct f.filename, asm.assembly_accession, t.taxonomy_id
            from project_taxonomy pt 
            join taxonomy t on t.taxonomy_id = pt.taxonomy_id 
            join project_analysis pa on pa.project_accession = pt.project_accession 
            join analysis a on pa.analysis_accession = a.analysis_accession 
            join assembly asm on asm.assembly_accession = a.vcf_reference_accession 
            join analysis_file af on af.analysis_accession = pa.analysis_accession 
            join file f on f.file_id = af.file_id 
            where pt.project_accession = '{project_id}' 
            and f.filename IN ({file_names})
            and f.file_type IN ('vcf', 'VCF', 'vcf_aggregate', 'VCF_AGGREGATE')
            """
        return list(get_all_results_for_query(pg_conn, query))


@retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
def get_accession_report_files_from_ftp(working_dir, project_id):
    try:
        ftp = FTP('ftp.ebi.ac.uk', timeout=600)
        ftp.login()
        ftp.cwd(f'pub/databases/eva/{project_id}')
        files_in_ftp = ftp.nlst()
        ftp_accession_report_files = [f for f in files_in_ftp if ".accessioned.vcf" in f]
        if ftp_accession_report_files:
            download_file_dir = os.path.join(working_dir, project_id, 'FTP')
            os.makedirs(download_file_dir, exist_ok=True)
            logging.info(f"Trying to download files for project: {project_id}")
            for file in ftp_accession_report_files:
                try:
                    download_file_from_ftp(ftp, file, os.path.join(download_file_dir, file))
                except Exception as e:
                    raise Exception(f"Exception while downloading file {file}. Exception: {e}")

            return [os.path.join(download_file_dir, f) for f in os.listdir(download_file_dir)]
        else:
            return None
    except Exception:
        raise Exception(f"Error fetching files from ftp for project {project_id}")


@retry(tries=3, delay=2, backoff=1.5, jitter=(1, 3))
def download_file_from_ftp(ftp, file, local_file_path):
    logger.info(f"Trying to download file: {file}")
    with open(local_file_path, 'wb') as local_file:
        ftp.retrbinary(f"RETR {file}", local_file.write)


def run_import_accession_job_for_project(working_dir, params, project_id):
    logger.info(f"Starting processing project: {project_id}")

    project_path = os.path.join(params['project_dir'], project_id)
    if os.path.exists(project_path):
        accession_report_files = [
            os.path.join(project_path, '60_eva_public', f)
            for f in os.listdir(os.path.join(project_path, '60_eva_public'))
            if ".accessioned.vcf" in f]
    else:
        logger.warning(f"Could not find the Project {project_id} in project_path {project_path}. "
                       f"Trying to retrieve accession report files from FTP")
        try:
            accession_report_files = get_accession_report_files_from_ftp(working_dir, project_id)
        except Exception:
            raise Exception(f"Error fetching files from ftp for study {project_id}.")

    if not accession_report_files:
        raise Exception(f"No accession report files found for project {project_id} in CODON/FTP")
    else:
        formatted_name_file_dict = {
            os.path.basename(file).replace('.accessioned.vcf', '.vcf'): file
            for file in accession_report_files
        }

        file_asm_tax_list = get_tax_asm_details(params, project_id, formatted_name_file_dict.keys())
        logger.info(f"acc report files and their corresponding asm accession and taxonomy_id : {file_asm_tax_list}")

        if len(file_asm_tax_list) != len(accession_report_files):
            missing_files = set(formatted_name_file_dict.keys()) - set([file_asm_tax_list[i][0] for i in range(len(file_asm_tax_list))])
            raise Exception(
                f"For project {project_id}, File mismatch between DB and Codon/FTP."
                f"\nFiles missing: {[os.path.basename(formatted_name_file_dict[f]) for f in missing_files]}")
        else:
            os.makedirs(os.path.join(working_dir, project_id), exist_ok=True)
            with get_metadata_connection_handle("production_processing", params['private_settings_xml_file']) as pg_conn:
                acc_file_db_name_csv = os.path.join(working_dir, project_id, 'acc_file_db_name.csv')
                with open(acc_file_db_name_csv, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['acc_report_file', 'db_name'])
                    for acc_file, asm, taxonomy in file_asm_tax_list:
                        db_name = resolve_variant_warehouse_db_name(pg_conn, asm, taxonomy)
                        writer.writerow([formatted_name_file_dict[acc_file], db_name])

                run_nextflow(working_dir, params, project_id, acc_file_db_name_csv)


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
