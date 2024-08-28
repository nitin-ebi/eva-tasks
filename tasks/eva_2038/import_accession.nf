#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Import accessions to Variant warehouse

    Inputs:
            --java_app                  path to the java app jar to run accession import job
            --acc_import_job_props      path to the accession import job properties
            --acc_file_db_name_csv      csv file with the mappings for accession report file and db_name
            --logs_dir                  logs directory
    """
}

params.java_app = null
params.acc_import_job_props = null
params.acc_file_db_name_csv = null
params.logs_dir = null

params.help = null
if (params.help) exit 0, helpMessage()

if (!params.java_app || !params.acc_import_job_props || !params.acc_file_db_name_csv || !params.logs_dir) {
    if (!params.java_app) log.warn('Provide path to java app jar using --java_app')
    if (!params.acc_import_job_props) log.warn('Provide path to accession import job properties file using --acc_import_job_props')
    if (!params.acc_file_db_name_csv) log.warn('Provide maps of accession report files and their corresponding db_names using --acc_file_db_name_csv')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, "Required parameters missing"
}

workflow {
    acc_report_files_dbname = Channel.fromPath(params.acc_file_db_name_csv)
                    .splitCsv(header:true)
                    .map{row -> tuple(file(row.acc_report_file), row.db_name)}

    import_accession(acc_report_files_dbname)
}

process import_accession {
    label 'default_time', 'med_mem'

    clusterOptions {
        log_filename = accession_report.getFileName().toString()
        return "-o $params.logs_dir/acc_import.${log_filename}.log \
                -e $params.logs_dir/acc_import.${log_filename}.err"
    }

    input:
    tuple val(accession_report), val(db_name)

    script:
    def pipeline_parameters = ""
    pipeline_parameters += " --input.accession.report=${accession_report}"
    pipeline_parameters += " --spring.data.mongodb.database=${db_name}"

    """
    java -Xmx4G -jar $params.java_app --spring.config.location=file:$params.acc_import_job_props --parameters.path=$params.acc_import_job_props $pipeline_parameters
    """
}
