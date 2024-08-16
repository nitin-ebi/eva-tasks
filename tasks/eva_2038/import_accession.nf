#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Import accessions to Variant warehouse

    Inputs:
            --java_app                  path to the java app jar to run accession import job
            --acc_import_job_props      path to the accession import job properties
            --accession_report          path to the accession report
            --db_name                   database name for the variant warehouse
            --logs_dir                  logs directory
    """
}

params.java_app = null
params.acc_import_job_props = null
params.accession_report = null
params.db_name = null
params.logs_dir = null

params.help = null
if (params.help) exit 0, helpMessage()

if (!params.java_app || !params.acc_import_job_props || !params.accession_report || !params.db_name || !params.logs_dir) {
    if (!params.java_app) log.warn('Provide path to java app jar using --java_app')
    if (!params.acc_import_job_props) log.warn('Provide path to accession import job properties file using --acc_import_job_props')
    if (!params.accession_report) log.warn('Provide path to the accession_report file --accession_report')
    if (!params.db_name) log.warn('Provide db_name using --db_name')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, "Required parameters missing"
}

workflow {
    import_accession()
}

process import_accession {
    label 'default_time', 'med_mem'

    clusterOptions {
        log_filename = params.accession_report.fileName
        return "-o ${params.logs_dir}/acc_import.${log_filename}.log \
                -e ${params.logs_dir}/acc_import.${log_filename}.err"
    }

    script:
    def pipeline_parameters = ""
    pipeline_parameters += " --input.accession.report=${params.accession_report}"
    pipeline_parameters += " --spring.data.mongodb.database=${params.db_name}"

    """
    java -Xmx4G -jar $params.java_app --spring.config.location=file:$params.acc_import_job_props --parameters.path=$params.acc_import_job_props $pipeline_parameters
    """
}
