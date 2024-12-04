#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Create duplicate during accessioning then detect them

    Inputs:
            --
    """
}

params.python = ''
params.delete_variants_from_study = ''
params.maven_xml_file = ''
params.study = ''
params.output_dir = ''
params.accession_job_props=''
params.accession_pipeline=''
params.assembly_accession=''
params.fasta=''
params.report=''
params.monitor_duplicate_accessions=''
params.accession_properties=''


workflow {
    vcf_file_ch = Channel.fromPath(params.input_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file).name, file(row.vcf_file))}
    delete_all_accession_for_study()
    accession_vcf(delete_all_accession_for_study.out.db_clean_up, vcf_file_ch)
    detect_duplicates(accession_vcf.out.accession_done.collect())
}


process delete_all_accession_for_study {
    label 'long_time', 'med_mem'

    input:

    output:
    val 1, emit: db_clean_up

    script:
    """
    $params.python $params.delete_variants_from_study --maven_xml_file $params.maven_xml_file --study $params.study
    """

}


/*
 * Accession VCFs
 */
process accession_vcf {
    label 'long_time', 'med_mem'

    input:
    val db_clean_up
    tuple val(vcf_filename), val(vcf_file)

    output:
    path "${accessioned_filename}.tmp", emit: accession_done

    script:
    def pipeline_parameters = ""
    pipeline_parameters += " --parameters.assemblyAccession=" + params.assembly_accession.toString()
    pipeline_parameters += " --parameters.fasta=" + params.fasta.toString()
    pipeline_parameters += " --parameters.assemblyReportUrl=file:" + params.report.toString()
    pipeline_parameters += " --parameters.vcf=" + vcf_file.toString()
    pipeline_parameters += " --parameters.projectAccession=" + params.study.toString()


    accessioned_filename = vcf_filename.take(vcf_filename.indexOf(".vcf")) + ".accessioned.vcf"
    log_filename = "accessioning.${vcf_filename}"

    pipeline_parameters += " --parameters.outputVcf=" + "${params.output_dir}/${accessioned_filename}"

    """
    (java -Xmx${task.memory.toGiga()-1}G -jar $params.accession_pipeline --spring.config.location=file:$params.accession_job_props $pipeline_parameters) || true
    echo 'done' > ${accessioned_filename}.tmp
    """
}



process detect_duplicates {

    label 'med_time', 'med_mem'

    input:
    val all_vcf_files

    output:
    val 1, emit: detect_duplicates

    script:
    """
    $params.python $params.monitor_duplicate_accessions -p $params.accession_properties -s $params.study -o $params.output_dir -e eva-dev@ebi.ac.uk submittedVariantEntity
    """
}