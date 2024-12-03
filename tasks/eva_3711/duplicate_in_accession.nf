#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Process a chunk of merge or split candidates

    Inputs:
            --source_duplicate_file     File containing list of duplicate (split or merge)
            --clustering_props          Properties file for the Merge resolution (Clustering CLUSTER_UNCLUSTERED_VARIANTS_JOB)
            --assembly_accession        Target assembly where the merge and split should be detected and corrected
            --instance_id               Instance id to run clustering
            --output_dir                Directory where the output log will be copied
    """
}

params.python = null
params.delete_variants_from_study = null
params.maven_xml_file = null
params.study = null
params.output_dir = null
params.assembly_accession=''
params.fasta=''
params.report=''


workflow {
    delete_all_accession_for_study(params.study)
    accession_vcf(delete_all_accession_for_study.out.db_clean_up, vcf_file_ch)
    detect_duplicates(accession_vcf.out.accession_done.collect())
}


process delete_all_accession_for_study {
    label 'long_time', 'med_mem'

    input:
    val study

    output:
    val 1, emit: db_clean_up

    script:
    """
    $params.python $params.delete_variants_from_study --maven_xml_file $params.maven_xml_file --study $study
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
    pipeline_parameters += " --parameters.vcfAggregation=none"
    pipeline_parameters += " --parameters.fasta=" + params.fasta.toString()
    pipeline_parameters += " --parameters.assemblyReportUrl=file:" + params.report.toString()
    pipeline_parameters += " --parameters.vcf=" + vcf_file.toString()

    accessioned_filename = vcf_filename.take(vcf_filename.indexOf(".vcf")) + ".accessioned.vcf"
    log_filename = "accessioning.${vcf_filename}"

    pipeline_parameters += " --parameters.outputVcf=" + "${params.public_dir}/${accessioned_filename}"

    """
    (java -Xmx${task.memory.toGiga()-1}G -jar $params.jar.accession_pipeline --spring.config.location=file:$params.accession_job_props $pipeline_parameters) || true
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
    $params.python $params.monitor_duplicate_accessions -p $params.accession_properties -o $params.output_dir -e eva-dev@ebi.ac.uk submittedVariantEntity
    """
}