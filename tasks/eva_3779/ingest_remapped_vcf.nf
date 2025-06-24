



/*
 * Ingest the remapped submitted variants from a VCF file into the accessioning warehouse.
 */
process ingest_vcf_into_mongo {
    label 'long_time', 'med_mem'

    input:
    each path(remapped_vcf)


    output:
    path "${remapped_vcf}_ingestion.log", emit: ingestion_log_filename

    publishDir "$params.output_dir/logs", overwrite: true, mode: "copy", pattern: "*.log*"

    script:
    """
    # Check the file name to know which database to load the variants into
    TMP=$remapped_vcf
    if [[ \$TMP == *_eva_remapped.vcf ]]
    then
        loadTo=EVA
    else
        loadTo=DBSNP
    fi
    # Extract the source GCA
    remappedFrom=\${TMP:1:14}
    java -Xmx${task.memory.toGiga()-1}G -jar $params.jar.vcf_ingestion \
        --spring.config.location=file:${params.ingestion_properties} \
        --parameters.vcf=${remapped_vcf} \
        --parameters.assemblyReportUrl=file:${params.target_report} \
        --parameters.loadTo=\${loadTo} \
        --parameters.remappedFrom==\${remappedFrom}
        > ${remapped_vcf}_ingestion.log
    """
}



workflow {
    main:
        // Add list of vcf to load
        remapped_vcfs = channel.fromPath( '' )

        ingest_vcf_into_mongo(remapped_vcfs)

}