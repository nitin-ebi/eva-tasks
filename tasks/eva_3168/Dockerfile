FROM python:3.10

ENV vcf_validator_version=0.9.4
ENV NXF_VER=22.10.6

WORKDIR /opt

RUN curl -LJo /usr/local/bin/vcf_validator  https://github.com/EBIvariation/vcf-validator/releases/download/v${vcf_validator_version}/vcf_validator_linux \
    && curl -LJo /usr/local/bin/vcf_assembly_checker  https://github.com/EBIvariation/vcf-validator/releases/download/v${vcf_validator_version}/vcf_assembly_checker_linux \
    && chmod 755 /usr/local/bin/vcf_assembly_checker /usr/local/bin/vcf_validator

RUN apt update && apt install -y default-jdk

RUN curl -s https://get.nextflow.io | bash \
    && mv nextflow /usr/local/bin/

COPY validation.nf /opt/validation.nf