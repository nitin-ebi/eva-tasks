import argparse
import csv
import logging
import subprocess
from pathlib import Path

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

container_name = 'eva_pre_submission_validator'
container_validation_dir = '/opt/vcf_validation'
container_validation_output_dir = '/opt/vcf_validation/vcf_validation_output'


def check_if_file_missing(mapping_file):
    files_missing = False
    with open(mapping_file) as open_file:
        reader = csv.DictReader(open_file, delimiter=',')
        for row in reader:
            if not Path(row['vcf']).is_file():
                files_missing = True
                logging.error('%s does not exist', row['vcf'])

    return files_missing


def check_docker(docker):
    docker_exist_cmd = subprocess.run([docker, '--version'])
    if docker_exist_cmd.returncode != 0:
        raise RuntimeError("Please make sure docker is installed and available on the path")

    # TODO: make sure container is availabe and ready for running
    container_exist_cmd = subprocess.run([docker, 'images'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run pre submission validation checks on vcf files', add_help=False)
    parser.add_argument("--docker_path", help="full path to the docker installation, "
                                              "not required if docker is available on path", required=False)
    parser.add_argument("--vcf_files_mapping",
                        help="csv file with the mappings for vcf files, fasta and assembly report", required=True)
    parser.add_argument("--output_dir", help="output_directory where the reports will be output", required=True)
    args = parser.parse_args()

    docker = args.docker_path if args.docker_path else 'docker'

    mapping_file = Path(args.vcf_files_mapping)
    output_dir = Path(args.output_dir)

    # check if mapping file is present
    if not mapping_file.is_file():
        raise RuntimeError('Mapping file {} not found'.format(mapping_file))
    elif check_if_file_missing(mapping_file):
        raise RuntimeError('some files (vcf/fasta) mentioned in metadata file could not be found')

    # check if docker container is ready for running validation
    # check_docker(docker)

    # copy mapping file to container validation dir
    subprocess.run([docker, "exec", container_name, "mkdir", "-p", f"{container_validation_dir}/{mapping_file.parent}"])
    subprocess.run([docker, 'cp', mapping_file, f"{container_name}:{container_validation_dir}/{mapping_file}"])

    # create the directory structure in container and copy all vcf files in container
    with open(mapping_file) as open_file:
        reader = csv.DictReader(open_file, delimiter=',')
        for row in reader:
            subprocess.run([docker, "exec", container_name, "mkdir", "-p",
                            f"{container_validation_dir}/{Path(row['vcf']).parent}"])
            subprocess.run([docker, "cp", row['vcf'], f"{container_name}:{container_validation_dir}/{row['vcf']}"])

    # run validation in container
    subprocess.run([docker, 'exec', container_name, 'nextflow', 'run', 'validation.nf',
                    "--vcf_files_mapping", f"{container_validation_dir}/{mapping_file}",
                    "--output_dir", f"{container_validation_output_dir}"])

