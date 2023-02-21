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


def run_command_with_output(command_description, command, return_process_output=False,
                            log_error_stream_to_output=False):
    process_output = ""

    logging.info("Starting process: " + command_description)
    logging.info("Running command: " + command)

    stdout = subprocess.PIPE
    # Some utilities like mongodump and mongorestore output non-error messages to error stream
    # This is a workaround for that
    stderr = subprocess.STDOUT if log_error_stream_to_output else subprocess.PIPE

    with subprocess.Popen(command, stdout=stdout, stderr=stderr, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in iter(process.stdout.readline, ''):
            line = str(line).rstrip()
            logging.info(line)
            if return_process_output:
                process_output += line + "\n"
        if not log_error_stream_to_output:
            for line in iter(process.stderr.readline, ''):
                line = str(line).rstrip()
                logging.error(line)
    if process.returncode != 0:
        logging.error(command_description + " failed! Refer to the error messages for details.")
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logging.info(command_description + " - completed successfully")
    if return_process_output:
        return process_output


def verify_docker(docker):
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
    # verify_docker(docker)

    try:
        run_command_with_output("Removing existing files from validation directory in container",
                                "{0} exec {1} rm -rf {2}".format(docker, container_name, container_validation_dir))

        run_command_with_output("Creating directory structure for copying vcf metadata file in container",
                                "{0} exec {1} mkdir -p {2}".format(docker, container_name,
                                                                   f"{container_validation_dir}/{mapping_file.parent}"))
        run_command_with_output("Copying vcf metadata file into container",
                                "{0} cp {1} {2}".format(docker, mapping_file,
                                                        f"{container_name}:{container_validation_dir}/{mapping_file}"))
        with open(mapping_file) as open_file:
            reader = csv.DictReader(open_file, delimiter=',')
            for row in reader:
                run_command_with_output("Creating directory structure for copying vcf files in container",
                                        "{0} exec {1} mkdir -p {2}".format(docker, container_name,
                                                                           f"{container_validation_dir}/{Path(row['vcf']).parent}"))
                run_command_with_output("Copying vcf files into container",
                                        "{0} cp {1} {2}".format(docker, row['vcf'],
                                                                f"{container_name}:{container_validation_dir}/{row['vcf']}"))

        run_command_with_output("Running Validation - Nextflow",
                                "{0} exec {1} nextflow run validation.nf --vcf_files_mapping {2} --output_dir {3}"
                                .format(docker, container_name, f"{container_validation_dir}/{mapping_file}",
                                        container_validation_output_dir))

        run_command_with_output("Copying validation output from container to host",
                                "{0} cp {1} {2}".format(docker, f"{container_name}:{container_validation_output_dir}",
                                                        output_dir))
    except subprocess.CalledProcessError as ex:
        logging.error(ex)
