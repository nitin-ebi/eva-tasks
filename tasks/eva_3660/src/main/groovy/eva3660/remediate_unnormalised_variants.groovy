package eva3660

import groovy.cli.picocli.CliBuilder

import org.springframework.boot.builder.SpringApplicationBuilder


def cli = new CliBuilder()
cli.workingDir(args: 1, "Path to the working directory where processing files will be kept", required: true)
cli.envPropertiesFile(args: 1, "Properties file with db details to use for remediation", required: true)
cli.dbName(args: 1, "Database name that needs to be remediated", required: true)
cli.fastaDir(args: 1, "Path to the root of the directory containing FASTA files", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}


// Run the remediation application
new SpringApplicationBuilder(RemediationApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.workingDir, options.dbName, options.fastaDir)
