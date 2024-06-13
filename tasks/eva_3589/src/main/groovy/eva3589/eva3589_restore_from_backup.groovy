package eva3589

import groovy.cli.picocli.CliBuilder
import uk.ac.ebi.eva.accession.core.GenericApplication

import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.*


def cli = new CliBuilder()
cli.devPropertiesFile(args: 1, "Properties file to use for the dev database connection", required: true)


def options = cli.parse(args)
if (!options) {
    cli.usage()
    throw new Exception("Invalid command line options provided!")
}

// this is equivalent to if __name__ == '__main__' in Python
if (this.getClass().getName().equals('eva3589.eva3589_restore_from_backup')) {
    def devEnv = createFromSpringContext(options.devPropertiesFile, GenericApplication.class)
    new TestDataset(options.assemblyAccession, devEnv).restoreDevCollectionsFromBackup()
}

