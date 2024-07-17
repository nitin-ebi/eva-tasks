package eva3623

import eva3623.CorrectSSOperationFromEVA3399
import groovy.cli.picocli.CliBuilder
import uk.ac.ebi.eva.accession.core.GenericApplication
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.*


// This script finds all UPDATE operation with _id starting with EVA3399 and modify the reason associated to match what
// is expected in a MERGE operation.
def cli = new CliBuilder()
cli.prodPropertiesFile(args: 1, "Production properties file to use for database connection", required: true)

def options = cli.parse(args)
if (!options) {
    cli.usage()
    throw new Exception("Invalid command line options provided!")
}


// this is equivalent to if __name__ == '__main__' in Python
if (this.getClass().getName().equals('eva3623.eva3623_correct_svoe_reason')) {
    def prodEnv = createFromSpringContext(options.prodPropertiesFile, GenericApplication.class)
    new CorrectSSOperationFromEVA3399(prodEnv).correctSSOperations()


}
