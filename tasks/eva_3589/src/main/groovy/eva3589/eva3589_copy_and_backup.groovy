package eva3589

import groovy.cli.picocli.CliBuilder
import uk.ac.ebi.eva.accession.core.GenericApplication

import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.*


def cli = new CliBuilder()
cli.mergeCandidateFile(args: 1, "file containing the RS merge candidates (single hash multiple RS)", required: true)
cli.splitCandidateFile(args: 1, "file containing the RS split candidates (single RS multiple hash)", required: true)
cli.assemblyAccession(args: 1, "Assembly accession where the submitted variant should be found", required: true)
cli.devPropertiesFile(args: 1, "Properties file to use for the dev database connection", required: true)
cli.prodPropertiesFile(args: 1, "Properties file to use for the prod database connection", required: true)


def options = cli.parse(args)
if (!options) {
    cli.usage()
    throw new Exception("Invalid command line options provided!")
}

static def getRsidsFromMergeCandidatesFile(String filename) {
    def inputStream = new File(filename).newInputStream()
    def rsids = []
    inputStream.eachLine { line ->
        def rsidsInLine = line.split("\\t").tail().collect({ it.toLong() })
        rsids.addAll(rsidsInLine)
    }
    inputStream.close()
    return rsids
}

static def getRsidsFromSplitCandidatesFile(String filename) {
    def inputStream = new File(filename).newInputStream()
    def rsids = []
    inputStream.eachLine { line ->
        def rsidInLine = line.split("\\t").head().toLong()
        rsids.add(rsidInLine)
    }
    inputStream.close()
    return rsids
}

// this is equivalent to if __name__ == '__main__' in Python
if (this.getClass().getName().equals('eva3589.eva3589_copy_and_backup')) {
    def devEnv = createFromSpringContext(options.devPropertiesFile, GenericApplication.class)
    def prodEnv = createFromSpringContext(options.prodPropertiesFile, GenericApplication.class)

    // Get all the RS IDs in the candidates files
    def rsidsInMerge = getRsidsFromMergeCandidatesFile(options.mergeCandidateFile)
    def rsidsInSplit = getRsidsFromSplitCandidatesFile(options.splitCandidateFile)
    def rsidsUniqued = new HashSet<Long>(rsidsInMerge)
    rsidsUniqued.addAll(rsidsInSplit)
    def allRsids = new ArrayList<>(rsidsUniqued)

    logger.info("Got ${allRsids.size()} RS IDs from files")

    // Copy the relevant data and back it up.
    def dataset = new TestDataset(options.assemblyAccession, prodEnv, devEnv)
    dataset.copyDataToDev(allRsids)
    dataset.backupDevCollections()
}

