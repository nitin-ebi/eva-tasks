package eva3660

import uk.ac.ebi.eva.accession.core.contig.ContigMapping
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms

import java.nio.file.Path

class ContigRenamingProcessor {

    private ContigMapping contigMapping

    ContigRenamingProcessor(Path pathToAssemblyReport) {
        contigMapping = new ContigMapping("file:" + pathToAssemblyReport.toAbsolutePath().toString())
    }

    String getInsdcAccession(String contigName) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName)

        StringBuilder message = new StringBuilder()
        // TODO this is quite strict, do we want to keep this logic?
        //  https://github.com/EBIvariation/eva-accession/blob/master/eva-accession-core/src/main/java/uk/ac/ebi/eva/accession/core/contig/ContigMapping.java#L169
        if (contigMapping.isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return contigSynonyms.getGenBank()
        }
        throw new IllegalArgumentException(message.toString())
    }

}
