package eva3660

import uk.ac.ebi.eva.accession.core.contig.ContigMapping
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms

import java.nio.file.Path

import static org.springframework.util.StringUtils.hasText;


class ContigNotFoundException extends IllegalArgumentException {
    ContigNotFoundException(String message) {
        super(message)
    }
}


class ContigRenamingProcessor {

    private ContigMapping contigMapping

    ContigRenamingProcessor(Path pathToAssemblyReport) {
        contigMapping = new ContigMapping("file:" + pathToAssemblyReport.toAbsolutePath().toString())
    }

    String getInsdcAccession(String contigName) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName)

        StringBuilder message = new StringBuilder()
        if (isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return contigSynonyms.getGenBank()
        }
        throw new ContigNotFoundException(message.toString())
    }

    /* More lenient version of this method in accessioning pipeline:
     * https://github.com/EBIvariation/eva-accession/blob/master/eva-accession-core/src/main/java/uk/ac/ebi/eva/accession/core/contig/ContigMapping.java#L169
     */
    boolean isGenbankReplacementPossible(String contig, ContigSynonyms contigSynonyms, StringBuilder reason) {
        if (contigSynonyms == null) {
            reason.append("Contig '" + contig + "' was not found in the assembly report!");
            return false;
        }

        if (!hasText(contigSynonyms.getGenBank())) {
            reason.append("No Genbank equivalent found for contig '" + contig + "' in the assembly report");
            return false;
        }
        return true;
    }

}
