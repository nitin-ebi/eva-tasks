package eva3660

import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader

import java.nio.file.Path

class NormalisationProcessor {

    private FastaSequenceReader fastaReader

    NormalisationProcessor(Path pathToFasta) {
        fastaReader = new FastaSequenceReader(pathToFasta)
    }

    /**
     * Normalisation with specified reference, alternate, secondary alternate alleles, and maf allele.
     * Also truncate common leading context allele if present (i.e. allows empty alleles).
     *
     * @param contig Name of contig as found in FASTA
     * @param valsforNorm Values required for normalisation (position & various alleles) Position of variant
     * @return normalised values
     */
    ValuesForNormalisation normaliseAndTruncate(String contig, ValuesForNormalisation valsForNorm) {
        // mafAllele can be null
        boolean mafIsNull = false
        List<String> allelesToNorm = [valsForNorm.reference, valsForNorm.alternate, valsForNorm.mafAllele] + valsForNorm.secondaryAlternates
        if (Objects.isNull(valsForNorm.mafAllele)) {
            mafIsNull = true
            allelesToNorm = [valsForNorm.reference, valsForNorm.alternate] + valsForNorm.secondaryAlternates
        }
        def (newPosition, newAlleles) = normalise(contig, valsForNorm.position, allelesToNorm)
        if (allSameStart(newAlleles)) {
            newAlleles = truncateLeftmost(newPosition, newAlleles)
        }
        if (mafIsNull) {
            return new ValuesForNormalisation(newPosition, newAlleles[0], newAlleles[1], null, newAlleles[3..-1])
        }
        return new ValuesForNormalisation(newPosition, newAlleles[0], newAlleles[1], newAlleles[2], newAlleles[3..-1])
    }

    /**
     * Normalise alleles to be parsimonious and left-aligned.
     * See here: https://genome.sph.umich.edu/wiki/Variant_Normalization
     *
     * @param contig Name of contig as found in FASTA
     * @param position Position of variant
     * @param alleles List of alleles
     * @return normalised position and list of normalised alleles (guaranteed to preserve input order)
     */
    Tuple normalise(String contig, int position, List<String> alleles) {
        // Allow for initially empty alleles
        def (newPosition, newAlleles) = addContextIfEmpty(contig, position, alleles)
        // While all alleles end in same nucleotide
        while (allSameEnd(newAlleles)) {
            // Truncate rightmost nucleotide
            newAlleles = truncateRightmost(newAlleles)
            // If exists an empty allele, extend alleles 1 to the left
            (newPosition, newAlleles) = addContextIfEmpty(contig, newPosition, newAlleles)
        }
        // While all alleles start with same nucleotide and have length 2 or more
        while (allSameStart(newAlleles) && allLengthAtLeastTwo(newAlleles)) {
            // Truncate leftmost nucleotide
            (newPosition, newAlleles) = truncateLeftmost(newPosition, newAlleles)
        }
        return new Tuple(newPosition, newAlleles)
    }

    private Tuple addContextIfEmpty(String contig, int position, List<String> alleles) {
        def newPosition = position
        def newAlleles = alleles
        def existEmptyAlleles = alleles.stream().any{it.size() }
        if (existEmptyAlleles) {
            // Extend alleles 1 to the left
            newPosition--
            // Note VCF specifies what to do if position starts at 1, but AFAICT the normalisation algorithm does not
            // See vt implementation: https://github.com/atks/vt/blob/master/variant_manip.cpp#L513
            def contextBase = fastaReader.getSequenceToUpperCase(contig, newPosition, newPosition)
            newAlleles = newAlleles.stream().collect { "${contextBase}${it}" }
        }
        return new Tuple(newPosition, newAlleles)
    }

    private boolean allSameEnd(List<String> alleles) {
        return alleles.stream().collect{it[-1] }.toSet().size() == 1
    }

    private boolean allSameStart(List<String> alleles) {
        return alleles.stream().collect{it[0] }.toSet().size() == 1
    }

    private boolean allLengthAtLeastTwo(List<String> alleles) {
        return alleles.stream().allMatch{it.size() >= 2 }
    }

    private List<String> truncateRightmost(List<String> alleles) {
        return alleles.stream().collect{ it.substring(0, it.size()-1) }
    }

    private Tuple truncateLeftmost(int position, List<String> alleles) {
        return new Tuple(++position, alleles.stream().collect{ it.substring(1) })
    }

    void close() {
        fastaReader.close()
    }

}
