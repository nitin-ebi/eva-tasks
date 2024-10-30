package eva3660

import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader

import java.nio.file.Path

class NormalisationProcessor {

    private FastaSequenceReader fastaReader

    NormalisationProcessor(Path pathToFasta) {
        fastaReader = new FastaSequenceReader(pathToFasta)
    }

    /**
     * Normalisation with specified reference allele.
     *
     * @param contig Name of contig as found in FASTA
     * @param position Position of variant
     * @param reference Reference allele
     * @param alternates List of alternate alleles
     * @return normalised position, normalised ref and list of normalised alternates
     */
    Tuple normaliseWithRef(String contig, long position, String reference, List<String> alternates) {
        Tuple2<Long, List<String>> result = normalise(contig, position, [reference] + alternates)
        return new Tuple(result.v1, result.v2[0], result.v2[1..-1])
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
    Tuple2<Long, List<String>> normalise(String contig, long position, List<String> alleles) {
        // Allow for initially empty alleles
        def (newPosition, newAlleles) = addContextIfEmpty(contig, position, alleles)
        // While all alleles end in same nucleotide
        while (allAllelesSameEnd(newAlleles)) {
            // Truncate rightmost nucleotide
            newAlleles = newAlleles.stream().collect{ it.substring(0, it.size()-1) }
            // If exists an empty allele, extend alleles 1 to the left
            (newPosition, newAlleles) = addContextIfEmpty(contig, newPosition, newAlleles)
        }
        // While all alleles start with same nucleotide and have length 2 or more
        while (allAllelesSameStart(newAlleles) && allAllelesLengthAtLeastTwo(newAlleles)) {
            // Truncate leftmost nucleotide
            newAlleles = newAlleles.stream().collect{ it.substring(1) }
            newPosition++
        }
        return new Tuple2(newPosition, newAlleles)
    }

    private Tuple2<Long, List<String>> addContextIfEmpty(String contig, long position, List<String> alleles) {
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
        return new Tuple2(newPosition, newAlleles)
    }

    private boolean allAllelesSameEnd(List<String> alleles) {
        return alleles.stream().collect{it[-1] }.toSet().size() == 1
    }

    private boolean allAllelesSameStart(List<String> alleles) {
        return alleles.stream().collect{it[0] }.toSet().size() == 1
    }

    private boolean allAllelesLengthAtLeastTwo(List<String> alleles) {
        return alleles.stream().allMatch{it.size() >= 2 }
    }

    void close() {
        fastaReader.close()
    }

}
