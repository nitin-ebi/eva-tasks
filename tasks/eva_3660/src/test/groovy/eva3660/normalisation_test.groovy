package eva3660

import org.junit.After
import org.junit.Before
import org.junit.Test

import java.nio.file.Paths

import static org.junit.Assert.assertEquals


class NormalisationTest {

    private NormalisationProcessor processor

    @Before
    void setUp() throws Exception {
        processor = new NormalisationProcessor(Paths.get(
                NormalisationTest.class.getResource("/grch38_chr21.fa").toURI()))
    }

    @After
    void tearDown() throws Exception {
        processor.close()
    }

    @Test
    void testNormalise() {
        // Section of chr21 consisting of ATTT repeats
        assertEquals("TTTTATTTATTTATTTATTTATTTATTTATTTATTTATTT",
                processor.fastaReader.getSequenceToUpperCase("CM000683.2", 7678481, 7678480 + (10*4)))

        // Variant that deletes one repeat from the middle is normalised to be left-aligned
        assertEquals(new Tuple(7678481, 7678485, 5, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, 7678496, 8, ["ATTTATTT", "ATTT"]))

        // Same variant with different representation (empty allele)
        assertEquals(new Tuple(7678481, 7678485, 5, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, 7678492, 4, ["ATTT", ""]))

        // Multiple alternate alleles
        assertEquals(new Tuple(7678481, 7678489, 9, ["TTTTA", "T", "TTTTATTTA"]),
                processor.normalise("CM000683.2", 7678489, 7678500, 12, ["ATTTATTT", "ATTT", "ATTTATTTATTT"]))
    }

    @Test
    void testNormaliseAndTruncate() {
        def input = new ValuesForNormalisation(7678489, 7678500, 12, "ATTTATTT", "ATTT", "ATTT", ["ATTTATTTATTT"])
        // Initial T is truncated
        def expectedOutput = new ValuesForNormalisation(7678482, 7678489, 8, "TTTA", "", "", ["TTTATTTA"])
        assertEquals(expectedOutput, processor.normaliseAndTruncate("CM000683.2", input))

        // Allows null mafAllele and no secondary alternates
        input = new ValuesForNormalisation(7678489, 7678500, 12, "ATTTATTT", "ATTT", null, [])
        expectedOutput = new ValuesForNormalisation(7678482, 7678489, 8, "TTTA", "",null, [])
        assertEquals(expectedOutput, processor.normaliseAndTruncate("CM000683.2", input))
    }

    @Test
    void testNormalisationComparedToBcftoolsNorm() {
        // TODO Check especially for:
        //  - variants at position 1
        //  - multiple alleles on one line
    }

}