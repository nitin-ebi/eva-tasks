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
        assertEquals(new Tuple2(7678481L, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, ["ATTTATTT", "ATTT"]))

        // Same variant with different representation (empty allele)
        assertEquals(new Tuple2(7678481L, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, ["ATTT", ""]))

        // Multiple alternate alleles
        assertEquals(new Tuple2(7678481L, ["TTTTA", "T", "TTTTATTTA"]),
                processor.normalise("CM000683.2", 7678489, ["ATTTATTT", "ATTT", "ATTTATTTATTT"]))
    }

    @Test
    void testNormaliseAndTruncate() {
        // TODO rewrite this
        // Same test as above (testNormalise) but with specified ref & primary alt
//        assertEquals(new Tuple(7678481L, "TTTTA", "T", ["TTTTATTTA"]),
//                processor.normalise("CM000683.2", 7678489, "ATTTATTT", "ATTT", ["ATTTATTTATTT"]))
    }

    @Test
    void testNormalisationComparedToBcftoolsNorm() {
        // TODO
    }

}