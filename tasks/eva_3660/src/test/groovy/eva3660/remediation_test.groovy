package eva3660

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import org.bson.Document
import org.junit.jupiter.api.Test
import org.opencb.commons.utils.CryptoUtils
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.PropertiesPropertySource
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument

import java.nio.file.Paths
import java.util.regex.Matcher
import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

class RemediationApplicationIntegrationTest {
    private static String UPPERCASE_LARGE_SEQ = "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"

    // Description of a "real" variant on chr21 that does require normalisation
    private static final DB_NAME = "eva_hsapiens_grch38"
    private static final TYPE = "INDEL"
    private static final CHR = "chr21"
    private static final REF = "ATTTATTT"
    private static final ALT = "ATTT"
    private static final START = 7678489
    private static final END = 7678496
    private static final LENGTH = 8
    private static final NORM_REF = "TTTA"
    private static final NORM_ALT = ""
    private static final NORM_START = 7678482

    def setUpEnvAndRunRemediationWithQC(List<Document> filesData, List<Document> variantsData,
                                        List<Document> annotationsData, def qcMethod) {
        String resourceDir = "src/test/resources"
        String testPropertiesFile = resourceDir + "/application-test.properties"
        String workingDir = resourceDir + "/test_run"
        String fastaDir = resourceDir + "/fasta"
        File nmcFile = new File(Paths.get(workingDir, "/non_merged_candidates/", DB_NAME + ".txt").toString())
        File umaFile = new File(Paths.get(workingDir, "/unresolved_maf_allele/", DB_NAME + ".txt").toString())

        // Removing existing data and setup DB with test Data
        AnnotationConfigApplicationContext context = getApplicationContext(testPropertiesFile, DB_NAME)
        MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class)
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        mongoTemplate.getCollection(RemediationApplication.FILES_COLLECTION).drop()
        mongoTemplate.getCollection(RemediationApplication.VARIANTS_COLLECTION).drop()
        mongoTemplate.getCollection(RemediationApplication.ANNOTATIONS_COLLECTION).drop()
        if (nmcFile.exists()) {
            nmcFile.delete()
        }
        if (umaFile.exists()) {
            umaFile.delete()
        }
        if (filesData != null && !filesData.isEmpty()) {
            mongoTemplate.getCollection(RemediationApplication.FILES_COLLECTION).insertMany(filesData)
        }
        if (variantsData != null && !variantsData.isEmpty()) {
            mongoTemplate.getCollection(RemediationApplication.VARIANTS_COLLECTION).insertMany(variantsData)
        }
        if (annotationsData != null && !annotationsData.isEmpty()) {
            mongoTemplate.getCollection(RemediationApplication.ANNOTATIONS_COLLECTION).insertMany(annotationsData)
        }

        // Run Remediation
        RemediationApplication remediationApplication = context.getBean(RemediationApplication.class)
        remediationApplication.run(new String[]{workingDir, DB_NAME, fastaDir})

        // Run QC
        qcMethod.call(mongoTemplate, workingDir)

        context.close()
    }

    private AnnotationConfigApplicationContext getApplicationContext(String testPropertiesFile, String testDBName) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()
        def appProps = new Properties()
        appProps.load(new FileInputStream(new File(testPropertiesFile)))
        Map<String, Object> otherProperties = new HashMap<>()
        otherProperties.put("parameters.path", testPropertiesFile)
        otherProperties.put("spring.data.mongodb.database", testDBName)
        context.getEnvironment().getPropertySources().addLast(new PropertiesPropertySource("main", appProps))
        if (Objects.nonNull(otherProperties)) {
            context.getEnvironment().getPropertySources().addLast(new MapPropertySource("other", otherProperties))
        }
        context.register(RemediationApplication.class)
        context.refresh()

        return context
    }

    @Test
    void testRegex() {
        Pattern pattern = Pattern.compile(RemediationApplication.REGEX_PATTERN)
        String[] matchingStrings = new String[] {
                buildVariantId("chr1", 77777777, "AcG", "AgT"),
                buildVariantId("chr1", 77777777, "", "AgT"),
                buildVariantId("chr1", 77777777, "AcG", ""),
                buildVariantId("chr1", 77777777, "555", "555"),
                buildVariantId("chr1", 77777777, "A", UPPERCASE_LARGE_SEQ),
                buildVariantId("chr1", 77777777, UPPERCASE_LARGE_SEQ, "A"),
                buildVariantId("chr1", 77777777, UPPERCASE_LARGE_SEQ, UPPERCASE_LARGE_SEQ),
                buildVariantId("chr_1", 77777777, "AcG", "AgT"),
                buildVariantId("chr_1", 77777777, "", "AgT"),
                buildVariantId("chr_1", 77777777, "AcG", ""),
                buildVariantId("chr_1", 77777777, "555", "555"),
                buildVariantId("chr_1", 77777777, "A", UPPERCASE_LARGE_SEQ),
                buildVariantId("chr_1", 77777777, UPPERCASE_LARGE_SEQ, "A"),
                buildVariantId("chr_1", 77777777, UPPERCASE_LARGE_SEQ, UPPERCASE_LARGE_SEQ)
        }
        for (String str : matchingStrings) {
            Matcher matcher = pattern.matcher(str)
            assertTrue(matcher.find(), "Expected string to match: " + str)
        }

        String[] notMatchingStrings = new String[] {
                buildVariantId("chr1", 77777777, "A", "G"),
                buildVariantId("chr1", 77777777, "A", ""),
                buildVariantId("chr1", 77777777, "", "g"),
                buildVariantId("chr1", 77777777, "A", "*"),
                buildVariantId("chr1", 77777777, "A", "<INS>"),
                buildVariantId("chr_1", 77777777, "A", "G"),
                buildVariantId("chr_1", 77777777, "A", ""),
                buildVariantId("chr_1", 77777777, "", "g"),
                buildVariantId("chr_1", 77777777, "A", "*"),
                buildVariantId("chr_1", 77777777, "A", "<INS>")
        }
        for (String str : notMatchingStrings) {
            Matcher matcher = pattern.matcher(str)
            assertFalse(matcher.find(), "Expected string not to match: " + str)
        }
    }

    @Test
    void testGetFastaAndReportPaths() {
        assertEquals(new Tuple2(
                Paths.get("/path/to/fasta/mus_musculus/GCA_000001635.2/GCA_000001635.2.fa"),
                Paths.get("/path/to/fasta/mus_musculus/GCA_000001635.2/GCA_000001635.2_assembly_report.txt")),
                RemediationApplication.getFastaAndReportPaths("/path/to/fasta", "eva_mmusculus_grcm38"))
    }

    @Test
    void testNormalisationRemediation_caseMultipleStatsForSidFid() {
        // Single variant with multiple stats objects for the same sid/fid pair can't be remediated
        List<Document> variants = [getVariantDocument(TYPE, CHR, REF, ALT, START, END, LENGTH,
                [getVariantFiles("sid1", "fid1", null), getVariantFiles("sid1", "fid1", null)],
                [getVariantStats("sid1", "fid1", ALT), getVariantStats("sid1", "fid1", ALT)]
        )]
        List<Document> files = [getFileDocument("sid1", "fid1", "file1"),
                                getFileDocument("sid1", "fid1", "file2")]
        List<Document> annotations = [getAnnotationDocument(variants[0]["_id"])]

        setUpEnvAndRunRemediationWithQC(files, variants, annotations, this.&qc_caseMultipleStatsForSidFid)
    }

    void qc_caseMultipleStatsForSidFid(MongoTemplate mongoTemplate, String workingDir) {
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(RemediationApplication.VARIANTS_COLLECTION)
        MongoCollection<Document> annotationsColl = mongoTemplate.getCollection(RemediationApplication.ANNOTATIONS_COLLECTION)

        // No change for variant or annotation
        assertEquals(1, variantsColl.find(Filters.eq("_id", "${CHR}_${START}_${REF}_${ALT}".toString())).into([]).size())
        assertEquals(1, annotationsColl.find(Filters.eq("_id", "${CHR}_${START}_${REF}_${ALT}_82_82".toString())).into([]).size())

        // assert the issue is logged in the file
        File unresolvedMafAlleleFile = new File(Paths.get(workingDir, "unresolved_maf_allele", DB_NAME + ".txt").toString())
        assertTrue(unresolvedMafAlleleFile.exists())
        try (BufferedReader fileReader = new BufferedReader(new FileReader(unresolvedMafAlleleFile))) {
            assertEquals("sid1,fid1,${CHR}_${START}_${REF}_${ALT}".toString(), fileReader.readLine())
        }
    }

    @Test
    void testNormalisationRemediation_caseNoStatsForSidFid() {
        // Single variant with no stats object can be remediated
        List<Document> variants = [getVariantDocument(TYPE, CHR, REF, ALT, START, END, LENGTH,
                [getVariantFiles("sid1", "fid1", null)],
                null
        )]
        List<Document> files = [getFileDocument("sid1", "fid1", "file1")]
        List<Document> annotations = [getAnnotationDocument(variants[0]["_id"])]

        setUpEnvAndRunRemediationWithQC(files, variants, annotations, this.&qc_caseNoStatsForSidFid)
    }

    void qc_caseNoStatsForSidFid(MongoTemplate mongoTemplate, String workingDir) {
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(RemediationApplication.VARIANTS_COLLECTION)
        MongoCollection<Document> annotationsColl = mongoTemplate.getCollection(RemediationApplication.ANNOTATIONS_COLLECTION)

        // Updated variant and annotation
        assertEquals(0, variantsColl.find(Filters.eq("_id", "${CHR}_${START}_${REF}_${ALT}".toString())).into([]).size())
        assertEquals(1, variantsColl.find(Filters.eq("_id", "${CHR}_${NORM_START}_${NORM_REF}_${NORM_ALT}".toString())).into([]).size())
        assertEquals(0, annotationsColl.find(Filters.eq("_id", "${CHR}_${START}_${REF}_${ALT}_82_82".toString())).into([]).size())
        assertEquals(1, annotationsColl.find(Filters.eq("_id", "${CHR}_${NORM_START}_${NORM_REF}_${NORM_ALT}_82_82".toString())).into([]).size())
    }

    @Test
    void testNormalisationRemediation_caseNoNormalisationNeeded() {
        List<Document> variants = [
                // Variant is of type SNV and is skipped
                getVariantDocument("SNV", "chr21", "T", "G", 123, 123, 1,
                        [getVariantFiles("sid1", "fid1", null)],null),
                // Variant is normalised but id doesn't change
                getVariantDocument("INDEL", "chr21", "T", "GG", 123, 124, 2,
                        [getVariantFiles("sid1", "fid1", null)],null),
        ]
        List<Document> files = [getFileDocument("sid1", "fid1", "file1")]
        List<Document> annotations = [getAnnotationDocument(variants[0]["_id"])]

        setUpEnvAndRunRemediationWithQC(files, variants, annotations, this.&qc_caseNoNormalisationNeeded)
    }

    void qc_caseNoNormalisationNeeded(MongoTemplate mongoTemplate, String workingDir) {
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(RemediationApplication.VARIANTS_COLLECTION)
        MongoCollection<Document> annotationsColl = mongoTemplate.getCollection(RemediationApplication.ANNOTATIONS_COLLECTION)

        // No change for variant or annotation
        assertEquals(1, variantsColl.find(Filters.eq("_id", "chr21_123_T_G".toString())).into([]).size())
        assertEquals(1, variantsColl.find(Filters.eq("_id", "chr21_123_T_GG".toString())).into([]).size())
        assertEquals(1, annotationsColl.find(Filters.eq("_id", "chr21_123_T_G_82_82".toString())).into([]).size())
    }

    @Test
    void testNormalisationRemediation_caseSingleNormalisedDocument() {
        // TODO include stats and files with secondary alts, but no ID collisions
    }

    @Test
    void testNormalisationRemediation_caseMultipleNormalisedDocuments() {
        // TODO include stats and files with secondary alts, but no ID collisions
    }

    @Test
    void testNormalisationRemediation_caseIdCollision_noCommonSidFid() {
        // TODO test for just merge behaviour (no split)
    }

    @Test
    void testNormalisationRemediation_caseIdCollisionCommon_sidFidOneFile() {
        // TODO test for just merge behaviour (no split)
    }

    @Test
    void testNormalisationRemediation_caseIdCollisionCommon_sidFidMultipleFiles() {
        // TODO test for just merge behaviour (no split)
    }

    @Test
    void testNormalisationRemediation_splitAndMergeInteraction() {
        // TODO test one complicated case with split, merge & stats
    }

    String buildVariantId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome)
        builder.append("_")
        builder.append(start)
        builder.append("_")
        if (!reference.equals("-")) {
            if (reference.length() < 50) {
                builder.append(reference)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)))
            }
        }

        builder.append("_")
        if (!alternate.equals("-")) {
            if (alternate.length() < 50) {
                builder.append(alternate)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)))
            }
        }
        return builder.toString()
    }

    Document getFileDocument(String studyId, String fileId, String fileName) {
        return new Document()
                .append("sid", studyId)
                .append("fid", fileId)
                .append("fname", fileName)
                .append("samp", new Document("samp1", 0).append("samp2", 1).append("samp3", 2))
    }

    Document getAnnotationDocument(String variantId) {
        return new Document()
                .append("_id", variantId + "_82_82")
                .append("vepv", 82)
                .append("cachev", 82)
    }

    Document getVariantDocument(String variantType, String chromosome, String ref, String alt, int start,
                                int end, int length, List<Document> files, List<Document> stats) {
        return new Document()
                .append("_id", buildVariantId(chromosome, start, ref, alt))
                .append("type", variantType)
                .append("chr", chromosome)
                .append("ref", ref)
                .append("alt", alt)
                .append("start", start)
                .append("end", end)
                .append("len", length)
                .append("files", files)
                .append("st", stats)
    }

    Document getVariantFiles(String studyId, String fileId, List<String> secondaryAlternates) {
        return new Document()
                .append("sid", studyId)
                .append("fid", fileId)
                .append("alts", secondaryAlternates)
                .append("samp", new Document("def", "0|0").append("0|1", [1]))
    }

    Document getVariantStats(String studyId, String fileId, String mafAl) {
        return new Document()
                .append("sid", studyId)
                .append("fid", fileId)
                .append("mafAl", mafAl)
    }

}