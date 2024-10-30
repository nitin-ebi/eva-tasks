package eva3660

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Updates
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.util.Pair
import uk.ac.ebi.eva.commons.models.data.Variant
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo
import  uk.ac.ebi.eva.pipeline.io.processors.VariantStatsProcessor

import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class RemediationApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(RemediationApplication.class)
    private static long counter = 0

    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    // Search for IDs that have either ref or alt longer than 1 base
    // Ignore non-ACGTN alleles (*, <>), but do include alleles longer than 50 bases (encoded ids)
    public static final String REGEX_PATTERN = "" +
            "([^_<>*\\s]{2,}" + // ref at least 2 chars
            "_[^_<>*\\s]*" +    // alt can be any length
            "\$)" +             // anchor on end of string
            "|" +               // OR
            "([^_<>*\\s]*" +    // ref any length
            "_[^_<>*\\s]{2,}" + // alt at least 2 chars
            "\$)"               // anchor on end of string

    @Autowired
    MongoTemplate mongoTemplate

    private static Map<String, Integer> sidFidNumberOfSamplesMap = new HashMap<>()
    private static VariantStatsProcessor variantStatsProcessor = new VariantStatsProcessor()

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]

        populateFilesIdAndNumberOfSamplesMap()

        // workaround to not saving a field by the name _class (contains the java class name) in the document
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(VARIANTS_COLLECTION)

        Query regexQuery = new Query(where("_id").regex(REGEX_PATTERN))
        def mongoCursor = variantsColl.find(regexQuery.getQueryObject()).noCursorTimeout(true).iterator()

        // Iterate through each variant one by one
        while (mongoCursor.hasNext()) {
            counter++
            if (counter % 10000 == 0) {
                logger.info("Processed {} variants", counter)
            }
            Document candidateDocument = mongoCursor.next()
            VariantDocument variantDocument
            try {
                // read the variant as a VariantDocument
                variantDocument = mongoTemplate.getConverter().read(VariantDocument.class, candidateDocument)
                logger.info("Processing Variant: {}", variantDocument)
            } catch (Exception e) {
                logger.error("Exception while converting Bson document to Variant Document with _id: {} " +
                        "chr {} start {} ref {} alt {}. Exception: {}", candidateDocument.get("_id"),
                        candidateDocument.get("chr"), candidateDocument.get("start"), candidateDocument.get("ref"),
                        candidateDocument.get("alt"), e.getMessage())
                continue
            }

            // filter out false positives
            if (!isCandidateForNormalisation(variantDocument)) {
                continue
            }

            logger.info("Variant might require normalisation. variant_id: {}, variant_ref: {}, variant_alt: {}",
                    variantDocument.getId(), variantDocument.getReference(), variantDocument.getAlternate())
            // TODO run normalisation (norm alg + truncate at most 1 leading context base)
            //  Needs to apply to all alternates including "secondary alternates" in sources

            // TODO create new id of variant
            String newId = ""  //VariantDocument.buildVariantId(variantDocument.getChromosome(), variantDocument.getStart(),
//                    variantDocument.getReference().toUpperCase(), variantDocument.getAlternate().toUpperCase())
            logger.info("New id of the variant : {}", newId)

            // check if new id is present in db and get the corresponding variant
            Query idQuery = new Query(where("_id").is(newId))
            VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

            // Check if there exists a variant in db that has the same id as newID
            if (variantInDB == null) {
                logger.warn("Variant with id {} not found in DB", newId)
                remediateCaseNoIdCollision(variantDocument, newId)
                continue
            }

            logger.info("Found existing variant in DB with id: {} {}", newId, variantInDB)
            // variant with new db present, needs to check for merging
            Set<VariantSourceEntity> remediatedVariantFileSet = variantDocument.getVariantSources() != null ?
                    variantDocument.getVariantSources() : new HashSet<>()
            Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                    variantInDB.getVariantSources() : new HashSet<>()
            Set<Pair> remediatedSidFidPairSet = remediatedVariantFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())
            Set<Pair> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())

            // take the common pair of sid-fid between the remediated variant and the variant in db
            Set<Pair> commonSidFidPairs = new HashSet<>(remediatedSidFidPairSet)
            commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

            if (commonSidFidPairs.isEmpty()) {
                logger.info("No common sid fid entries between remediated variant and variant in DB")
                remediateCaseMergeAllSidFidAreDifferent(variantInDB, variantDocument, newId)
                continue
            }

            // check if there is any pair of sid and fid from common pairs, for which there are more than one entry in files collection
            Map<Pair, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
            Set<Pair> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(entry -> entry.getKey())
                    .collect(Collectors.toSet())
            if (sidFidPairsWithGTOneEntry.isEmpty()) {
                logger.info("All common sid fid entries has only one file entry")
                Set<Pair> sidFidPairNotInDB = new HashSet<>(remediatedSidFidPairSet)
                sidFidPairNotInDB.removeAll(commonSidFidPairs)
                remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, variantDocument, sidFidPairNotInDB, newId)
                continue
            }

            logger.info("can't merge as sid fid common pair has more than 1 entry in file")
            remediateCaseCantMerge(workingDir, sidFidPairsWithGTOneEntry, dbName, variantDocument)
        }

        // Finished processing
        System.exit(0)
    }

    void remediateCaseNoIdCollision(VariantDocument originalVariant, String newId) {
        logger.info("case no id collision - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(originalVariant.getVariantSources())
        Set<VariantStatsMongo> remediatedStats = getRemediatedStats(originalVariant.getVariantStatsMongo())

        VariantDocument remediatedVariant = new VariantDocument(originalVariant.getVariantType(), originalVariant.getChromosome(),
                originalVariant.getStart(), originalVariant.getEnd(), originalVariant.getLength(), originalVariant.getReference(),
                originalVariant.getAlternate(), null, originalVariant.getIds(), remediatedFiles)
        remediatedVariant.setStats(remediatedStats)

        // insert updated variant and delete the existing one
        logger.info("case no id collision - insert new variant: {}", remediatedVariant)
        mongoTemplate.save(remediatedVariant, VARIANTS_COLLECTION)
        logger.info("case no id collision - delete original variant: {}", originalVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalVariant.getId())), VARIANTS_COLLECTION)

        // remediate Annotations
        remediateAnnotations(originalVariant.getId(), newId)
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument originalVariant, String newId) {
        logger.info("case merge all sid fid are different - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(originalVariant.getVariantSources())
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", remediatedFiles.stream()
                        .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

        logger.info("case merge all sid fid are different - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all sid fid are different - delete original variant: {}", originalVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(originalVariant.getId(), newId)
    }

    void remediateCaseCantMerge(String workingDir, Set<Pair> sidFidPairsWithGtOneEntry, String dbName, VariantDocument originalVariant) {
        logger.info("case can't merge variant - processing variant: {}", originalVariant)

        String nmcDirPath = Paths.get(workingDir, "non_merged_candidates").toString()
        File nmcDir = new File(nmcDirPath)
        if (!nmcDir.exists()) {
            nmcDir.mkdirs()
        }
        String nmcFilePath = Paths.get(nmcDirPath, dbName + ".txt").toString()
        try (BufferedWriter nmcFile = new BufferedWriter(new FileWriter(nmcFilePath, true))) {
            for (Pair<String, String> p : sidFidPairsWithGtOneEntry) {
                nmcFile.write(p.getFirst() + "," + p.getSecond() + "," + originalVariant.getId() + "\n")
            }
        } catch (IOException e) {
            logger.error("error writing case variant can't be merged in the file:  {}", originalVariant)
        }
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB,
                                                     VariantDocument originalVariant, Set<Pair> sidFidPairNotInDB, String newId) {
        logger.info("case merge all common sid fid has one file - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> candidateFiles = originalVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairNotInDB.contains(new Pair(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())
        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(candidateFiles)

        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", getRemediatedFiles(candidateFiles)
                        .stream().map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

        logger.info("case merge all common sid fid has one file - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all common sid fid has one file - delete original variant: {}", originalVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(originalVariant.getId(), newId)
    }

    void remediateAnnotations(String originalVariantId, String newVariantId) {
        String escapedOriginalVariantId = Pattern.quote(originalVariantId)
        String escapedNewVariantId = Pattern.quote(newVariantId)
        // Fix associated annotations - remove the original one and insert remediated one if not present
        Query annotationsCombinedRegexQuery = new Query(
                new Criteria().orOperator(
                        where("_id").regex("^" + escapedOriginalVariantId + ".*"),
                        where("_id").regex("^" + escapedNewVariantId + ".*")
                )
        )
        try {
            List<Document> annotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(annotationsCombinedRegexQuery.getQueryObject())
                    .into(new ArrayList<>())
            Set<String> remediatedAnnotationIdSet = annotationsList.stream()
                    .filter(doc -> doc.get("_id").toString().startsWith(newVariantId))
                    .map(doc -> doc.get("_id"))
                    .collect(Collectors.toSet())
            Set<Document> originalAnnotationsSet = annotationsList.stream()
                    .filter(doc -> doc.get("_id").toString().startsWith(originalVariantId))
                    .collect(Collectors.toSet())
            for (Document annotation : originalAnnotationsSet) {
                // if corresponding remediated annotation is already present skip it else insert it
                String originalAnnotationId = annotation.get("_id")
                String newAnnotationId = originalAnnotationId.replace(originalVariantId, newVariantId)
                if (!remediatedAnnotationIdSet.contains(newAnnotationId)) {
                    annotation.put("_id", newAnnotationId)
                    logger.info("insert new annotation: {}", annotation)
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(annotation)
                }
                // delete the original annotation
                logger.info("delete original annotation: {}", originalAnnotationId)
                mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalAnnotationId)), ANNOTATIONS_COLLECTION)
            }
        } catch (BsonSerializationException ex) {
            logger.error("Exception occurred while trying to remediate annotation for variant: {}", originalVariantId)
        }
    }

    private void populateFilesIdAndNumberOfSamplesMap() {
        def projectStage = Aggregates.project(Projections.fields(
                Projections.computed("sid_fid", new Document("\$concat", Arrays.asList("\$sid", "_", "\$fid"))),
                Projections.computed("numOfSamples", new Document("\$size", new Document("\$objectToArray", "\$samp")))
        ))
        def groupStage = Aggregates.group("\$sid_fid",
                Accumulators.sum("totalNumOfSamples", "\$numOfSamples"),
                Accumulators.sum("count", 1))

        def filterStage = Aggregates.match(Filters.eq("count", 1))

        sidFidNumberOfSamplesMap = mongoTemplate.getCollection(RemediationApplication.FILES_COLLECTION)
                .aggregate(Arrays.asList(projectStage, groupStage, filterStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap({ doc -> doc.getString("_id") }, { doc -> doc.getInteger("totalNumOfSamples") }))
    }


    Map<Pair, Integer> getSidFidPairNumberOfDocumentsMap(Set<Pair> commonSidFidPairs) {
        List<Bson> filterConditions = new ArrayList<>()
        for (Pair sidFidPair : commonSidFidPairs) {
            filterConditions.add(Filters.and(Filters.eq(RemediationApplication.FILES_COLLECTION_STUDY_ID_KEY, sidFidPair.getFirst()),
                    Filters.eq(RemediationApplication.FILES_COLLECTION_FILE_ID_KEY, sidFidPair.getSecond())))
        }
        Bson filter = Filters.or(filterConditions)

        Map<Pair, Integer> sidFidPairCountMap = mongoTemplate.getCollection(FILES_COLLECTION).find(filter)
                .into(new ArrayList<>()).stream()
                .map(doc -> new Pair(doc.get(RemediationApplication.FILES_COLLECTION_STUDY_ID_KEY),
                        doc.get(RemediationApplication.FILES_COLLECTION_FILE_ID_KEY)))
                .collect(Collectors.toMap(pair -> pair, count -> 1, Integer::sum))

        return sidFidPairCountMap
    }

    Set<VariantStatsMongo> getRemediatedStats(Set<VariantStatsMongo> variantStatsSet) {
        Set<VariantStatsMongo> variantStatsMongoSet = new HashSet<>()
        for (VariantStatsMongo stats : variantStatsSet) {
            variantStatsMongoSet.add(new VariantStatsMongo(stats.getStudyId(), stats.getFileId(), stats.getCohortId(),
                    stats.getMaf(), stats.getMgf(),
                    // TODO update with correct maf allele
                    stats.getMafAllele() != null ? stats.getMafAllele().toUpperCase() : stats.getMafAllele(),
                    stats.getMgfGenotype(),
                    stats.getMissingAlleles(), stats.getMissingGenotypes(), stats.getNumGt()))
        }

        return variantStatsMongoSet
    }

    Set<VariantSourceEntryMongo> getRemediatedFiles(Set<VariantSourceEntryMongo> variantSourceSet) {
        Set<VariantSourceEntryMongo> variantSourceEntryMongoSet = new HashSet<>()
        for (VariantSourceEntryMongo varSource : variantSourceSet) {
            // TODO constructor uppercases but does not normalise
            //  Also do we need to normalise all alternates here?
            variantSourceEntryMongoSet.add(new VariantSourceEntryMongo(varSource.getFileId(), varSource.getStudyId(),
                    varSource.getAlternates(), (BasicDBObject) varSource.getAttrs(), varSource.getFormat(), (BasicDBObject) varSource.getSampleData()))
        }

        return variantSourceEntryMongoSet
    }

    boolean isCandidateForNormalisation(VariantDocument variant) {
        return (variant.getVariantType() != Variant.VariantType.SNV &&
                (variant.getReference().size() > 1 || variant.getAlternate().size() > 1))
    }

}
