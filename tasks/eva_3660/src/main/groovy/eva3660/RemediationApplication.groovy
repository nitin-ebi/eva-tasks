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
import uk.ac.ebi.eva.commons.models.data.Variant
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo

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

        // TODO find fasta file - either a parameter or a search...
        NormalisationProcessor normaliser = new NormalisationProcessor(Paths.get("fasta.fa"))

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
            VariantDocument originalVariant
            try {
                // read the variant as a VariantDocument
                originalVariant = mongoTemplate.getConverter().read(VariantDocument.class, candidateDocument)
                logger.info("Processing Variant: {}", originalVariant)
            } catch (Exception e) {
                logger.error("Exception while converting Bson document to Variant Document with _id: {} " +
                        "chr {} start {} ref {} alt {}. Exception: {}", candidateDocument.get("_id"),
                        candidateDocument.get("chr"), candidateDocument.get("start"), candidateDocument.get("ref"),
                        candidateDocument.get("alt"), e.getMessage())
                continue
            }

            // filter out false positives
            if (!isCandidateForNormalisation(originalVariant)) {
                continue
            }
            logger.info("Variant might require normalisation. variant_id: {}, variant_ref: {}, variant_alt: {}",
                    originalVariant.getId(), originalVariant.getReference(), originalVariant.getAlternate())

            // Each source file has its own list of secondary alternates, these need to be normalised indpendently.
            // This also means that potentially more than one normalised document might be generated from one original
            // document (and correspondingly multiple merges, etc.)
            // TODO how does this work given that we know sid,fid pairs are not unique?
            for (VariantSourceEntryMongo variantSource: originalVariant.variantSources) {
                List<String> secondaryAlternates =  variantSource.alternates as List<String>
                String mafAllele = null
                for (VariantStatsMongo variantStats: originalVariant.variantStatsMongo) {
                    // TODO get the _corresponding_ stats object & mafAllele
                }
            }

            // TODO determine whether split is required, handle the split

            // Normalise all alleles and truncate common leading context allele if present
            ValuesForNormalisation normalisedValues = normaliser.normaliseAndTruncate(originalVariant.chromosome,
                    new ValuesForNormalisation(originalVariant.start, originalVariant.end, originalVariant.length,
                            originalVariant.reference, originalVariant.alternate, mafAllele, secondaryAlternates))

            // create new id of variant
            String newId = VariantDocument.buildVariantId(originalVariant.chromosome, normalisedValues.start, normalisedValues.reference.toUpperCase(),
                    normalisedValues.alternate.toUpperCase())
            logger.info("New id of the variant : {}", newId)
            if (originalVariant.getId().equals(newId)) {
                logger.info("Variant did not change from normalisation, skipping")
                continue
            }

            // check if new id is present in db and get the corresponding variant
            Query idQuery = new Query(where("_id").is(newId))
            VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

            // Check if there exists a variant in db that has the same id as newID
            if (variantInDB == null) {
                logger.warn("Variant with id {} not found in DB", newId)
                remediateCaseNoIdCollision(originalVariant, newId, normalisedValues)
                continue
            }

            logger.info("Found existing variant in DB with id: {} {}", newId, variantInDB)
            // variant with new db present, needs to check for merging
            Set<VariantSourceEntity> remediatedVariantFileSet = originalVariant.getVariantSources() != null ?
                    originalVariant.getVariantSources() : new HashSet<>()
            Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                    variantInDB.getVariantSources() : new HashSet<>()
            Set<Tuple2> remediatedSidFidPairSet = remediatedVariantFileSet.stream()
                    .map(vse -> new Tuple2(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())
            Set<Tuple2> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                    .map(vse -> new Tuple2(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())

            // take the common pairs of sid-fid between the remediated variant and the variant in db
            Set<Tuple2> commonSidFidPairs = new HashSet<>(remediatedSidFidPairSet)
            commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

            if (commonSidFidPairs.isEmpty()) {
                logger.info("No common sid fid entries between remediated variant and variant in DB")
                remediateCaseMergeAllSidFidAreDifferent(variantInDB, originalVariant, newId, normalisedValues)
                continue
            }

            // check if there is any pair of sid and fid from common pair, for which there are more than one entry in files collection
            Map<Tuple2, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
            Set<Tuple2> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(entry -> entry.getKey())
                    .collect(Collectors.toSet())
            if (sidFidPairsWithGTOneEntry.isEmpty()) {
                logger.info("All common sid fid entries has only one file entry")
                Set<Tuple2> sidFidPairNotInDB = new HashSet<>(remediatedSidFidPairSet)
                sidFidPairNotInDB.removeAll(commonSidFidPairs)
                remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, originalVariant, sidFidPairNotInDB, newId, normalisedValues)
                continue
            }

            logger.info("can't merge as sid fid common pair has more than 1 entry in file")
            remediateCaseCantMerge(workingDir, sidFidPairsWithGTOneEntry, dbName, originalVariant)
        }

        // Finished processing
        System.exit(0)
    }

    void remediateCaseNoIdCollision(VariantDocument originalVariant, String newId, ValuesForNormalisation normalisedVals) {
        logger.info("case no id collision - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(originalVariant.getVariantSources(), normalisedVals)
        Set<VariantStatsMongo> remediatedStats = getRemediatedStats(originalVariant.getVariantStatsMongo(), normalisedVals)

        VariantDocument remediatedVariant = new VariantDocument(originalVariant.getVariantType(), originalVariant.getChromosome(),
                normalisedVals.start, normalisedVals.end, normalisedVals.length, normalisedVals.reference,
                normalisedVals.alternate, null, originalVariant.getIds(), remediatedFiles)
        remediatedVariant.setStats(remediatedStats)

        // insert updated variant and delete the existing one
        logger.info("case no id collision - insert new variant: {}", remediatedVariant)
        mongoTemplate.save(remediatedVariant, VARIANTS_COLLECTION)
        logger.info("case no id collision - delete original variant: {}", originalVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalVariant.getId())), VARIANTS_COLLECTION)

        // remediate Annotations
        remediateAnnotations(originalVariant.getId(), newId)
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument originalVariant,
                                                 String newId, ValuesForNormalisation normalisedVals) {
        logger.info("case merge all sid fid are different - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(originalVariant.getVariantSources(), normalisedVals)
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(normalisedVals.reference,
                normalisedVals.alternate, variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

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

    void remediateCaseCantMerge(String workingDir, Set<Tuple2> sidFidPairsWithGtOneEntry, String dbName,
                                VariantDocument originalVariant) {
        logger.info("case can't merge variant - processing variant: {}", originalVariant)

        String nmcDirPath = Paths.get(workingDir, "non_merged_candidates").toString()
        File nmcDir = new File(nmcDirPath)
        if (!nmcDir.exists()) {
            nmcDir.mkdirs()
        }
        String nmcFilePath = Paths.get(nmcDirPath, dbName + ".txt").toString()
        try (BufferedWriter nmcFile = new BufferedWriter(new FileWriter(nmcFilePath, true))) {
            for (Tuple2<String, String> p : sidFidPairsWithGtOneEntry) {
                nmcFile.write(p.v1 + "," + p.v2 + "," + originalVariant.getId() + "\n")
            }
        } catch (IOException e) {
            logger.error("error writing case variant can't be merged in the file:  {}", originalVariant)
        }
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB, VariantDocument originalVariant,
                                                     Set<Tuple2> sidFidPairsNotInDB, String newId,
                                                     ValuesForNormalisation normalisedVals) {
        logger.info("case merge all common sid fid has one file - processing variant: {}", originalVariant)

        Set<VariantSourceEntryMongo> candidateFiles = originalVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairsNotInDB.contains(new Tuple2(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())
        Set<VariantSourceEntryMongo> remediatedFiles = getRemediatedFiles(candidateFiles, normalisedVals)

        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", getRemediatedFiles(candidateFiles, normalisedVals)
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

        sidFidNumberOfSamplesMap = mongoTemplate.getCollection(FILES_COLLECTION)
                .aggregate(Arrays.asList(projectStage, groupStage, filterStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap({ doc -> doc.getString("_id") }, { doc -> doc.getInteger("totalNumOfSamples") }))
    }


    Map<Tuple2, Integer> getSidFidPairNumberOfDocumentsMap(Set<Tuple2> commonSidFidPairs) {
        List<Bson> filterConditions = new ArrayList<>()
        for (Tuple2 sidFidPair : commonSidFidPairs) {
            filterConditions.add(Filters.and(Filters.eq(FILES_COLLECTION_STUDY_ID_KEY, sidFidPair.v1),
                    Filters.eq(FILES_COLLECTION_FILE_ID_KEY, sidFidPair.v2)))
        }
        Bson filter = Filters.or(filterConditions)

        Map<Tuple2, Integer> sidFidPairCountMap = mongoTemplate.getCollection(FILES_COLLECTION).find(filter)
                .into(new ArrayList<>()).stream()
                .map(doc -> new Tuple2(doc.get(FILES_COLLECTION_STUDY_ID_KEY),
                        doc.get(FILES_COLLECTION_FILE_ID_KEY)))
                .collect(Collectors.toMap(Tuple2 -> Tuple2, count -> 1, Integer::sum))

        return sidFidPairCountMap
    }

    Set<VariantStatsMongo> getRemediatedStats(Set<VariantStatsMongo> variantStatsSet, ValuesForNormalisation normalisedVals) {
        Set<VariantStatsMongo> variantStatsMongoSet = new HashSet<>()
        for (VariantStatsMongo stats : variantStatsSet) {
            variantStatsMongoSet.add(new VariantStatsMongo(stats.getStudyId(), stats.getFileId(), stats.getCohortId(),
                    stats.getMaf(), stats.getMgf(),
                    stats.getMafAllele() != null ? normalisedVals.mafAllele : stats.getMafAllele(),
                    stats.getMgfGenotype(),
                    stats.getMissingAlleles(), stats.getMissingGenotypes(), stats.getNumGt()))
        }

        return variantStatsMongoSet
    }

    Set<VariantSourceEntryMongo> getRemediatedFiles(Set<VariantSourceEntryMongo> variantSourceSet, ValuesForNormalisation normalisedVals) {
        Set<VariantSourceEntryMongo> variantSourceEntryMongoSet = new HashSet<>()
        for (VariantSourceEntryMongo varSource : variantSourceSet) {
            variantSourceEntryMongoSet.add(new VariantSourceEntryMongo(varSource.getFileId(), varSource.getStudyId(),
                    normalisedVals.secondaryAlternates as String[], (BasicDBObject) varSource.getAttrs(),
                    varSource.getFormat(), (BasicDBObject) varSource.getSampleData()))
        }

        return variantSourceEntryMongoSet
    }

    boolean isCandidateForNormalisation(VariantDocument variant) {
        return (variant.getVariantType() != Variant.VariantType.SNV &&
                (variant.getReference().size() > 1 || variant.getAlternate().size() > 1))
    }

}
