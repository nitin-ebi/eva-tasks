package eva3660

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import groovy.json.JsonSlurper
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
    private static NormalisationProcessor normaliser = null
    private static ContigRenamingProcessor contigRenamer = null
    private static String workingDir = ""
    private static String dbName = ""
    private static String fastaDir = ""

    @Override
    void run(String... args) throws Exception {
        workingDir = args[0]
        dbName = args[1]
        fastaDir = args[2]

        def (fastaPath, assemblyReportPath) = getFastaAndReportPaths(fastaDir, dbName)
        normaliser = new NormalisationProcessor(fastaPath)
        contigRenamer = new ContigRenamingProcessor(assemblyReportPath)

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

            // Each source file has its own list of secondary alternates, these need to be normalised independently.
            // This also means that potentially more than one normalised document might be generated from one original
            // document, as the final normalised ID might differ depending on the secondary alternates.
            Map<String, VariantDocument> variantIdToDocument = [:]
            for (VariantSourceEntryMongo variantSource : originalVariant.getVariantSources()) {
                remediateVariantForFile(originalVariant, variantSource, variantIdToDocument)
            }

            // At this point, we have one or more variant IDs (which may be same as the original ID)
            // We proceed with the rest of the remediation for each of these independently
            for (String newId : variantIdToDocument.keySet()) {
                logger.info("Processing new ID : {}", newId)
                if (originalVariant.getId().equals(newId)) {
                    logger.info("Variant did not change from normalisation, skipping")
                    continue
                }
                insertOrMergeRemediateVariant(originalVariant.getId(), newId, variantIdToDocument[newId])
            }
        }
    }

    void remediateVariantForFile(VariantDocument originalVariant, VariantSourceEntryMongo variantSource,
                                 Map<String, VariantDocument> variantIdToDocument) {
        String fid = variantSource.getFileId()
        String sid = variantSource.getStudyId()
        List<String> secondaryAlternates = variantSource.getAlternates() as List<String>
        if (secondaryAlternates == null) {
            secondaryAlternates = []
        }
        VariantStatsMongo variantStats = null
        String mafAllele = null
        List<VariantStatsMongo> variantStatsWithFidAndSid = originalVariant.getVariantStatsMongo().stream().filter {
            it.getFileId() == fid && it.getStudyId() == sid
        }.collect(Collectors.toList())
        // If we can't resolve which stats to use based on fid & sid, can't remediate
        // TODO ideally we would also check:
        //  - are the stats objects (particularly mafAllele) actually identical?
        //  - would normalisation would actually have changed anything? (hard to say without know what the mafALlele is)
        if (variantStatsWithFidAndSid.size() > 1) {
            logger.error("Found multiple stats objects for ({}, {}), skipping", sid, fid)
            writeFailureToResolveMafAllele(sid, fid, originalVariant.getId())
            return
        }
        // If we found exactly one stats object corresponding to fid & sid, we can proceed with remediation
        if (variantStatsWithFidAndSid.size() == 1) {
            variantStats = variantStatsWithFidAndSid.pop()
            mafAllele = variantStats.getMafAllele()
        }
        // If no stats found with fid and sid of this source, assume stats were not computed and continue with
        // remediation
        // If we found a mafAllele and it's not consistent with any alternates, something's wrong
        if (mafAllele != null && (mafAllele != originalVariant.getAlternate()
                && !secondaryAlternates.contains(mafAllele))) {
            logger.error("mafAllele {} for ({}, {}) not found among any alternates {} or {}, skipping",
                    mafAllele, sid, fid, originalVariant.getAlternate(), secondaryAlternates)
            writeFailureToResolveMafAllele(sid, fid, originalVariant.getId())
            return
        }

        // Convert the contig name to INSDC
        // Used ONLY in normalisation, the original contig name will be retained in the DB
        String insdcContig = contigRenamer.getInsdcAccession(originalVariant.getChromosome())

        // Normalise all alleles and truncate common leading context allele if present
        ValuesForNormalisation normalisedValues = normaliser.normaliseAndTruncate(insdcContig,
                new ValuesForNormalisation(originalVariant.getStart(), originalVariant.getEnd(), originalVariant.getLength(),
                        originalVariant.getReference(), originalVariant.getAlternate(), mafAllele, secondaryAlternates))

        // Create new variantId, file subdocument, and stats subdocument
        String remediatedId = VariantDocument.buildVariantId(originalVariant.getChromosome(),
                normalisedValues.getStart(), normalisedValues.getReference(), normalisedValues.getAlternate())
        VariantSourceEntryMongo remediatedFile = new VariantSourceEntryMongo(fid, sid,
                normalisedValues.getSecondaryAlternates() as String[],
                (BasicDBObject) variantSource.getAttrs(), variantSource.getFormat(),
                (BasicDBObject) variantSource.getSampleData())
        VariantStatsMongo remediatedStats = variantStats == null ? null : new VariantStatsMongo(sid, fid,
                variantStats.getCohortId(), variantStats.getMaf(), variantStats.getMgf(),
                variantStats.getMafAllele() != null ? normalisedValues.getMafAllele() : null,
                variantStats.getMgfGenotype(), variantStats.getMissingAlleles(),
                variantStats.getMissingGenotypes(), variantStats.getNumGt())

        if (variantIdToDocument.containsKey(remediatedId)) {
            // Need to add this fid/sid's files and stats subdocuments to the variant already in the map
            VariantDocument variantInMap = variantIdToDocument[remediatedId]
            variantInMap.setSources(variantInMap.getVariantSources() | ([remediatedFile] as Set))
            variantInMap.setStats(variantInMap.getVariantStatsMongo() | ([remediatedStats] as Set))
        } else {
            // Create a new remediated document, containing just the files and stats subdocuments with this
            // fid/sid pair
            VariantDocument remediatedVariant = new VariantDocument(
                    originalVariant.getVariantType(), originalVariant.getChromosome(),
                    normalisedValues.start, normalisedValues.end, normalisedValues.length,
                    normalisedValues.reference, normalisedValues.alternate, new HashSet<>(), originalVariant.getIds(),
                    [remediatedFile] as Set
            )
            if (remediatedStats != null) {
                remediatedVariant.setStats([remediatedStats] as Set)
            }
            variantIdToDocument[remediatedId] = remediatedVariant
        }
    }

    void insertOrMergeRemediateVariant(String originalId, String remediatedId, VariantDocument remediatedVariant) {
        // check if new id is present in db and get the corresponding variant
        Query idQuery = new Query(where("_id").is(remediatedId))
        VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

        // Check if there exists a variant in db that has the same id as newID
        if (variantInDB == null) {
            logger.warn("Variant with id {} not found in DB", remediatedId)
            remediateCaseNoIdCollision(originalId, remediatedId, remediatedVariant)
            return
        }

        logger.info("Found existing variant in DB with id: {} {}", remediatedId, variantInDB)
        // variant with new db present, needs to check for merging
        Set<VariantSourceEntity> remediatedVariantFileSet = remediatedVariant.getVariantSources() != null ?
                remediatedVariant.getVariantSources() : new HashSet<VariantSourceEntity>()
        Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                variantInDB.getVariantSources() : new HashSet<VariantSourceEntity>()
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
            remediateCaseMergeAllSidFidAreDifferent(originalId, variantInDB, remediatedId, remediatedVariant)
            return
        }

        // check if there is any pair of sid and fid from common pair, for which there are more than one entry in files
        // collection
        Map<Tuple2, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
        Set<Tuple2> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        if (sidFidPairsWithGTOneEntry.isEmpty()) {
            logger.info("All common sid fid entries has only one file entry")
            Set<Tuple2> sidFidPairNotInDB = new HashSet<>(remediatedSidFidPairSet)
            sidFidPairNotInDB.removeAll(commonSidFidPairs)
            remediateCaseMergeAllCommonSidFidHasOneFile(originalId, variantInDB, sidFidPairNotInDB, remediatedId,
                    remediatedVariant)
            return
        }

        logger.info("can't merge as sid fid common pair has more than 1 entry in file")
        remediateCaseCantMerge(workingDir, sidFidPairsWithGTOneEntry, dbName, remediatedVariant)
    }

    void remediateCaseNoIdCollision(String originalId, String newId, VariantDocument remediatedVariant) {
        // insert updated variant and delete the existing one
        logger.info("case no id collision - insert new variant: {}", newId)
        mongoTemplate.save(remediatedVariant, VARIANTS_COLLECTION)
        logger.info("case no id collision - delete original variant: {}", originalId)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalId)), VARIANTS_COLLECTION)

        remediateAnnotations(originalId, newId)
    }

    void remediateCaseMergeAllSidFidAreDifferent(String originalId, VariantDocument variantInDB, String newId,
                                                 VariantDocument remediatedVariant) {
        logger.info("case merge all sid fid are different - processing variant: {}", newId)

        // Add all files within remediatedVariant
        Set<VariantSourceEntryMongo> remediatedFiles = remediatedVariant.getVariantSources()

        // Recompute stats
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(remediatedVariant.getReference(),
                remediatedVariant.getAlternate(), variantInDB.getVariantSources(), remediatedFiles,
                sidFidNumberOfSamplesMap)

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
        logger.info("case merge all sid fid are different - delete original variant: {}", originalId)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalId)), VARIANTS_COLLECTION)

        remediateAnnotations(originalId, newId)
    }

    void remediateCaseCantMerge(String workingDir, Set<Tuple2> sidFidPairsWithGtOneEntry, String dbName,
                                VariantDocument remediatedVariant) {
        logger.info("case can't merge variant - processing variant: {}", remediatedVariant.getId())

        String nmcDirPath = Paths.get(workingDir, "non_merged_candidates").toString()
        File nmcDir = new File(nmcDirPath)
        if (!nmcDir.exists()) {
            nmcDir.mkdirs()
        }
        String nmcFilePath = Paths.get(nmcDirPath, dbName + ".txt").toString()
        try (BufferedWriter nmcFile = new BufferedWriter(new FileWriter(nmcFilePath, true))) {
            for (Tuple2<String, String> p : sidFidPairsWithGtOneEntry) {
                nmcFile.write(p.v1 + "," + p.v2 + "," + remediatedVariant.getId() + "\n")
            }
        } catch (IOException e) {
            logger.error("error writing case variant can't be merged in the file:  {}", remediatedVariant.getId())
        }
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(String originalId, VariantDocument variantInDB,
                                                     Set<Tuple2> sidFidPairsNotInDB, String newId,
                                                     VariantDocument remediatedVariant) {
        logger.info("case merge all common sid fid has one file - processing variant: {}", newId)

        // Add only files that are not already in variantInDB
        Set<VariantSourceEntryMongo> remediatedFiles = remediatedVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairsNotInDB.contains(new Tuple2(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())

        // Recompute stats
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", remediatedFiles
                        .stream().map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

        logger.info("case merge all common sid fid has one file - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all common sid fid has one file - delete original variant: {}", originalId)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalId)), VARIANTS_COLLECTION)

        remediateAnnotations(originalId, newId)
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

    private void writeFailureToResolveMafAllele(String sid, String fid, String originalId) {
        String umaDirPath = Paths.get(workingDir, "unresolved_maf_allele").toString()
        File umaDir = new File(umaDirPath)
        if (!umaDir.exists()) {
            umaDir.mkdirs()
        }
        String umaFilePath = Paths.get(umaDirPath, dbName + ".txt").toString()
        try (BufferedWriter umaFile = new BufferedWriter(new FileWriter(umaFilePath, true))) {
            umaFile.write(sid + "," + fid + "," + originalId + "\n")
        } catch (IOException e) {
            logger.error("error writing case unresolved MAF Allele in the file:  {}", originalId)
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

    boolean isCandidateForNormalisation(VariantDocument variant) {
        return (variant.getVariantType() != Variant.VariantType.SNV &&
                (variant.getReference().size() > 1 || variant.getAlternate().size() > 1))
    }

    static Tuple2 getFastaAndReportPaths(String fastaDir, String dbName) {
        // Get path to FASTA file and assembly report based on dbName
        def (eva, taxonomyCode, assemblyCode) = dbName.split("_")
        String scientificName = null
        String assemblyAccession = null

        JsonSlurper jsonParser = new JsonSlurper()
        def results = jsonParser.parse(new URL("https://www.ebi.ac.uk/eva/webservices/rest/v1/meta/species/list"))["response"]["result"][0]
        for (Map<String, String> result: results) {
            if (result["assemblyCode"] == assemblyCode && result["taxonomyCode"] == taxonomyCode) {
                scientificName = result["taxonomyScientificName"].toLowerCase().replace(" ", "_")
                assemblyAccession = result["assemblyAccession"]
                break
            }
        }
        if (scientificName == null || assemblyAccession == null) {
            throw new RuntimeException("Could not determine scientific name and assembly accession for db " + dbName)
        }

        // See here: https://github.com/EBIvariation/eva-common-pyutils/blob/master/ebi_eva_common_pyutils/reference/assembly.py#L61
        return new Tuple2(Paths.get(fastaDir, scientificName, assemblyAccession, assemblyAccession + ".fa"),
                Paths.get(fastaDir, scientificName, assemblyAccession, assemblyAccession + "_assembly_report.txt"))
    }

}
