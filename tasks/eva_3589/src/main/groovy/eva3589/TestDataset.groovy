package eva3589

import org.bson.Document
import org.slf4j.LoggerFactory

import uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment

import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Query.query
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.*

class TestDataset {
    static def logger = LoggerFactory.getLogger(TestDataset.class)

    String assembly
    EVADatabaseEnvironment prodEnv
    EVADatabaseEnvironment devEnv

    static final String backupSuffix = "_backup"

    TestDataset() {}

    TestDataset(String assembly, EVADatabaseEnvironment devEnv) {
        this.assembly = assembly
        this.prodEnv = null
        this.devEnv = devEnv
    }

    TestDataset(String assembly, EVADatabaseEnvironment prodEnv, EVADatabaseEnvironment devEnv) {
        this.assembly = assembly
        this.prodEnv = prodEnv
        this.devEnv = devEnv
    }

    def copyDataToDev(List<Long> rsids) {
        // Copy the following data to from prod to dev:
        // 1. all CVEs with the specified RS IDs
        // 2. all SVEs assigned to the specified RS IDs
        // 3. all CVOEs involving those CVEs found in (1)
        // 4. all SVOEs involving those SVEs found in (2)
        def cvesToCopy = prodEnv.mongoTemplate.find(query(where("asm").is(assembly).and("accession").in(rsids)), cveClass)
        logger.info("Copying ${cvesToCopy.size()} CVEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(cvesToCopy, cveClass)

        def dbsnpCvesToCopy = prodEnv.mongoTemplate.find(query(where("asm").is(assembly).and("accession").in(rsids)), dbsnpCveClass)
        logger.info("Copying ${dbsnpCvesToCopy.size()} dbSNP CVEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(dbsnpCvesToCopy, dbsnpCveClass)

        def svesToCopy = prodEnv.mongoTemplate.find(query(where("seq").is(assembly).and("rs").in(rsids)), sveClass)
        logger.info("Copying ${svesToCopy.size()} SVEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(svesToCopy, sveClass)

        def dbsnpSvesToCopy = prodEnv.mongoTemplate.find(query(where("seq").is(assembly).and("rs").in(rsids)), dbsnpSveClass)
        logger.info("Copying ${dbsnpSvesToCopy.size()} dbSNP SVEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(dbsnpSvesToCopy, dbsnpSveClass)

        def cvoesToCopy = prodEnv.mongoTemplate.find(query(where("inactiveObjects.asm").is(assembly)
                .orOperator(where("accession").in(rsids), where("inactiveObjects.accession").in(rsids))), cvoeClass)
        logger.info("Copying ${cvoesToCopy.size()} CVOEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(cvoesToCopy, cvoeClass)

        def dbsnpCvoesToCopy = prodEnv.mongoTemplate.find(query(where("inactiveObjects.asm").is(assembly)
                .orOperator(where("accession").in(rsids), where("inactiveObjects.accession").in(rsids))), dbsnpCvoeClass)
        logger.info("Copying ${dbsnpCvoesToCopy.size()} dbSNP CVOEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(dbsnpCvoesToCopy, dbsnpCvoeClass)

        def ssids = [svesToCopy, dbsnpSvesToCopy].collectMany{ it.accession }

        def svoesToCopy = prodEnv.mongoTemplate.find(query(where("inactiveObjects.seq").is(assembly)
                .orOperator(where("accession").in(ssids), where("inactiveObjects.accession").in(ssids))), svoeClass)
        logger.info("Copying ${svoesToCopy.size()} SVOEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(svoesToCopy, svoeClass)

        def dbsnpSvoesToCopy = prodEnv.mongoTemplate.find(query(where("inactiveObjects.seq").is(assembly)
                .orOperator(where("accession").in(ssids), where("inactiveObjects.accession").in(ssids))), dbsnpSvoeClass)
        logger.info("Copying ${dbsnpSvoesToCopy.size()} dbSNP SVOEs to dev...")
        devEnv.bulkInsertIgnoreDuplicates(dbsnpSvoesToCopy, dbsnpSvoeClass)
    }

    def backupDevCollections() {
        // Backup all collections to <collection name>_backup
        devEnv.mongoTemplate.getCollectionNames().each {collectionName ->
            if (!collectionName.endsWith(backupSuffix)) {
                logger.info("Backing up ${collectionName}")
                devEnv.mongoTemplate.getCollection(collectionName).aggregate(
                        Collections.singletonList(new Document("\$out",
                                "${collectionName}_${backupSuffix}".toString()))).allowDiskUse(true).size()
            }
        }
    }

    def restoreDevCollectionsFromBackup() {
        // Restore collections to their original (backed-up) state
        devEnv.mongoTemplate.getCollectionNames().each { collectionName ->
            if (collectionName.endsWith(backupSuffix)) {
                def targetCollectionName = collectionName.replace(backupSuffix, "")
                logger.info("Restoring ${targetCollectionName} from backup")
                devEnv.mongoTemplate.getCollection(targetCollectionName).drop()
                devEnv.mongoTemplate.getCollection(collectionName).aggregate(
                        Collections.singletonList(new Document("\$out",
                                targetCollectionName))).allowDiskUse(true).size()
            }
        }
    }

}
