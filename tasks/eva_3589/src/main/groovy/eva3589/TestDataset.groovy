package eva3589

import org.bson.Document
import org.slf4j.LoggerFactory

import uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment

import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Query.query
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.getDbsnpSveClass
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.getSveClass

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
        //TODO
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
