package eva3623

import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.query.Update
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity
import uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment
import uk.ac.ebi.eva.groovy.commons.RetryableBatchingCursor

import static org.springframework.data.mongodb.core.query.Criteria.where
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.getDbsnpSvoeClass
import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.getSvoeClass
import static org.springframework.data.mongodb.core.query.Query.query

class CorrectSSOperationFromEVA3399 {
    static def logger = LoggerFactory.getLogger(CorrectSSOperationFromEVA3399.class)

    EVADatabaseEnvironment prodEnv

    CorrectSSOperationFromEVA3399() {}

    CorrectSSOperationFromEVA3399(EVADatabaseEnvironment prodEnv) {
        this.prodEnv = prodEnv
    }

    def correctSSOperations = {
        def numRecordsProcessed = 0
        def numRecordsUpdated = 0
        [svoeClass, dbsnpSvoeClass].each { collectionClass ->
            RetryableBatchingCursor cursor = new RetryableBatchingCursor<>(
                    where("_id").regex('^EVA3399_'),
                    prodEnv.mongoTemplate, collectionClass)
            def bulkOps = prodEnv.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionClass.class)

            cursor.each { List<SubmittedVariantOperationEntity> svoes ->
                numRecordsProcessed += svoes.size()
                // Only keep the one that have not been modified already
                def svoesToUpdate = svoes.findAll { !it.reason.startsWith("Original ") }
                svoesToUpdate.each {
                    String originalClusteredVariant = it.getInactiveObjects().get(0).getClusteredVariantAccession()
                    String newReason = "Original rs" + originalClusteredVariant + " associated with SS was merged into new rs."
                    bulkOps.updateOne(query(where("_id").is(it.getId())), new Update().set('reason', newReason))
                }

            }
            def bulkResult = bulkOps.execute()
            if (bulkResult.modifiedCountAvailable && bulkResult.modifiedCount > 0) {
                numRecordsUpdated += bulkResult.modifiedCount
                logger.info("Updated ${bulkResult.modifiedCount} SubmittedVariantOperationEntity, Total Processed ${numRecordsProcessed}, Updated ${numRecordsUpdated}")
            }
        }
    }
}
