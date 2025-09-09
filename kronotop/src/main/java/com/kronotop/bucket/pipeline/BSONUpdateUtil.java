package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import org.bson.BsonValue;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class BSONUpdateUtil {

    private BSONUpdateUtil() {
        // Utility class
    }

    /**
     * Updates a BSON document by applying the set operations from UpdateOptions.
     * This method traverses the setOps map and updates or adds the corresponding fields in the document.
     *
     * @param document The original BSON document as ByteBuffer
     * @param setOps   Map of field names to new values
     * @return DocumentUpdateResult containing the updated document and modified fields
     */
    public static DocumentUpdateResult applyUpdateOperations(ByteBuffer document, Map<String, BsonValue> setOps, Set<String> unsetOps) {
        if (setOps.isEmpty() && unsetOps.isEmpty()) {
            return new DocumentUpdateResult(document, Map.of(), Set.of());
        }

        document.rewind();
        Document bsonDoc = BSONUtil.fromBson(document);

        Set<String> droppedSelectors = new HashSet<>();
        for (String selector : unsetOps) {
            // Currently, we only support the root level fields as a selector.
            if (bsonDoc.containsKey(selector)) {
                droppedSelectors.add(selector);
                bsonDoc.remove(selector);
            }
        }

        Map<String, BsonValue> newValues = new HashMap<>();

        // Apply set operations and track the new values
        for (Map.Entry<String, BsonValue> setOp : setOps.entrySet()) {
            String fieldName = setOp.getKey();
            BsonValue bsonValue = setOp.getValue();

            // Track the new value being set (both new and updated fields)
            newValues.put(fieldName, bsonValue);

            bsonDoc.put(fieldName, bsonValue);
        }

        ByteBuffer updatedDocument = ByteBuffer.wrap(BSONUtil.toBytes(bsonDoc));
        return new DocumentUpdateResult(updatedDocument, newValues, droppedSelectors);
    }

    /**
     * Record to hold the result of a BSON document update operation.
     *
     * @param document  The updated BSON document as ByteBuffer
     * @param newValues Map of field names to their new BSON values that were set (both new and updated fields)
     */
    public record DocumentUpdateResult(ByteBuffer document, Map<String, BsonValue> newValues, Set<String> droppedSelectors) {
    }
}