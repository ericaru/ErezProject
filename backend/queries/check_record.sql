-- Purpose: Checks if a record exists in the DB
-- Only take undeleted records into consideration (undeleted related to the TransactionInsertionData)

SELECT 1 
FROM Measurements 
WHERE PatientId = ? AND LoincNum = ? AND ValidStartTime = ?
AND (TransactionDeletionTime IS NULL OR TransactionDeletionTime > ?)  -- and ends after insert time (or still open)
LIMIT 1;
