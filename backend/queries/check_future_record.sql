-- Purpose: Checks if there exists an identical record to the one you want to insert with a later *TransactionInsertionTime*
-- Such case enforces us to input the new record with a TransactionDeleteTime, and will be rejected.
-- This query will return said time

SELECT MAX(TransactionInsertionTime)
FROM Measurements
WHERE PatientId = ? AND LoincNum = ? AND ValidStartTime = ?
  AND TransactionInsertionTime > ?;