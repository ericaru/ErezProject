-- Purpose: Get the unit from the most recent version of this record using *TransactionInsertionTime*
-- Designated to prevent user from mismatching the units when inputting a row
-- This query will return said unit as TEXT

SELECT Unit
FROM Measurements
WHERE PatientId = ? AND LoincNum = ? AND ValidStartTime = ?
ORDER BY TransactionInsertionTime DESC
LIMIT 1;