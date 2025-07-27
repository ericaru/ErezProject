-- Purpose: To update a measurement's value based on it's PatientId, LoincNum and ValidStartTime
-- If more than 1 record fits this condition, we'll update the one last inputed into the DB (based on TransactionTime)

UPDATE Measurements
SET Value = ?
WHERE rowid = (
    SELECT rowid FROM Measurements
    WHERE PatientId = ?
      AND LoincNum = ?
      AND ValidStartTime = ?
    ORDER BY TransactionInsertionTime DESC
    LIMIT 1
);