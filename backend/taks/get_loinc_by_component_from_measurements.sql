-- Purpose: Get all appropriate LOINC code based on a given name, from the patient's data.
-- Used for update and deletion only 

SELECT DISTINCT m.LoincNum
FROM Measurements m
JOIN Loinc l ON m.LoincNum = l.LoincNum
WHERE LOWER(l.Component) = LOWER(?)
  AND m.PatientId = ?
  AND m.ValidStartTime = ?
  AND (m.TransactionDeletionTime IS NULL OR m.TransactionDeletionTime > ?)
