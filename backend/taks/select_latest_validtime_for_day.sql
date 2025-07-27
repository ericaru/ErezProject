-- Purpose: Get the latest date-time of ValidStartTime, for a record at a spacific datetime.
-- Used for deletion where it's required that the user will be able to ignore time and input only date.
-- This query can accept either loinc_num or loinc_code to resolve the desired date

SELECT m.ValidStartTime
FROM Measurements m
JOIN Loinc l ON m.LoincNum = l.LoincNum
WHERE m.PatientId = ?
  AND DATE(m.ValidStartTime) = DATE(?)
  AND (m.TransactionDeletionTime IS NULL OR m.TransactionDeletionTime > ?)
  AND (
      (? IS NOT NULL AND m.LoincNum = ?)
      OR
      (? IS NOT NULL AND LOWER(l.Component) = LOWER(?))
  )
ORDER BY m.ValidStartTime DESC
LIMIT 1;