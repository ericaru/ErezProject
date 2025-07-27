-- check_loinc.sql
-- Purpose: Check if the given LOINC code exists in the Loinc table

SELECT 1
FROM Loinc
WHERE LoincNum = ?
LIMIT 1;
