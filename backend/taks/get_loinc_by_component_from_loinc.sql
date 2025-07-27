-- Purpose: Get all appropriate LOINC code based on a given name, from LOINC table. 
-- Used for insersion operation.

SELECT DISTINCT LoincNum
FROM Loinc
WHERE LOWER(Component) = LOWER(?)
