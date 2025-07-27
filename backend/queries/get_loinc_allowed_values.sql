-- Purpose: Get LOINC allowed values from table based on LOINC-Code. 
-- Used for insersion/ update operations.

SELECT DISTINCT AllowedValues
FROM Loinc
WHERE LoincNum = ?
