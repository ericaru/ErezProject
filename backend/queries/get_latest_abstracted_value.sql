-- Purpose: Get the latest abstracted value for a specific patient and concept from AbstractedMeasurements table
-- Returns only the most recent abstracted value based on StartDateTime
-- Used by SimpleRuleEngine to get current state values for rule application

SELECT AbstractedValue
FROM AbstractedMeasurements
WHERE PatientId = ? AND ConceptName = ?
ORDER BY StartDateTime DESC
LIMIT 1;