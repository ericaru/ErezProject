-- Purpose: Check that a patient ID exists

SELECT 1
FROM Patients
WHERE PatientId = ?
LIMIT 1;
