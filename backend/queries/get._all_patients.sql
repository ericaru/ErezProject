-- Purpose: Get all patients for batch processing

SELECT PatientId, FirstName, LastName, Sex
FROM Patients
ORDER BY PatientId;