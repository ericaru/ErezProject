-- Purpose: Get the parameters from Patients table for a specific patient, 
-- that are needed for the calculation of abstractions and tules

SELECT Sex
FROM Patients
WHERE PatientId = ?