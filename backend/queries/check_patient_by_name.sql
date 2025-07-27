-- Purpose: Fetch All PatientIds by FirstName and LastName
-- Will be used to verify the system ID for a patient if only the name is known.
-- The query is not case sensitive

SELECT PatientId, FirstName, LastName, Sex
FROM Patients
WHERE FirstName = ? COLLATE NOCASE AND LastName = ? COLLATE NOCASE;
