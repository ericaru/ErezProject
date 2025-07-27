-- Purpose: Inserts a new abstracted measurement
INSERT INTO AbstractedMeasurements (PatientId, LoincNum, ConceptName, AbstractedValue, StartDateTime, EndDateTime)
VALUES (?, ?, ?, ?, ?, ?);
