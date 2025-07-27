-- Purpose: Inserts 1 record into the Measurements table -> A new record
INSERT INTO Measurements (PatientId, LoincNum, Value, Unit, ValidStartTime, TransactionInsertionTime)
VALUES (?, ?, ?, ?, ?, ?);
