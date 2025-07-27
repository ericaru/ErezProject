-- Purpose: Creates 3 tables to manage the patients in the DB

CREATE TABLE IF NOT EXISTS Patients (
    PatientId TEXT PRIMARY KEY,
    FirstName TEXT,
    LastName TEXT,
    Sex TEXT
);


CREATE TABLE IF NOT EXISTS Measurements (
    MeasurementId INTEGER PRIMARY KEY AUTOINCREMENT,
    PatientId TEXT,
    LoincNum TEXT,
    Value TEXT NOT NULL,
    Unit TEXT,
    ValidStartTime TEXT NOT NULL,
    TransactionInsertionTime TEXT NOT NULL, -- The time a record was inserted to the DB
    TransactionDeletionTime TEXT, -- The time a record was deleted from the DB
    FOREIGN KEY (PatientId) REFERENCES Patients(PatientId),
    FOREIGN KEY (LoincNum) REFERENCES Loinc(LoincNum)
);


CREATE TABLE IF NOT EXISTS AbstractedMeasurements (
    MeasurementId INTEGER PRIMARY KEY AUTOINCREMENT,
    PatientId TEXT NOT NULL,
    LoincNum TEXT NOT NULL,
    ConceptName TEXT NOT NULL,
    AbstractedValue TEXT NOT NULL,
    StartDateTime TEXT NOT NULL,
    EndDateTime TEXT NOT NULL,
    FOREIGN KEY (PatientId) REFERENCES Patients(PatientId),
    FOREIGN KEY (LoincNum) REFERENCES Loinc(LoincNum)
);