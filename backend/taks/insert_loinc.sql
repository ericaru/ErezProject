-- Purpose: Inserts 1 record into the LOINC table -> A new code

INSERT OR IGNORE INTO Loinc (LoincNum, Component, Property, TimeAspect, System, ScaleType, MethodType, AllowedValues)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
