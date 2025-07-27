-- Purpose: Search patient measurement history with LOINC name.
-- Params: patient_id, [loinc_num], [date_range], [time_range]
-- The query has a place holder to recieve a specific date/time range externally. 
-- In addition, the time clause will also define the snapshot date, which will allow to query the DB based on it's current content, or past content (using ValidEndDate)

SELECT m.LoincNum,
       l.Component AS LOINCConceptName,
       m.Value,
       m.Unit,
       m.ValidStartTime,
       m.TransactionInsertionTime
FROM Measurements m
JOIN Loinc l ON m.LoincNum = l.LoincNum
WHERE {where_clause}
ORDER BY m.ValidStartTime ASC