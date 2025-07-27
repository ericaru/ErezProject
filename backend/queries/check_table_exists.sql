-- Purpose: Check if 'Patients' table exists
-- Output: Returns row if exists

SELECT name FROM sqlite_master
WHERE type='table' AND name='Patients';