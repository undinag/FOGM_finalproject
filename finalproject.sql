SET sql_mode = '';

-- ----------------------------------------------------------------------------
-- Get patient data for MH patients
-- -- Major Depressive Disorder (4152280) (not included for now)
-- -- Anxiety Disorder (442077)
-- -- Depressive Disorder (440383)?
-- -- Bipolar Disorder (436665)
-- -- PTSD (436676)
-- -- Criteria
    -- Observed between 2008 and 2019
    -- At least 180 days of data
    -- 18 + years old (worse agreement in children) (at time of observation start)
-- ----------------------------------------------------------------------------

##  Get person_ids and ancestor_concept_id for
DROP TABLE IF EXISTS user_uog2000.mh_desc;
CREATE TABLE user_uog2000.mh_desc AS
SELECT DISTINCT concept_name, descendant_concept_id
FROM concept_ancestor
         INNER JOIN concept on concept_id=ancestor_concept_id
WHERE ancestor_concept_id in (442077,440383,436665,436676);

DROP TABLE IF EXISTS user_uog2000.mh_people;
CREATE TEMPORARY TABLE user_uog2000.mh_people AS
SELECT DISTINCT person_id, concept_name
FROM condition_occurrence
INNER JOIN user_uog2000.mh_desc on descendant_concept_id=condition_concept_id
INNER JOIN person using (person_id)
WHERE YEAR(condition_start_date) >= 2008
    AND YEAR(condition_start_date) <= 2019
    AND (2008 - year_of_birth) >= 18
    AND YEAR(condition_start_date) - year_of_birth >= 18
    AND gender_concept_id in (8532, 8507);


DROP TABLE IF EXISTS user_uog2000.mh_patients;
CREATE TABLE user_uog2000.mh_patients AS
SELECT person_id, concept_name
FROM (SELECT person_id,
             concept_name,
             min(observation_date) as min_d,
             max(observation_date) as max_d
FROM observation
INNER JOIN user_uog2000.mh_people using (person_id)
GROUP BY person_id, concept_name) as t
WHERE DATEDIFF(max_d, min_d) >= 180;

CREATE INDEX person_id ON user_uog2000.mh_patients (person_id);
SELECT count(person_id) FROM user_uog2000.mh_patients;

-- ----------------------------------------------------------------------------
-- Get TOP 200 disease occurrences for mh_patients
-- ----------------------------------------------------------------------------

# Get the top 200 diseases for MH patients
DROP TABLE IF EXISTS user_uog2000.mh_top_cond;
CREATE TABLE user_uog2000.mh_top_cond AS
SELECT condition_concept_id,
        count(DISTINCT person_id) as num_people
FROM condition_occurrence
INNER JOIN user_uog2000.mh_patients using (person_id)
WHERE condition_concept_id not in (SELECT descendant_concept_id FROM user_uog2000.mh_desc)
AND condition_concept_id != 0
GROUP BY condition_concept_id
ORDER BY num_people DESC
LIMIT 200;

SET @mh_num=121379;

-- Save counts to CSV file --
# SELECT condition_concept_id, concept_name, num_people, (num_people/@mh_num * 100) as perc
# FROM user_uog2000.mh_top_cond
# INNER JOIN concept on condition_concept_id=concept_id;

# Get the number of condition occurrences per patient
DROP TABLE IF EXISTS user_uog2000.mh_top_cond_occur;
CREATE TABLE user_uog2000.mh_top_cond_occur AS
SELECT person_id, condition_concept_id, condition_start_date
FROM condition_occurrence
INNER JOIN user_uog2000.mh_patients using (person_id)
INNER JOIN user_uog2000.mh_top_cond using (condition_concept_id)
WHERE YEAR(condition_start_date) >= 2008
    AND YEAR(condition_start_date) <= 2019;

CREATE INDEX person_id ON user_uog2000.mh_top_cond_occur (person_id);

# SELECT count(*) FROM user_uog2000.mh_top_cond_occur;
# SELECT person_id, count(*)
# FROM user_uog2000.mh_top_cond_occur
# GROUP BY person_id;

-- ----------------------------------------------------------------------------
-- Get TOP 100 procedure occurrences for mh_patients
-- ----------------------------------------------------------------------------

# Get the top 100 procedures for MH patients
DROP TABLE IF EXISTS user_uog2000.mh_top_prod;
CREATE TABLE user_uog2000.mh_top_prod AS
SELECT procedure_concept_id,
        count(DISTINCT person_id) as num_people
FROM procedure_occurrence
INNER JOIN user_uog2000.mh_patients using (person_id)
WHERE procedure_concept_id != 0
GROUP BY procedure_concept_id
ORDER BY num_people DESC
LIMIT 100;

# Get procedure occurrence
DROP TABLE IF EXISTS user_uog2000.mh_top_prod_occur;
CREATE TABLE user_uog2000.mh_top_prod_occur AS
SELECT person_id, procedure_concept_id, procedure_date
FROM procedure_occurrence
INNER JOIN user_uog2000.mh_patients using (person_id)
INNER JOIN user_uog2000.mh_top_prod using (procedure_concept_id)
WHERE YEAR(procedure_date) >= 2008
AND YEAR(procedure_date) <= 2019;

CREATE INDEX person_id ON user_uog2000.mh_top_prod_occur (person_id);

SELECT count(*) from user_uog2000.mh_top_prod_occur;
SELECT count(DISTINCT person_id) FROM user_uog2000.mh_top_prod_occur;

-- Save counts in CSV --
SELECT procedure_concept_id, concept_name, num_people, ((num_people/@mh_num) * 100) as perc
FROM user_uog2000.mh_top_prod
INNER JOIN concept on concept_id=procedure_concept_id;

-- ----------------------------------------------------------------------------
-- Get list of MH related drugs
-- By ancestors:
    -- Antidepressants 21604686 (include seratonin-norepi, SSRIs, tricyclic,
    -- Antipsychotics 21604490 (includes lithium/mood stabilizers)
    -- Psycholeptic 21604489 (includes antipsychotic and anxiolytics, hypnotics and sedatives)
    -- Anxiolytics 21604564
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS user_uog2000.mh_drugs;
CREATE TEMPORARY TABLE user_uog2000.mh_drugs AS
SELECT descendant_concept_id #drug_concept_id, count(DISTINCT person_id) as num_people
FROM concept_ancestor
INNER JOIN concept on concept_id=descendant_concept_id
WHERE ancestor_concept_id in (21604686, 21604489)
AND vocabulary_id = 'RxNorm'
AND standard_concept = 'S';

DROP TABLE IF EXISTS user_uog2000.mh_mh_drug;
CREATE TABLE user_uog2000.mh_mh_drug AS
SELECT drug_concept_id, count(DISTINCT person_id) as num_people
FROM  drug_era
INNER JOIN user_uog2000.mh_patients using (person_id)
INNER JOIN user_uog2000.mh_drugs on descendant_concept_id=drug_concept_id
WHERE (YEAR(drug_era_start_datetime) >= 2008 OR YEAR(drug_era_end_datetime) >= 2008)
GROUP BY drug_concept_id
ORDER BY num_people DESC
LIMIT 100;

SELECT drug_concept_id, concept_name, num_people, ((num_people/@mh_num) * 100) as perc
FROM user_uog2000.mh_mh_drug
INNER JOIN concept on concept_id=drug_concept_id;

-- ----------------------------------------------------------------------------
-- Get TOP 100 drug exposures for mh_patients
-- ----------------------------------------------------------------------------

# Get the top 200 drug exposures for MH patients
DROP TABLE IF EXISTS user_uog2000.mh_top_drug;
CREATE TABLE user_uog2000.mh_top_drug AS
SELECT drug_concept_id,
        count(DISTINCT person_id) as num_people
FROM drug_era
INNER JOIN user_uog2000.mh_patients using (person_id)
WHERE drug_concept_id != 0
GROUP BY drug_concept_id
ORDER BY num_people DESC
LIMIT 100;

DROP TABLE IF EXISTS user_uog2000.all_drug;
CREATE TEMPORARY TABLE user_uog2000.all_drug AS
SELECT drug_concept_id
FROM user_uog2000.mh_mh_drug
UNION
SELECT drug_concept_id
FROM user_uog2000.mh_top_drug;

# Get drug exposures
DROP TABLE IF EXISTS user_uog2000.mh_top_drug_occur;
CREATE TABLE user_uog2000.mh_top_drug_occur AS
SELECT person_id, drug_concept_id, drug_era_start_datetime
FROM drug_era
INNER JOIN user_uog2000.mh_patients using (person_id)
INNER JOIN user_uog2000.all_drug using (drug_concept_id)
WHERE (YEAR(drug_era_start_datetime) >= 2008 OR YEAR(drug_era_end_datetime) >= 2008);

SELECT count(*) from user_uog2000.mh_top_drug_occur;
SELECT count(DISTINCT person_id) FROM user_uog2000.mh_top_drug_occur;

CREATE INDEX person_id ON user_uog2000.mh_top_drug_occur (person_id);

-- Save counts in CSV --
SELECT drug_concept_id, concept_name, num_people, ((num_people/@mh_num) * 100) as perc
FROM user_uog2000.mh_top_drug
INNER JOIN concept on concept_id=drug_concept_id;

-- count how many patients have at least one condition/procedure/exposure
DROP TABLE IF EXISTS user_uog2000.mh_final_patients;
CREATE TABLE user_uog2000.mh_final_patients AS
SELECT person_id, sum(count) as total
FROM
((SELECT person_id, count(*) as count
    FROM user_uog2000.mh_top_cond_occur
    GROUP BY person_id)
UNION
    (SELECT person_id, count(*) as count
    FROM user_uog2000.mh_top_prod_occur
        GROUP BY person_id)
UNION
    (SELECT person_id, count(*) as count
    FROM user_uog2000.mh_top_drug_occur
    GROUP BY person_id)) as t
GROUP BY person_id
ORDER BY total DESC;

SELECT count(*) FROM user_uog2000.mh_final_patients;

-- ----------------------------------------------------------------------------
-- Combine into R stan friendly format
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS user_uog2000.mh_all_dat;
CREATE TABLE user_uog2000.mh_all_dat AS
SELECT person_id, concept_id, date
FROM ((SELECT person_id, condition_concept_id as concept_id, condition_start_date as date
    FROM user_uog2000.mh_top_cond_occur)
UNION ALL
    (SELECT person_id, procedure_concept_id as concept_id, procedure_date as date
    FROM user_uog2000.mh_top_prod_occur)
UNION ALL
    (SELECT person_id, drug_concept_id as concept_id, drug_era_start_datetime as date
    FROM user_uog2000.mh_top_drug_occur)) as t
ORDER BY person_id, date;

DROP TABLE IF EXISTS user_uog2000.concept_maps;
CREATE TABLE user_uog2000.concept_maps AS
SELECT DISTINCT concept_id, concept_name, vocabulary_id
FROM user_uog2000.mh_all_dat
INNER JOIN concept using (concept_id);

SELECT count(DISTINCT person_id) FROM user_uog2000.mh_all_dat