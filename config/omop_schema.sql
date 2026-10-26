CREATE TABLE IF NOT EXISTS person (
    person_id INTEGER PRIMARY KEY,
    gender_concept_id INTEGER,
    gender_source_value TEXT,
    year_of_birth INTEGER,
    month_of_birth INTEGER,
    day_of_birth INTEGER,
    race_concept_id INTEGER,
    ethnicity_concept_id INTEGER,
    person_source_value TEXT
);

CREATE TABLE IF NOT EXISTS visit_occurrence (
    visit_occurrence_id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    visit_concept_id INTEGER,
    visit_type_concept_id INTEGER,
    visit_start_date TEXT,
    visit_start_datetime TEXT,
    visit_end_date TEXT,
    visit_end_datetime TEXT,
    visit_source_value TEXT,
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

CREATE TABLE IF NOT EXISTS measurement (
    measurement_id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    visit_occurrence_id INTEGER,
    measurement_concept_id INTEGER,
    measurement_type_concept_id INTEGER,
    measurement_date TEXT,
    measurement_datetime TEXT,
    value_as_number REAL,
    unit_source_value TEXT,
    measurement_source_value TEXT,
    measurement_source_concept_id INTEGER,
    num_observations INTEGER,
    value_max REAL,
    value_min REAL,
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

CREATE TABLE IF NOT EXISTS drug_exposure (
    drug_exposure_id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    visit_occurrence_id INTEGER,
    drug_concept_id INTEGER,
    drug_type_concept_id INTEGER,
    drug_exposure_start_date TEXT,
    drug_exposure_start_datetime TEXT,
    drug_exposure_end_date TEXT,
    drug_exposure_end_datetime TEXT,
    quantity REAL,
    dose_unit_source_value TEXT,
    drug_source_value TEXT,
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

CREATE INDEX IF NOT EXISTS idx_visit_person ON visit_occurrence(person_id);
CREATE INDEX IF NOT EXISTS idx_measurement_person ON measurement(person_id);
CREATE INDEX IF NOT EXISTS idx_measurement_concept ON measurement(measurement_concept_id);
CREATE INDEX IF NOT EXISTS idx_drug_person ON drug_exposure(person_id);
