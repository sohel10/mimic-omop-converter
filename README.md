# MIMIC-IV → OMOP Common Data Model Converter

A PySpark-based ETL pipeline that converts MIMIC-IV clinical data (364K patients, 158M lab events) into the OMOP Common Data Model v5.4, outputting a validated SQLite database.

## Why This Exists

OMOP CDM is the standard data format used by OHDSI's network of 800+ healthcare institutions. Most EHR data—including MIMIC-IV—ships in proprietary schemas. This converter bridges that gap, enabling MIMIC-IV data to work with any OMOP-compatible analytics tool (ATLAS, ACHILLES, PatientLevelPrediction).

## Results

| OMOP Table | Rows | Source |
|---|---|---|
| person | 364,627 | patients.csv |
| visit_occurrence | 546,028 | admissions.csv |
| measurement | 16,588,459 | labevents.csv (aggregated from 158M raw rows) |
| drug_exposure | 20,292,611 | prescriptions.csv |

**Output:** 4.7 GB SQLite database, zero orphan foreign keys, zero null primary keys.

## Architecture

### Pipeline Steps

1. **Load** - Spark reads compressed CSVs with schema inference
2. **Filter** - Broadcast join against patient ID list (replaces .isin() which caused JVM code-gen OOM with 364K IDs)
3. **Aggregate** - Lab events grouped by (patient_id, itemid), computing mean/max/min per patient per lab test
4. **Map** - MIMIC columns renamed and cast to OMOP CDM v5.4 schema with vocabulary concept IDs
5. **Write** - Streamed to SQLite in 500K-row batches via toLocalIterator() (avoids collecting 37M rows into pandas)

### Engineering Decisions

**Broadcast join over .isin():** Passing 364,627 patient IDs to Spark's .isin() generates a single IN() predicate that overflows the JVM code generator. Broadcasting the patient ID DataFrame as a small lookup table eliminates this.

**Lab aggregation:** 158M individual lab readings aggregated to 16.5M summary rows (mean, max, min per patient per lab test), preserving clinical signal while reducing data volume 10x.

**Chunked SQLite writes:** toPandas() on 20M+ rows exceeds driver memory. toLocalIterator() streams rows one partition at a time, writing 500K-row batches. DataFrames repartitioned to 20 partitions to stay under maxResultSize (4GB).

**Resume logic:** Converter checks each table's row count before writing, allowing safe re-runs after interruptions.

## Spark Configuration

| Setting | Value | Reason |
|---|---|---|
| spark.driver.memory | 16g | Handles broadcast of 364K patient IDs |
| spark.executor.memory | 8g | Per-executor allocation |
| spark.sql.shuffle.partitions | 200 | Balances parallelism on single machine |
| spark.sql.autoBroadcastJoinThreshold | 50m | Ensures patient ID table is broadcast |
| spark.driver.maxResultSize | 4g | Accommodates large partition results |

## OMOP Mappings

| MIMIC Field | OMOP Field | Mapping |
|---|---|---|
| gender (M/F) | gender_concept_id | M=8507, F=8532 |
| subject_id | person_id | Direct |
| hadm_id | visit_occurrence_id | Direct |
| itemid | measurement_concept_id | LOINC mapping (12 common labs) |
| drug | drug_source_value | Preserved as source text |

### LOINC Vocabulary (Partial)

| MIMIC itemid | Lab Test | OMOP Concept ID |
|---|---|---|
| 50862 | Sodium | 3024561 |
| 50861 | Potassium | 3024560 |
| 50912 | Creatinine | 3018677 |
| 50893 | Calcium | 3008480 |
| 50889 | Troponin T | 3022192 |
| 50809 | Glucose | 3007637 |

### Not Yet Mapped

- **RxNorm:** drug_concept_id set to 0 (requires NLM RxNorm API)
- **SNOMED:** No ICD-10 condition mapping yet
- **Race/ethnicity:** Set to 0 (not in MIMIC-IV patients.csv)

## Quick Start
conda create -n mimic-spark python=3.10
conda activate mimic-spark
pip install pyspark pandas
python convert.py /path/to/mimic-iv/data 364627 data/output

## Project Structure
mimic-omop-converter/
convert.py # CLI entry point
src/
mimic_reader.py # Spark CSV loader with broadcast join
omop_mapper.py # MIMIC to OMOP transformations
vocabulary.py # LOINC/RxNorm/SNOMED concept mappings
loader.py # Chunked SQLite writer
config/
omop_schema.sql # OMOP CDM v5.4 DDL

## References

- [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/)
- [MIMIC-IV Documentation](https://mimic.mit.edu/)
- [The Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/)

## Author

**Sohel Ahmed, Ph.D.** - sohelcu06@gmail.com
