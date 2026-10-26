#!/usr/bin/env python3
"""MIMIC-IV to OMOP CDM Converter (Spark-based)."""

import sys
import time
import sqlite3
from pathlib import Path
from pyspark.sql import SparkSession
from src.mimic_reader import load_subset
from src.omop_mapper import OMOPMapper
from src.vocabulary import VocabularyMapper
from src.loader import OMOPLoader


def table_has_data(db_path, table_name):
    if not Path(db_path).exists():
        return False
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python convert.py <data_dir> [n_patients] [output_dir]")
        sys.exit(1)

    data_dir = sys.argv[1]
    n_patients = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data/output"

    print("=" * 70)
    print("MIMIC-IV to OMOP Converter (Spark)")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("MIMIC-OMOP-Converter") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50m") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

    start = time.time()

    db_path = f"{output_dir}/omop.db"

    # Check which tables already have data
    tables_done = {}
    for t in ['person', 'visit_occurrence', 'measurement', 'drug_exposure']:
        tables_done[t] = table_has_data(db_path, t)
        if tables_done[t]:
            print(f"  ✓ {t} already loaded, skipping")

    if all(tables_done.values()):
        print("\nAll tables already loaded. Delete omop.db to re-run.")
        spark.stop()
        return

    # Step 1: Load MIMIC data
    print(f"\n[1] Loading {n_patients} MIMIC patients (Spark)...")
    data = load_subset(data_dir, n_patients, spark)

    # Step 2: Init vocabulary
    print("\n[2] Initializing vocabulary mapper...")
    vocab = VocabularyMapper()
    mapper = OMOPMapper(vocab)

    # Step 3: Map to OMOP
    print("\n[3] Mapping to OMOP tables...")
    person = mapper.map_patients_to_person(data['patients'])
    visit = mapper.map_admissions_to_visit(data['admissions'])
    measurement = mapper.map_labevents_to_measurement(data['labevents'])
    drug_exposure = mapper.map_prescriptions_to_drug_exposure(data['prescriptions'])

    person.cache()
    visit.cache()
    print(f"  - person: {person.count()} rows")
    print(f"  - visit_occurrence: {visit.count()} rows")

    # Step 4: Write to SQLite in chunks
    print(f"\n[4] Loading to SQLite: {db_path}")
    loader = OMOPLoader(db_path)
    loader.init_schema()

    if not tables_done['person']:
        loader.load_table(person, 'person')
    if not tables_done['visit_occurrence']:
        loader.load_table(visit, 'visit_occurrence')
    if not tables_done['measurement']:
        loader.load_table(measurement, 'measurement')
    if not tables_done['drug_exposure']:
        loader.load_table(drug_exposure, 'drug_exposure')

    loader.close()

    elapsed = time.time() - start
    print(f"\n{'=' * 70}")
    print(f"✓ Conversion complete in {elapsed/60:.1f} minutes")
    print(f"  Output: {db_path}")
    print(f"{'=' * 70}")

    spark.stop()


if __name__ == "__main__":
    main()
