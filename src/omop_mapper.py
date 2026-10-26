from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType
from .vocabulary import VocabularyMapper


class OMOPMapper:
    def __init__(self, vocab_mapper: VocabularyMapper = None):
        self.vocab = vocab_mapper or VocabularyMapper()

    def map_patients_to_person(self, patients_df):
        person = patients_df.select(
            F.col('subject_id').alias('person_id'),
            F.create_map(
                F.lit('M'), F.lit(8507),
                F.lit('F'), F.lit(8532)
            )[F.col('gender')].alias('gender_concept_id'),
            F.col('gender').alias('gender_source_value'),
            F.col('anchor_age').cast(IntegerType()).alias('year_of_birth'),
            F.lit(1).alias('month_of_birth'),
            F.lit(1).alias('day_of_birth'),
            F.lit(0).alias('race_concept_id'),
            F.lit(0).alias('ethnicity_concept_id'),
            F.col('subject_id').cast('string').alias('person_source_value')
        )
        return person

    def map_admissions_to_visit(self, admissions_df):
        visit = admissions_df.select(
            F.col('hadm_id').alias('visit_occurrence_id'),
            F.col('subject_id').alias('person_id'),
            F.lit(9201).alias('visit_concept_id'),
            F.lit(262).alias('visit_type_concept_id'),
            F.to_date(F.col('admittime')).alias('visit_start_date'),
            F.col('admittime').alias('visit_start_datetime'),
            F.to_date(F.col('dischtime')).alias('visit_end_date'),
            F.col('dischtime').alias('visit_end_datetime'),
            F.col('hadm_id').cast('string').alias('visit_source_value')
        )
        return visit

    def map_labevents_to_measurement(self, labevents_df):
        lab_clean = labevents_df.filter(F.col('valuenum').isNotNull())

        measurement = lab_clean.groupBy(
            F.col('subject_id').alias('person_id'),
            F.col('itemid').alias('measurement_concept_id')
        ).agg(
            F.first(F.col('hadm_id')).alias('visit_occurrence_id'),
            F.count('*').alias('measurement_count'),
            F.avg('valuenum').cast(DoubleType()).alias('value_as_number'),
            F.max('valuenum').cast(DoubleType()).alias('value_max'),
            F.min('valuenum').cast(DoubleType()).alias('value_min'),
            F.first(F.col('valueuom')).alias('unit_source_value'),
            F.max(F.col('charttime')).alias('measurement_datetime')
        ).select(
            F.monotonically_increasing_id().alias('measurement_id'),
            F.col('person_id'),
            F.col('visit_occurrence_id'),
            F.col('measurement_concept_id'),
            F.lit(5001471).alias('measurement_type_concept_id'),
            F.to_date(F.col('measurement_datetime')).alias('measurement_date'),
            F.col('measurement_datetime'),
            F.col('value_as_number'),
            F.col('unit_source_value'),
            F.col('measurement_concept_id').cast('string').alias('measurement_source_value'),
            F.col('measurement_concept_id').alias('measurement_source_concept_id'),
            F.col('measurement_count').cast(IntegerType()).alias('num_observations'),
            F.col('value_max').cast(DoubleType()).alias('value_max'),
            F.col('value_min').cast(DoubleType()).alias('value_min')
        )
        return measurement

    def map_prescriptions_to_drug_exposure(self, prescriptions_df):
        drug = prescriptions_df.select(
            F.monotonically_increasing_id().alias('drug_exposure_id'),
            F.col('subject_id').alias('person_id'),
            F.col('hadm_id').alias('visit_occurrence_id'),
            F.lit(0).alias('drug_concept_id'),
            F.lit(38000175).alias('drug_type_concept_id'),
            F.to_date(F.col('starttime')).alias('drug_exposure_start_date'),
            F.col('starttime').alias('drug_exposure_start_datetime'),
            F.coalesce(
                F.to_date(F.col('stoptime')),
                F.to_date(F.col('starttime'))
            ).alias('drug_exposure_end_date'),
            F.coalesce(
                F.col('stoptime'), F.col('starttime')
            ).alias('drug_exposure_end_datetime'),
            F.col('dose_val_rx').cast(DoubleType()).alias('quantity'),
            F.coalesce(F.col('dose_unit_rx'), F.lit('Unknown')).alias('dose_unit_source_value'),
            F.coalesce(F.col('drug'), F.lit('Unknown')).alias('drug_source_value')
        )
        return drug
