from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
from typing import Optional

class MIMICReader:
    def __init__(self, data_root: str, spark: SparkSession):
        self.data_root = Path(data_root)
        self.spark = spark
        if not self.data_root.exists():
            raise FileNotFoundError(f"Data root not found: {self.data_root}")
    
    def _find_file(self, filename: str) -> str:
        hosp_path = self.data_root / "hosp" / filename
        if hosp_path.with_suffix('.csv.gz').exists():
            return str(hosp_path.with_suffix('.csv.gz'))
        elif hosp_path.exists():
            return str(hosp_path)
        root_path = self.data_root / filename
        if root_path.with_suffix('.csv.gz').exists():
            return str(root_path.with_suffix('.csv.gz'))
        return str(root_path)
    
    def load_patients(self, nrows: Optional[int] = None):
        path = self._find_file("patients.csv")
        df = self.spark.read.option("inferSchema", "true").csv(path, header=True)
        if nrows:
            df = df.limit(nrows)
        return df
    
    def load_admissions(self):
        path = self._find_file("admissions.csv")
        return self.spark.read.option("inferSchema", "true").csv(path, header=True)
    
    def load_labevents(self):
        path = self._find_file("labevents.csv")
        return self.spark.read.option("inferSchema", "true").csv(path, header=True)
    
    def load_prescriptions(self):
        path = self._find_file("prescriptions.csv")
        return self.spark.read.option("inferSchema", "true").csv(path, header=True)

def load_subset(data_root: str, n_patients: int = 1000, spark: SparkSession = None):
    if spark is None:
        spark = SparkSession.builder.appName("MIMIC-OMOP").getOrCreate()
    
    reader = MIMICReader(data_root, spark)
    patients = reader.load_patients(nrows=n_patients)
    patients.cache()
    patient_count = patients.count()
    print(f"Loaded {patient_count} unique patients")
    
    # Broadcast join instead of .isin() — avoids 364K-element IN() predicate
    patient_ids = F.broadcast(patients.select('subject_id').distinct())
    
    admissions = reader.load_admissions().join(patient_ids, 'subject_id', 'inner')
    labevents = reader.load_labevents().join(patient_ids, 'subject_id', 'inner')
    prescriptions = reader.load_prescriptions().join(patient_ids, 'subject_id', 'inner')
    
    # Lazy — no .count() here, let downstream steps trigger execution
    print("Joined admissions, labevents, prescriptions (lazy)")
    
    return {
        'patients': patients,
        'admissions': admissions,
        'labevents': labevents,
        'prescriptions': prescriptions,
    }
