import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import pandas as pd


def read_ndjson(filepath: str) -> List[Dict]:
    """Read NDJSON file (one JSON object per line)."""
    resources = []
    with open(filepath, 'r') as f:
        for line in f:
            if line.strip():
                resources.append(json.loads(line))
    return resources


def extract_observation_fields(obs: Dict) -> Dict:
    """Extract key fields from a FHIR Observation resource."""
    try:
        patient_ref = obs.get('subject', {}).get('reference', '')
        patient_id = patient_ref.split('/')[-1] if '/' in patient_ref else None
        
        codings = obs.get('code', {}).get('coding', [])
        loinc_code = next(
            (c['code'] for c in codings if c.get('system') == 'http://loinc.org'),
            None
        )
        
        value_qty = obs.get('valueQuantity', {})
        value = value_qty.get('value')
        unit = value_qty.get('unit')
        
        obs_datetime = obs.get('effectiveDateTime')
        
        return {
            'patient_id': patient_id,
            'loinc_code': loinc_code,
            'value': value,
            'unit': unit,
            'effectiveDateTime': obs_datetime
        }
    except Exception as e:
        return None


def map_to_omop_measurement(obs_fields: Dict, loinc_to_concept: Dict) -> Dict:
    """Map extracted FHIR fields to OMOP measurement table format."""
    if not obs_fields or not obs_fields['patient_id'] or not obs_fields['loinc_code'] or obs_fields['value'] is None:
        return None
    
    try:
        concept_id = loinc_to_concept.get(obs_fields['loinc_code'], 0)
        dt_str = obs_fields['effectiveDateTime']
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00')) if dt_str else None
        
        return {
            'person_id': int(obs_fields['patient_id'].split('-')[-1]),
            'measurement_concept_id': concept_id,
            'measurement_date': dt.date() if dt else None,
            'measurement_datetime': dt,
            'value_as_number': float(obs_fields['value']),
            'unit_source_value': obs_fields['unit'],
            'measurement_source_value': obs_fields['loinc_code'],
        }
    except Exception as e:
        return None


def load_fhir_observations(fhir_file: str, loinc_to_concept: Dict) -> pd.DataFrame:
    """Load FHIR Observation NDJSON file and convert to OMOP measurement."""
    measurements = []
    
    print(f"Reading {fhir_file}...")
    resources = read_ndjson(fhir_file)
    
    for resource in resources:
        fields = extract_observation_fields(resource)
        if fields:
            omop_row = map_to_omop_measurement(fields, loinc_to_concept)
            if omop_row:
                measurements.append(omop_row)
    
    df = pd.DataFrame(measurements)
    print(f"Total observations converted: {len(df)}")
    return df
