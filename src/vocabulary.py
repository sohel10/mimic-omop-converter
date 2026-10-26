from typing import Optional

class VocabularyMapper:
    GENDER_CONCEPTS = {'M': 8507, 'F': 8532}
    LOINC_MAPPINGS = {50862: 3024561, 50861: 3024560, 50893: 3008480, 50883: 3023181, 50912: 3018677, 50868: 3027314, 50889: 3022192, 50891: 3002963, 50809: 3007637, 50885: 3011407, 50896: 3000709, 50898: 3031876}
    MEASUREMENT_TYPE_CONCEPT = 5001471
    DRUG_TYPE_CONCEPT = 38000175
    
    def get_gender_concept(self, gender_char: str) -> int:
        return self.GENDER_CONCEPTS.get(gender_char, 0)
    
    def get_loinc_concept(self, itemid: int) -> Optional[int]:
        return self.LOINC_MAPPINGS.get(itemid)
