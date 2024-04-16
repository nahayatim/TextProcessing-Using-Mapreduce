from mrjob.job import MRJob
from math import sqrt
import json

class ChiSquareReducer(MRJob):
    
    def reducer(self, key, values):
        category, term = key
        observed = sum(count for count, _ in values)
        
        # Calculate total count of all terms in the same category
        total_count = sum(total_count for total_count, _ in values)
        
        # Calculate expected frequency of the term in the category
        expected = total_count * observed
        
        # Calculate chi-square value
        chi_square = (observed - expected)**2 / expected
        
        yield category, (term, chi_square)

if __name__ == '__main__':
    ChiSquareReducer.run()
