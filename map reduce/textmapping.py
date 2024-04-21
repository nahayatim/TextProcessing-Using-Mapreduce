from math import pow, log
from mrjob.job import MRJob
from mrjob.step import MRStep

class ChiSquareCalculator(MRJob):
    
    def mapper(self, _, line):
        try:
            category, review_text = line.strip().split(' ', 1)
            if category and review_text:
                terms = review_text.split()
                for term in terms:
                    yield (category, term), 1
        except Exception as e:
            pass
    
    def reducer_init(self):
        self.term_category_counts = {}
        self.category_totals = {}
        self.total_doc_count = 0
    
    def reducer(self, term_category, counts):
        category, term = term_category
        count = sum(counts)
        self.term_category_counts.setdefault(term, {})
        self.term_category_counts[term].setdefault(category, 0)
        self.term_category_counts[term][category] = count
        self.category_totals.setdefault(category, 0)
        self.category_totals[category] += count
        self.total_doc_count += count
    
    def reducer_final(self):
        for term, category_counts in self.term_category_counts.items():
            observed_counts = category_counts.values()
            expected_counts = [(self.category_totals[category] * sum(category_counts.values())) / self.total_doc_count for category in category_counts]
            chi_square_values = [pow(observed - expected, 2) / expected for observed, expected in zip(observed_counts, expected_counts)]
            yield term, chi_square_values
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, 
               reducer_init=self.reducer_init, 
               reducer=self.reducer, 
               reducer_final=self.reducer_final)
    ]


if __name__ == '__main__':
    ChiSquareCalculator.run()

