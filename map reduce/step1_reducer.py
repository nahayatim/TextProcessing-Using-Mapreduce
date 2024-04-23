from mrjob.job import MRJob
from heapq import nlargest

class MergeTopTerms(MRJob):
    
    def mapper(self, _, line):
        category, term_chi2 = line.strip().split('\t')
        term, chi2 = term_chi2.split(':')
        yield None, (float(chi2), term, category)  # Emit key-value pairs: (None, (chi-square, term, category))
    
    def reducer_init(self):
        self.top_terms = {}
        
    def reducer(self, _, values):
        for chi2, term, category in values:
            if category not in self.top_terms:
                self.top_terms[category] = []
            self.top_terms[category].append((chi2, term))
        
    def reducer_final(self):
        # Emit the top 75 terms for each category
        for category, top_terms in self.top_terms.items():
            top_75 = nlargest(75, top_terms)
            for chi2, term in top_75:
                yield category, f"{term}:{chi2}"

if __name__ == '__main__':
    MergeTopTerms.run()

