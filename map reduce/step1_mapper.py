from mrjob.job import MRJob

class ChiSquareCalculator(MRJob):
    
    def mapper(self, _, line):
        try:
            # Split the line into category and review text
            category, review_text = line.strip().split(' ', 1)
            if category and review_text:
                terms = review_text.split()  # Split review text into terms
                for term in terms:
                    yield (category, term), 1  # Emit key-value pairs: ((category, term), 1)
        except Exception as e:
            # Ignore lines with invalid format or missing category/review text
            pass
    
    def reducer(self, key, values):
        category, term = key
        count = sum(values)  # Count the occurrences of the term in the category
        # Emit key-value pairs: (category, (term, count))
        yield category, (term, count)

if __name__ == '__main__':
    ChiSquareCalculator.run()


