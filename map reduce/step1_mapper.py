from mrjob.job import MRJob
import json

class ChiSquareMapper(MRJob):
    
    def mapper(self, _, line):
        try:
            review_data = json.loads(line)
            category = review_data.get("category", "")
            terms = review_data.get("reviewText", "").split()
            for term in terms:
                yield (category, term), (1, 1)
        except:
            pass

if __name__ == '__main__':
    ChiSquareMapper.run()
