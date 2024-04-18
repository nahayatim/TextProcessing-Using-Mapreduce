import re
import json
from mrjob.job import MRJob
import collections

STOPWORDS = set() #We will populate this with stop words

def load_stopwords(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            STOPWORDS.add(line.strip())

class ChiSquareWordCount(MRJob):

    def configure_args(self):
        super(ChiSquareWordCount, self).configure_args()
        self.add_file_arg('--stopwords')

    def mapper_init(self):
        load_stopwords(self.options.stopwords)


        
    def mapper(self, _, line):
        review = json.loads(line)
        text = review['reviewText']
        category = review['category']

        tokens = re.split(r'\W+', text.lower())  
        filtered_tokens = [t for t in tokens if len(t) > 1 and t not in STOPWORDS]

        for token in filtered_tokens:
            yield (token, category), 1 

    def reducer(self, key, values):
        word, category = key
        word_count = sum(values)  # Count of the word within the category

        # Data structure to store counts
        self.counts = collections.defaultdict(lambda: {
            'word_count': 0,
            'category_count': 0,
            'overall_count': 0
        })  

        # Update counts
        self.counts[word]['word_count'] += word_count
        self.counts[word]['category_count'] += 1  # Count occurrences of this category
        self.counts[word]['overall_count'] += word_count 

    def final(self):
        """Emit aggregated counts for chi-square calculations"""
        for word, data in self.counts.items():
            yield word, str(data)

if __name__ == '__main__':
    ChiSquareWordCount.run()