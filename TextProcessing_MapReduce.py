import re
import json
from mrjob.job import MRJob
import argparse
import collections

STOPWORDS = set() #We will populate this with stop words

def load_stopwords(stopwords_file):
    """Load stopwords from a file"""
    with open(stopwords_file, 'r') as file:
        for line in file:
            STOPWORDS.add(line.strip())


class ChiSquareWordCount(MRJob):
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
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--stopwords', help='Path to stopwords file')
    parser.add_argument('--reviews', help='Path to reviews dataset')
    args = parser.parse_args()

    # Add your logic to load stopwords and run the MRJob here


    load_stopwords(args.stopwords)  
    ChiSquareWordCount.ARGS = [
        ('--reviews', args.reviews) 
    ]
    ChiSquareWordCount.run() 
