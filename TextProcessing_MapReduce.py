import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
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

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.second_reducer),
            MRStep(reducer=self.third_reducer),
            MRStep(reducer=self.fourth_reducer)
        ]

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
            yield ('*total', 1)
            yield ((token, '*total'), 1)
            yield ((category, '*total'), 1)

    def reducer(self, key, values):
       yield key, sum(values)

        

    def second_reducer(self, key, values):
        total = sum(values)
        if key[1] == '*total':
            self.increment_counter('total', key[0], total)
        else:
            chi_square = (total - self.get_counter('total', key[0]))**2 / self.get_counter('total', key[0])
            yield (key[1], -chi_square), key[0]

    
    def third_reducer(self, key, values):
        terms = list(values)[:75]
        for term in terms:
            yield None, term
        yield key[0], terms

    def fourth_reducer(self, key, values):
        if key is None:
           yield '*merged', ' '.join(sorted(set(values)))
        else:
            yield key, ' '.join(f'{term}:{chi_square}' for term, chi_square in values)


if __name__ == '__main__':
    ChiSquareWordCount.run()