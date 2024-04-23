import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import collections

STOPWORDS = set() #We will populate this with stop words

def load_stopwords(file_path):
    #Load stop words from a file and populate the STOPWORDS set
    with open(file_path, 'r') as f:
        for line in f:
            STOPWORDS.add(line.strip())

class ChiSquareWordCount(MRJob):

    def configure_args(self):
        #Configure arguments for the MapReduce job, including the path to the stop words file.
        super(ChiSquareWordCount, self).configure_args()
        self.add_file_arg('--stopwords')

    def steps(self):
        #Define the MapReduce steps
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.second_reducer),
            MRStep(reducer=self.third_reducer),
            MRStep(reducer=self.fourth_reducer)
        ]

    def mapper_init(self):

        #Initialize the mapper by loading stop words from the file specified in the arguments.
        #load_stopwords(self.options.stopwords)
        with open(self.options.stopwords, 'r') as f:
            for line in f:
                STOPWORDS.add(line.strip())

    @staticmethod
    def calculate_chi_square(N, W, X, Y, Z):
        return N * (W * Z - X * Y)**2 / ((W + X) * (Y + Z) * (W + Y) * (X + Z))
        
    def mapper(self, _, line):
        
        #Mapper function to tokenize, filter, and emit unigrams along with their categories.
        review = json.loads(line)
        text = review['reviewText']
        category = review['category']

        # Tokenize the text and filter out stop words and tokens with length 1
       # Pre-compile the regular expression pattern
        token_pattern = re.compile(r'[\s\t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_"\'`~#@&*%€$§\\/]+')
        # Use the compiled regex object to split the text
        tokens = token_pattern.split(text.lower())
        filtered_tokens = [t for t in tokens if len(t) > 1 and t not in STOPWORDS]
    
        # Emit each unigram along with its category
        for token in filtered_tokens:
            yield (token, category), 1 
            yield ('*total', category), 1
            yield (token, '*total'), 1
            yield ('*total', '*total'), 1


    def reducer(self, key, values):
       #Reducer function to aggregate the counts of unigrams per category.
       total = sum(values)
       yield None, (key, total)

        

    def second_reducer(self, _, values):
        #Second reducer function to calculate chi-square values for unigrams.
        counts = collections.defaultdict(lambda: collections.defaultdict(int))
        total_counts = collections.defaultdict(int)
        for (word, category), count in values:
            counts[word][category] = count
            total_counts[word] += count
            total_counts[category] += count
        N = sum(total_counts.values())
        for word, categories in counts.items():
            for category, W in categories.items():
                X = total_counts[word] - W
                Y = total_counts[category] - W
                Z = N - W - X - Y
                chi_square = self.calculate_chi_square(N, W, X, Y, Z)
                yield (category, -chi_square), (word, chi_square)

    
    def third_reducer(self, key, values):
        # Extract category name from the key
        category = key[0]
        # Sort the values (terms) by their chi-square values in descending order
        sorted_values = sorted(values, key=lambda x: float(x[1]), reverse=True)
        # Select the top 75 terms per category
        top_terms = sorted_values[:75]
        # Yield the category name and the top 75 terms
        yield category, ' '.join([f'{term}:{chi:.2f}' for term, chi in top_terms])

    def fourth_reducer(self, key, values):
        #Fourth reducer function to format the output according to the task requirements.
        output_values = []  # Initialize output_values here
        if key is None:
            # Flatten the list of terms and remove duplicates
            all_terms = sorted(set(item for sublist in values for item in sublist))
            yield '*merged', ' '.join(all_terms)
        else:
            # Extract category name from the key
            category = key
            # Flatten the list of terms
            all_terms = [item for sublist in values for item in sublist]
            # Yield the category name and all terms
            yield category, ' '.join(all_terms)

if __name__ == '__main__':
    ChiSquareWordCount.run()
