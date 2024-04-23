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
            MRStep(reducer=self.second_reducer)
            #MRStep(reducer=self.third_reducer)
            #MRStep(reducer=self.fourth_reducer)
        ]

    def mapper_init(self):

        #Initialize the mapper by loading stop words from the file specified in the arguments.
    
        load_stopwords(self.options.stopwords)


        
    def mapper(self, _, line):
        
        #Mapper function to tokenize, filter, and emit unigrams along with their categories.
        
        review = json.loads(line)
        text = review['reviewText']
        category = review['category']

        # Tokenize the text and filter out stop words and tokens with length 1
        tokens = re.split(r'\W+', text.lower())  
        filtered_tokens = [t for t in tokens if len(t) > 1 and t not in STOPWORDS]

        # Emit each unigram along with its category
        l = len(filtered_tokens)
        for token in filtered_tokens:
            yield (category, token, l), 1 

            # Increment counters for total unigrams, category-specific unigrams, and category totals
            self.increment_counter('total', '*total', 1)
            self.increment_counter('total', token, 1)
            self.increment_counter('total', category, 1)

    def reducer(self, key, values):
       
       #Reducer function to aggregate the counts of unigrams per category.
       yield key, sum(values)

        

    def second_reducer(self, key, values):
        
        #Second reducer function to calculate chi-square values for unigrams.
        total = key[2] # Get the total count from the key
        cur_value = next(values)
        # Calculate chi-square values for unigrams
        chi_square = (total - cur_value) ** 2 / cur_value
        new_key = (key[0], key[1], chi_square)
        yield new_key, None 

    
    def third_reducer(self, key, values):
        # Third reducer function to combie the output into one list, sort it and 
        # First, combine the 
        category = key[0]
        output_values = []
        valid_values = [value for value in values if isinstance(value, str) and ':' in value]
        output_values.append(f'{category} ' + ' '.join(valid_values))
        yield output_values, None
        

    def fourth_reducer(self, key, values):

        #Fourth reducer function to format the output according to the task requirements.

        output_values = []  # Initialize output_values here
        if key is None:
            yield '*merged', ' '.join(sorted(set(values)))
        else:
            # Extract category name from the key
            category = key
            # Filter out any non-string values and ensure they are in the correct format
            valid_values = [value for value in values if isinstance(value, str) and ':' in value]
            # Sort terms by their chi-square values in descending order
            sorted_values = sorted(valid_values, key=lambda x: float(x.split(':')[1]), reverse=True)
            # Select the top 75 terms for the category
            top_terms = sorted_values
            # Append top terms to output_values
            output_values.append(f'{category} ' + ' '.join(top_terms))
            yield output_values, None

if __name__ == '__main__':
    ChiSquareWordCount.run()