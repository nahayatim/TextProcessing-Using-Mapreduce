import re
import sys
import os
import json

# Get the path to the input file from command line arguments
input_path = sys.argv[1]
stopwords_path = sys.argv[2]

# Make sure the path is relative to the script location
reviews_path = os.path.join(os.path.dirname(__file__), reviews_path)
stopwords_path = os.path.join(os.path.dirname(__file__), stopwords_path)

# Read the reviews from the JSON file
with open(reviews_path, 'r') as file:
    reviews = json.load(file)

# Read the stopwords from the text file
with open(stopwords_path, 'r') as file:
    stopwords = file.read().splitlines()


# Define the delimiters
delimiters = r'\s|\t|\d|\(|\)|\[|\]|\{|\}|\.|\!|\?|\,|\;|\:|\+|\=|\-|\_|\\"|\'|\`|\~|\#|\@|\&|\*|\%|\€|\$|\§|\/'

# Preprocessing function
def preprocess_review(review):
    # Tokenization
    tokens = re.split(delimiters, review)
    
    # Case Folding
    tokens = [token.lower() for token in tokens]
    
    # Stopword Filtering and filter out tokens of length 1
    tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
    
    return tokens

# Apply preprocessing to each review
preprocessed_reviews = [preprocess_review(review) for review in reviews]

