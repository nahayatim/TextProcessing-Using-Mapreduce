import json
import re

# Load stopwords from file
with open("stopwords.txt", "r") as f:
    stopwords = set(f.read().splitlines())

# Define regex pattern for tokenization
pattern = re.compile(r"[\s\t\d\(\)\[\]\{\}\.\!\?;:,\+=\-_\"'`~#@&\*%€\$§\\/]+")

def preprocess_review(review_text):
    # Tokenization using regex pattern
    tokens = pattern.split(review_text)
    # Convert tokens to lowercase
    tokens = [token.lower() for token in tokens]
    # Filter out stopwords and tokens consisting of only one character
    tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
    return tokens

# Function to process each line of the input file
def process_line(line):
    try:
        review_data = json.loads(line)
        review_text = review_data.get("reviewText", "")
        if review_text:
            tokens = preprocess_review(review_text)
            return tokens
    except Exception as e:
        # Ignore lines with invalid JSON or missing review text
        pass
    return []

if __name__ == "__main__":
    import sys
    # Process input file provided as command-line argument
    input_file = sys.argv[1]
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            tokens = process_line(line)
            if tokens:
                print(" ".join(tokens))
