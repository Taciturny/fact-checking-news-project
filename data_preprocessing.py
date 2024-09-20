import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
# Load stopwords once to improve performance
STOP_WORDS = set(stopwords.words('english'))

def clean_text(text):
    """
    Clean the text by removing special characters and converting it to lowercase.

    Args:
        text (str): The input text to be cleaned.

    Returns:
        str: The cleaned text.
    """
    text = re.sub(r'[^\w\s]', '', text)  # Remove special characters
    return text.lower().strip()  # Convert to lowercase and strip whitespace


def remove_stopwords(text):
    """
    Remove stopwords from the text.

    Args:
        text (str): The input text.

    Returns:
        str: The text with stopwords removed.
    """
    words = word_tokenize(text)
    return ' '.join([word for word in words if word not in STOP_WORDS])


def preprocess_dataset(text):
    """
    Apply all preprocessing steps to the text.

    Args:
        text (str): The input statement.

    Returns:
        str: The preprocessed statement.
    """
    text = clean_text(text)
    text = remove_stopwords(text)
    return text
