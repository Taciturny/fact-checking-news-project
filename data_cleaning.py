import pandas as pd
import re
from typing import Dict
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


# Load stopwords once to improve performance
STOP_WORDS = set(stopwords.words('english'))

def load_us_states() -> Dict[str, str]:
    """Load a dictionary of US states and territories with abbreviations."""
    return {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
        'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
        'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
        'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon',
        'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
        'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
        'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'Washington D.C.',
        'PR': 'Puerto Rico', 'GU': 'Guam', 'VI': 'U.S. Virgin Islands', 'AS': 'American Samoa',
        'MP': 'Northern Mariana Islands'
    }

def clean_state(state: pd.Series, state_mappings: Dict[str, str]) -> pd.Series:
    def clean_single_state(state):
        if pd.isna(state):
            return "Unknown"

        state = str(state).strip().lower()

        # Handle Washington D.C. variations
        if re.match(r'washington,?\s*d\.?c\.?', state):
            return 'Washington D.C.'

        # Handle state abbreviations
        state_upper = state.upper()
        if state_upper in state_mappings:
            return state_mappings[state_upper]

        # Remove non-state qualifiers
        state = re.sub(r'\s*-.*$', '', state)

        # Correct common misspellings and variations
        misspellings = {
            'virgina': 'Virginia',
            'virgiia': 'Virginia',
            'tennesse': 'Tennessee',
            'tex': 'Texas',
            'pa - pennsylvania': 'Pennsylvania',
            'rhode island': 'Rhode Island',
            'washington, d.c.': 'Washington D.C.',
            'district of columbia': 'Washington D.C.',
            'washington dc': 'Washington D.C.',
            'washington d.c.': 'Washington D.C.',
            'atlanta': 'Georgia',
            'virgina director, coalition to stop gun violence': 'Virginia',
            'the united states': 'Unknown',
            'china': 'Unknown',
            'russia': 'Unknown',
            'qatar': 'Unknown',
            'united kingdom': 'Unknown'
        }

        if state in misspellings:
            return misspellings[state]

        return state.title()

    # Apply the cleaning function to the state column
    return state.apply(clean_single_state)

def clean_party(party):
    if pd.isna(party):
        return 'Unknown'

    party = str(party).lower().strip()

    # Major parties
    if 'republican' in party:
        return 'Republican'
    if 'democrat' in party:
        return 'Democrat'
    if 'independent' in party:
        return 'Independent'

    # Minor parties to keep
    minor_parties = ['libertarian', 'green', 'tea party', 'constitution']
    for minor in minor_parties:
        if minor in party:
            return party.title()

    # Group others as 'Other'
    return 'Other'

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Load the state mappings
    state_mappings = load_us_states()

    # Clean state column
    df['state_cleaned'] = clean_state(df['state'], state_mappings)

    # Flag non-US entries for review instead of removing them
    df['is_us_state'] = df['state_cleaned'].isin(state_mappings.values())

    # Flag non-standard entries (not US states or flagged as 'Unknown')
    df['flagged_for_review'] = df['state_cleaned'].apply(lambda x: x == "Unknown")

    # Clean party column
    df['party_cleaned'] = df['party'].apply(clean_party)

    return df





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


def preprocess_statement(text):
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


def handle_missing_values(df):
    """
    Handle missing values in the dataframe. Missing values in high-missing columns
    are filled with 'Unknown'. Columns with low missing values are forward filled.

    Args:
        df (pd.DataFrame): The input dataframe with possible missing values.

    Returns:
        pd.DataFrame: The dataframe with handled missing values.
    """
    # Columns with a high percentage of missing values
    high_missing_cols = ['job_title', 'state', 'context']
    df[high_missing_cols] = df[high_missing_cols].fillna('Unknown')

    # Columns with low missing values (we assume forward fill is appropriate here)
    low_missing_cols = ['subject', 'speaker', 'party', 'barely_true_counts', 'false_counts',
                        'half_true_counts', 'mostly_true_counts', 'pants_on_fire_counts']
    df[low_missing_cols] = df[low_missing_cols].ffill()

    return df


def preprocess_dataset(df):
    """
    Apply all preprocessing steps to the dataset including handling missing values
    and text processing of the statements.

    Args:
        df (pd.DataFrame): The input dataframe containing the dataset.

    Returns:
        pd.DataFrame: The processed dataframe with cleaned and tokenized statements.
    """
    # Handle missing values
    df = handle_missing_values(df)

    # Apply text preprocessing to the 'statement' column
    df['processed_statement'] = df['statement'].apply(preprocess_statement)

    return df




def print_cleaning_summary(df: pd.DataFrame, column: str):
    print(f"\nCleaning summary for {column}:")
    print(df[column].value_counts().head(10))  # Show top 10 original values
    print("\nAfter cleaning:")
    print(df[f"{column}_cleaned"].value_counts().head(10))  # Show top 10 cleaned values

    if column == 'state':
        non_us = df[~df['is_us_state']]
        print(f"\nEntries not recognized as US states/territories: {len(non_us)}")
        if len(non_us) > 0:
            print(non_us['state_cleaned'].value_counts().head(10))
