import os
import pandas as pd
import uuid
from sklearn.model_selection import train_test_split
from data_preprocessing import preprocess_dataset  # Import from your data_preprocessing.py


def load_datasets():
    # Define columns and base path
    columns = ['id', 'label', 'statement', 'subject', 'speaker', 'job_title', 'state', 'party',
               'barely_true_counts', 'false_counts', 'half_true_counts', 'mostly_true_counts', 'pants_on_fire_counts',
               'context']

    base_path = 'data'

    # Load the datasets, assuming no headers in the TSV files
    train_df = pd.read_csv(os.path.join(base_path, 'train.tsv'), names=columns, sep='\t', header=None)
    test_df = pd.read_csv(os.path.join(base_path, 'test.tsv'), names=columns, sep='\t', header=None)
    val_df = pd.read_csv(os.path.join(base_path, 'valid.tsv'), names=columns, sep='\t', header=None)
    politifact_df = pd.read_csv(os.path.join(base_path, 'politifact_factchecks_20240919.csv'))
    snopes_df = pd.read_csv(os.path.join(base_path, 'snopes_factchecks_data.csv'))
    return train_df, test_df, val_df, politifact_df, snopes_df


def preprocess_datasets(df):
    """
    Apply preprocessing to each statement in the dataframe.

    Args:
        df (pd.DataFrame): The dataframe with a 'statement' column to preprocess.

    Returns:
        pd.DataFrame: The dataframe with cleaned 'statement' data.
    """
    df['statement'] = df['statement'].apply(preprocess_dataset)
    return df


def preprocess_liar(df):
    df['source'] = 'LIAR'
    return df[['label', 'statement', 'source']]


def preprocess_politifact(df):
    df['source'] = 'PolitiFact'
    return df[['rating', 'statement', 'source']].rename(columns={'rating': 'label'})


def preprocess_snopes(df):
    df['source'] = 'Snopes'
    return df[['rating', 'claim', 'source']].rename(columns={'rating': 'label', 'claim': 'statement'})


def combine_datasets(dfs):
    return pd.concat(dfs, ignore_index=True)


def create_unique_ids(df):
    df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df


def split_for_evaluation(df, eval_size=0.1):
    knowledge_base, eval_set = train_test_split(df, test_size=eval_size, random_state=42, stratify=df['source'])
    return knowledge_base, eval_set


def save_datasets(knowledge_base, eval_set, base_path='data'):
    knowledge_base_path = os.path.join(base_path, 'knowledge_base.csv')
    eval_set_path = os.path.join(base_path, 'evaluation_set.csv')
    knowledge_base.to_csv(knowledge_base_path, index=False)
    eval_set.to_csv(eval_set_path, index=False)

def main():

    base_path = 'data'
    # Load datasets
    train_df, test_df, val_df, politifact_df, snopes_df = load_datasets()

    # Preprocess LIAR datasets
    train_df = preprocess_datasets(train_df)
    test_df = preprocess_datasets(test_df)
    val_df = preprocess_datasets(val_df)

    # Preprocess PolitiFact dataset
    politifact_df = preprocess_datasets(preprocess_politifact(politifact_df))

    # Rename 'claim' to 'statement' first, then preprocess the Snopes dataset
    snopes_df = preprocess_snopes(snopes_df)
    snopes_df = preprocess_datasets(snopes_df)

    # Combine all datasets
    liar_dfs = [preprocess_liar(df) for df in (train_df, test_df, val_df)]
    all_dfs = liar_dfs + [politifact_df, snopes_df]
    combined_df = combine_datasets(all_dfs)

    # Create unique IDs
    combined_df = create_unique_ids(combined_df)
    print(combined_df.isna().sum())
    print(combined_df.shape)

    # Split for knowledge base and evaluation
    knowledge_base, eval_set = split_for_evaluation(combined_df)
    print(knowledge_base.shape)
    print(eval_set.shape)

    # Save datasets
    save_datasets(knowledge_base, eval_set, base_path)

    print("Data processing complete. Files saved: knowledge_base.csv, evaluation_set.csv")

    # Print some statistics
    print(f"Knowledge base size: {len(knowledge_base)}")
    print(f"Evaluation set size: {len(eval_set)}")
    print("\nSource distribution in knowledge base:")
    print(knowledge_base['source'].value_counts(normalize=True))
    print("\nSource distribution in evaluation set:")
    print(eval_set['source'].value_counts(normalize=True))


if __name__ == "__main__":
    main()




# Data processing complete. Files saved: knowledge_base.csv, evaluation_set.csv
# Knowledge base size: 11790
# Evaluation set size: 1311

# Source distribution in knowledge base:
# source
# LIAR          0.976336
# Snopes        0.012214
# PolitiFact    0.011450
# Name: proportion, dtype: float64

# Source distribution in evaluation set:
# source
# LIAR          0.976354
# Snopes        0.012204
# PolitiFact    0.011442
# Name: proportion, dtype: float64
