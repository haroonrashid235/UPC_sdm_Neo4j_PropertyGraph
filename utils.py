import os

import pandas as pd


def load_csv_to_data_frame(filename, header_filename=None, NUM_ROWS=10000):
    assert isinstance(filename, str)
    assert '.csv' in filename
    assert os.path.exists(filename)
    assert header_filename is None or '.csv' in header_filename

    header = None
    if header_filename is not None:
        header = pd.read_csv(header_filename, sep=';').columns.values

    # Read the csv file into the pandas dataframe
    if header is not None:
        header = [x.split(":")[0] for x in header]
        df = pd.read_csv(filename, sep=';', names=header, error_bad_lines=False, nrows=NUM_ROWS)
    else:
        df = pd.read_csv(filename, sep=';', error_bad_lines=False, nrows=NUM_ROWS)

    # df = df.loc[:, df.isnull().mean() < NULL_RATIO]
    df.dropna(axis=0, how='all', inplace=True)
    return df


def filter_columns(df, column_names):
    assert isinstance(df, pd.core.frame.DataFrame)
    assert isinstance(column_names, list)

    df = df[column_names]
    df = df.drop_duplicates().dropna()
    return df


def df_to_csv(df, save_path):
    assert isinstance(df, pd.core.frame.DataFrame)
    assert isinstance(save_path, str)
    assert '.csv' in save_path

    df.to_csv(save_path, index=False, sep=';')

# CLEAN_DIR = 'cleaned_output'
# os.makedirs(CLEAN_DIR, exist_ok=True)
#
# # Authors
# FILE_NAME = 'output_/output_author.csv'
# author_df = load_csv_to_data_frame(FILE_NAME)
# print("Authors")
# print(author_df.columns)
# author_df.to_csv(os.path.join(CLEAN_DIR, 'authors.csv'), index=False, header=True)
#
# # ARTICLES
# article_filename = 'output_/output_article.csv'
# article_header_filename = 'output_/output_article_header.csv'
# article_df = load_csv_to_data_frame(article_filename, header_filename=article_header_filename)
#
# print(article_df.columns)
# article_cols = ['article:ID', 'title:string', 'volume:string', 'year:int']
# article_df = filter_columns(article_df, article_cols)
# print("\nArticles")
# print(article_df.columns)
#
# # Conferences
# proceeding_filename = 'output_/output_inproceedings.csv'
# proceeding_filename_header = 'output_/output_inproceedings_header.csv'
# proceeding_df = load_csv_to_data_frame(proceeding_filename, header_filename=proceeding_filename_header)
# proceeding_cols = ['booktitle:string']
# proceeding_df = filter_columns(proceeding_df, proceeding_cols)
# print("\nInProceedings")
# print(proceeding_df.columns)
#
# # Journal
# journal_filename = 'output_/output_journal.csv'
# journal_df = load_csv_to_data_frame(journal_filename)
# print("\nJournal")
# print(journal_df.columns)
#
# # Modeling Relationships
#
# # Article Author Relationship
#
