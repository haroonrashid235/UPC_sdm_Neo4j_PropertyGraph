import random
import nltk
import lorem
import numpy as np

nltk.download('brown')
nltk.download('punkt')

from utils import *
from textblob import TextBlob
from geotext import GeoText

class DBLP_DataLoader():

    def __init__(self):
        self.db_keywords = [
            "edition",
            "proceeding",
            "artificial intelligence",
            "machine learning",
            "Data Management",
            "Semantic Data",
            "Data Modeling",
            "Big Data",
            "Data Processing",
            "Data Storage",
            "Data Querying"
        ]

    def generate_abstract(self, row):
        return lorem.paragraph()

    def is_lead_author(self, row):
        author_name = row['author']
        last_name = author_name.split(' ')[-1]
        return last_name in row['key']

    def trim_all_columns(self, df):
        """
        Trim whitespace from ends of each value across all series in dataframe
        """
        trim_strings = lambda x: x.strip() if isinstance(x, str) else x
        return df.applymap(trim_strings)

    def extract_keywords_from_title(self, title):
        k = 5
        blob = TextBlob(title)
        keywords = blob.noun_phrases
        keywords = random.choices(self.db_keywords, k=3) + keywords
        if len(keywords) >= k:
            return "|".join(random.choices(keywords, k=k))
        else:
            return "|".join(keywords)

    def extract_venue_from_text(self, text):
        cities = GeoText(text).cities
        countries = GeoText(text).countries
        if cities and countries:
            return cities[-1] + "|" + countries[-1]
        return None

    def get_random_reviewers(self, authors, reviewers):
        current_authors = set(authors)
        reviewers = [x for x in reviewers if x not in current_authors]
        return "|".join(random.choices(reviewers, k=3))

    def extract_conference_papers(self):
        print('Extracting conference papers...')
        df = load_csv_to_data_frame(filename='output_/output_proceedings.csv',
                                    header_filename='output_/output_proceedings_header.csv')

        columns = ['key', 'title', 'booktitle', 'year']

        df = filter_columns(df, columns)

        # Extract keywords
        df['keywords'] = df['title'].apply(self.extract_keywords_from_title)

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['abstract'] = df.apply(self.generate_abstract, axis=1)

        df_to_csv(df, 'output/conference_papers.csv')
        print('Conference papers extracted.')

    def extract_journal_papers(self):
        print('Extracting journal papers...')
        df = load_csv_to_data_frame(filename='output_/output_article.csv',
                                    header_filename='output_/output_article_header.csv')

        columns = ['key', 'title', 'journal', 'year', 'volume']
        df = filter_columns(df, columns)

        # Extract keywords
        df['keywords'] = df['title'].apply(self.extract_keywords_from_title)

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['abstract'] = df.apply(self.generate_abstract, axis=1)

        df = self.trim_all_columns(df)
        df_to_csv(df, 'output/journal_papers.csv')
        print('Journal papers extracted.')

    def extract_conferences(self):
        print('Extracting conferences...')
        df = load_csv_to_data_frame(filename='output_/output_inproceedings.csv',
                                    header_filename='output_/output_inproceedings_header.csv')

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        # Extract useful columns
        columns = ['booktitle', 'year']
        df = filter_columns(df, columns)

        df_to_csv(df, 'output/conferences.csv')
        print('Conferences extracted.')

    def extract_journals(self):
        print('Extracting journals...')
        df = load_csv_to_data_frame(filename='output_/output_article.csv',
                                    header_filename='output_/output_article_header.csv')

        # Extract useful columns
        columns = ['journal', 'volume', 'year']
        df = filter_columns(df, columns)

        # Ignoring rows with non-numerical value in year column
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        df_to_csv(df, 'output/journals.csv')
        print('Journals extracted.')

    def extract_conference_venues(self):
        print('Extracting cities...')
        df = load_csv_to_data_frame(filename='output_/output_proceedings.csv',
                                    header_filename='output_/output_proceedings_header.csv')

        df['venue'] = df['title'].apply(self.extract_venue_from_text)
        df[['city', 'country']] = df['venue'].str.split("|", expand=True)

        df = filter_columns(df, ['booktitle', 'title', 'year', 'city', 'country'])

        df_to_csv(df, 'output/conf_venues.csv')
        print('Conference cities extracted')

    def extract_conference_authors(self):
        print('Extracting authors from conference papers...')
        df = load_csv_to_data_frame(filename='output_/output_inproceedings.csv',
                                    header_filename='output_/output_inproceedings_header.csv')

        # Extract useful columns
        columns = ['author', 'key']
        df = filter_columns(df, columns)

        df['author'] = df['author'].str.split('|')

        df = df.set_index(['key'])['author'].apply(pd.Series).stack(
        ).reset_index(name='author').drop('level_1', axis=1)

        df['is_lead_author'] = df.apply(self.is_lead_author, axis=1)

        df_lead_author = df[df['is_lead_author'] == True]
        df_coauthor = df[df['is_lead_author'] == False]

        # Extract useful columns
        df_lead_author = filter_columns(df_lead_author, ['key', 'author'])
        df_coauthor = filter_columns(df_coauthor, ['key', 'author'])

        df_to_csv(df_lead_author, 'output/lead_authors_conference.csv')
        df_to_csv(df_coauthor, 'output/coauthors_conference.csv')
        print('Authors from conference papers extracted.')

    def extract_journal_authors(self):
        print('Extracting authors from journal papers...')
        df = load_csv_to_data_frame(filename='output_/output_article.csv',
                                    header_filename='output_/output_article_header.csv')

        # Extract useful columns
        columns = ['author', 'key']
        df = filter_columns(df, columns)

        df['author'] = df['author'].str.split('|')

        df = df.set_index(['key'])['author'].apply(pd.Series).stack(
        ).reset_index(name='author').drop('level_1', axis=1)

        df['is_lead_author'] = df.apply(self.is_lead_author, axis=1)

        df_lead_author = df[df['is_lead_author'] == True]
        df_coauthor = df[df['is_lead_author'] == False]

        # Extract useful columns
        df_lead_author = filter_columns(df_lead_author, ['key', 'author'])
        df_coauthor = filter_columns(df_coauthor, ['key', 'author'])

        df_to_csv(df_lead_author, 'output/lead_authors_journal.csv')
        df_to_csv(df_coauthor, 'output/coauthors_journal.csv')
        print('Authors from journal papers extracted.')

    def generate_conference_reviewers(self):
        print("Generating conference's reviewers")
        df = load_csv_to_data_frame(filename='output_/output_inproceedings.csv',
                                    header_filename='output_/output_inproceedings_header.csv')

        df_authors = filter_columns(df, ['author', 'key'])

        df_authors['author'] = df_authors['author'].str.split('|')

        df_authors_all = df_authors.set_index(['key']).author.apply(
            pd.Series).stack().reset_index(name='author').drop('level_1', axis=1)

        authors = df_authors_all['author'].tolist()

        df_authors['reviewer'] = df_authors['author'].apply(
            lambda author: self.get_random_reviewers(author, authors))

        df_authors = filter_columns(df_authors, ['key', 'reviewer'])
        df_authors['textual_description'] = "This is a random review of your paper"

        df_to_csv(df_authors, 'output/conference_reviews.csv')
        print("Conference's reviews generated.")

    def generate_journal_reviewers(self):
        print("Generating random journal's reviewers")
        df = load_csv_to_data_frame(filename='output_/output_article.csv',
                                    header_filename='output_/output_article_header.csv')

        df_authors = filter_columns(df, ['author', 'key'])
        df_authors['author'] = df_authors['author'].str.split('|')

        df_authors_all = df_authors.set_index(['key']).author.apply(
            pd.Series).stack().reset_index(name='author').drop('level_1', axis=1)

        authors = df_authors_all['author'].tolist()

        df_authors['reviewer'] = df_authors['author'].apply(
            lambda author: self.get_random_reviewers(author, authors))

        df_authors = filter_columns(df_authors, ['key', 'reviewer'])

        df_authors['textual_description'] = "This is a random review of your paper"

        df_to_csv(df_authors, 'output/journal_reviews.csv')
        print("Journal's reviews generated.")

    @staticmethod
    def _concat_all_authors():
        df_lead_authors_conference = load_csv_to_data_frame(
            'output/lead_authors_conference.csv')
        df_lead_authors_conference = df_lead_authors_conference['author']

        df_lead_authors_journal = load_csv_to_data_frame('output/lead_authors_journal.csv')
        df_lead_authors_journal = df_lead_authors_journal['author']

        df_coauthors_conference = load_csv_to_data_frame('output/coauthors_conference.csv')
        df_coauthors_conference = df_coauthors_conference['author']

        df_coauthors_journal = load_csv_to_data_frame('output/coauthors_journal.csv')
        df_coauthors_journal = df_coauthors_journal['author']

        final_df = pd.concat([df_lead_authors_conference, df_lead_authors_journal,
                              df_coauthors_conference, df_coauthors_journal])
        return final_df

    def get_authors_affiliation(self):
        print("Getting author's affiliation")
        df_schools = load_csv_to_data_frame(filename='output_/output_school.csv')
        df_schools.rename(columns={'school:string': 'school',
                                   ':ID': 'school_id'},
                          inplace=True)
        schools_list = df_schools['school'].values.tolist()

        df_authors = pd.DataFrame(self._concat_all_authors())
        df_authors['affiliation'] = np.random.choice(schools_list, size=len(df_authors))

        df_to_csv(df_authors, 'output/authors_affiliation.csv')
        print("Author's affiliation generated")


if __name__ == '__main__':
    data_loader = DBLP_DataLoader()
    data_loader.extract_conferences()
    data_loader.extract_journals()
    data_loader.extract_conference_papers()
    data_loader.extract_journal_papers()
    data_loader.extract_conference_venues()
    data_loader.extract_conference_authors()
    data_loader.extract_journal_authors()
    data_loader.generate_conference_reviewers()
    data_loader.generate_journal_reviewers()
    data_loader.get_authors_affiliation()
