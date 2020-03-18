import argparse
from graph_loader import GraphLoader

parser = argparse.ArgumentParser(description='DBLP Data Loader')
parser.add_argument('--uri', default="bolt://localhost:7687",
                    help='URI for the Neo4J database')
parser.add_argument('--user', default='neo4j',
                    help='Username for the neo4j database')
parser.add_argument('--password',
                    help='password for the neo4j database')

args = parser.parse_args()

if __name__ == "__main__":
    user = args.user
    password = args.password
    uri = args.uri

    graph_loader = GraphLoader(uri, user, password)
    graph_loader.load_conference()
    graph_loader.add_index_to_conferences()
    graph_loader.load_journals()
    graph_loader.add_index_to_journals()
    graph_loader.load_conference_papers_and_keywords()
    graph_loader.load_journal_papers_and_keywords()
    graph_loader.add_index_to_papers()
    graph_loader.load_conference_edition()
    graph_loader.load_conference_authors()
    graph_loader.load_journal_authors()
    graph_loader.add_index_to_authors()
    graph_loader.generate_random_citations()
    graph_loader.add_reviewers_to_confernece_papers()
    graph_loader.add_reviewers_to_journal()
    graph_loader.close()

