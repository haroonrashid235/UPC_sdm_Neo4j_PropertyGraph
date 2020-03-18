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

    graph_loader.evolve_conference_paper_reviews()
    graph_loader.load_evolve_journal_paper_reviews()
    graph_loader.evolve_authors_affiliations()

    graph_loader.close()