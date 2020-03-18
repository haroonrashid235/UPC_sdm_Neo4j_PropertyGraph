import argparse

from neo4j import GraphDatabase

database_keywords = [
    "Data Management",
    "Semantic Data",
    "Data Modeling",
    "Big Data",
    "Data Processing",
    "Data Storage",
    "Data Querying"
]


def get_DB_communities(search_keywords):
    with driver.session() as session:
        result = session.run(f"""
                MATCH (c)-[:HAS]->(p:Paper)-[:MENTIONS]->(k:Keyword)
                WHERE k.name IN {search_keywords}
                    WITH c, COLLECT(p.title) as papersList, toFloat(COUNT(p)) AS totalDBPapers
                    MATCH (c)-[:HAS]->(p1:Paper)
                    WITH c, totalDBPapers, papersList,  toFloat(COUNT(p1)) AS total, (totalDBPapers / toFloat(COUNT(
                    p1))) AS ratio 
                    WHERE ratio > 0.9
                    RETURN COLLECT(c.title) as confName, papersList
                """)

    return result


def get_top_papers(papers):
    with driver.session() as session:
        result = session.run(
            """
            CALL algo.pageRank.stream('Paper', 'CITED_BY', {iterations:1, dampingFactor:0.85})
            YIELD nodeId, score
            WITH algo.asNode(nodeId).title AS page, score
            WHERE page IN """ + str(papers) + """
            RETURN page, score
            ORDER BY score DESC LIMIT 100;
        """
        )
    return result


def get_gurus(top_papers):
    with driver.session() as session:
        result = session.run(
            """
            Match (a:Author)-[w:WRITES]->(p:Paper) 
            WHERE p.title IN """ + str(top_papers) + """ 
            WITH a.name as authorNames, count(w) as count
            WHERE count >= 2
            RETURN collect(authorNames) as Gurus
        """
        )
    return result


parser = argparse.ArgumentParser(description='DBLP Data Loader')
parser.add_argument('--uri', default="bolt://localhost:7687",
                    help='URI for the Neo4J database')
parser.add_argument('--user', default='neo4j',
                    help='Username for the neo4j database')
parser.add_argument('--password',
                    help='password for the neo4j database')


args = parser.parse_args()
uri = args.uri
user = args.user
password = args.password
driver = GraphDatabase.driver(uri, auth=(user, password))
