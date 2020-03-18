from neo4j import GraphDatabase


class GraphLoader:

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def load_conference(self):
        print('Loading conferences to Neo4J...')
        with self._driver.session() as session:
            session.run("""
                USING PERIODIC COMMIT 1000
                LOAD CSV WITH HEADERS FROM 'file:///output/conferences.csv' AS row FIELDTERMINATOR ';'
                MERGE (c:Conference { title: row.booktitle, startDate: toString(toInteger(row.year)) + '-01-01', 
                endDate: toString(toInteger(row.year)) + '-01-02'})
                """)
        print('Conferences loaded.')

    def add_index_to_conferences(self):
        with self._driver.session() as session:
            session.run('CREATE INDEX ON :Conference(title)')

    def load_journals(self):
        print('Loading journals to Neo4J...')
        with self._driver.session() as session:
            session.run("""
                USING PERIODIC COMMIT 1000
                LOAD CSV WITH HEADERS FROM 'file:///output/journals.csv' AS row FIELDTERMINATOR ';'
                MERGE (j:Journal { title: row.journal, date: toString(toInteger(row.year)) + '-01-01', volume: row.volume})
                MERGE (v:Volume {number:row.volume})
                MERGE (j)-[:BELONGS_TO]->(v)
                MERGE (v)-[:ISSUED_IN]->(y:Year {year: row.year})
            """)
            print('Journals loaded.')

    def add_index_to_journals(self):
        with self._driver.session() as session:
            session.run('CREATE INDEX ON :Journal(title)')
            session.run('CREATE INDEX ON :Journal(volume)')

    def add_index_to_authors(self):
        with self._driver.session() as session:
            session.run('CREATE INDEX ON :Author(name)')

    def load_conference_papers_and_keywords(self):
        print('Loading conference papers to Neo4J...')
        with self._driver.session() as session:
            session.run("""
                USING PERIODIC COMMIT 1000
                LOAD CSV WITH HEADERS FROM 'file:///output/conference_papers.csv' AS row FIELDTERMINATOR ';'
                MERGE (p:Paper { key: row.key, title: row.title, abstract: row.abstract})
                MERGE (c:Conference {title: row.booktitle, startDate: toString(toInteger(row.year)) + '-01-01' })
                WITH row, p, c
                UNWIND split(row.keywords, '|') AS keyword
                MERGE (k:Keyword {name: keyword})
                MERGE (c)-[:HAS]->(p)
                MERGE (p)-[:MENTIONS]->(k)
            """)
            print('Conference papers loaded.')

    def load_journal_papers_and_keywords(self):
        print('Loading journal papers to Neo4J...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/journal_papers.csv' AS row FIELDTERMINATOR ';'
                MERGE (p:Paper { key: row.key, title: row.title, abstract:row.abstract})
                MERGE (j:Journal { title: row.journal, date: toString(toInteger(row.year)) + '-01-01', volume: row.volume})
                WITH row, p, j
                UNWIND split(row.keywords, '|') AS keyword
                MERGE (k:Keyword {name: keyword})
                MERGE (j)-[:HAS]->(p)
                MERGE (p)-[:MENTIONS]->(k)
            """)
            print('Journal papers loaded.')

    def add_index_to_papers(self):
        with self._driver.session() as session:
            session.run('CREATE INDEX ON :Paper(key)')

    def load_conference_edition(self):
        print('Loading conference venues...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/conf_venues.csv' AS row FIELDTERMINATOR ';'
                MERGE (e:Edition {city:row.city, country:row.country})
                MERGE (c:Conference { title: row.booktitle })
                MERGE (y:Year {year: row.year})
                MERGE (c)-[:HAS_EDITION]-(e)
                MERGE (e)-[:HELD_IN]-(y)
            """)
        print('Conference venues loaded.')

    def load_conference_authors(self):
        print('Loading conference authors...')
        with self._driver.session() as session:
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output/lead_authors_conference.csv' AS row FIELDTERMINATOR ';'
                    MERGE (paper:Paper {key:row.key})
                    MERGE (author:Author { name: row.author})
                    MERGE (paper)<-[:WRITES{IS_LEAD_AUTHOR:true}]-(author)
                    """)
        with self._driver.session() as session:
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output/coauthors_conference.csv' AS row FIELDTERMINATOR ';'
                    MERGE (paper:Paper {key:row.key})
                    MERGE (coauthor:Author { name: row.author})
                    MERGE (paper)<-[:WRITES{IS_LEAD_AUTHOR:false}]-(author)
                    """)
        print('Conference authors loaded...')

    def load_journal_authors(self):
        print('Loading journal authors...')
        with self._driver.session() as session:
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output/lead_authors_journal.csv' AS row FIELDTERMINATOR ';'
                    MERGE (paper:Paper {key:row.key})
                    MERGE (author:Author { name: row.author})
                    MERGE (paper)<-[:WRITES{IS_LEAD_AUTHOR:true}]-(author)
                    """)
        with self._driver.session() as session:
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output/coauthors_journal.csv' AS row FIELDTERMINATOR ';'
                    MERGE (paper:Paper {key:row.key})
                    MERGE (coauthor:Author { name: row.author})
                    MERGE (paper)<-[:WRITES{IS_LEAD_AUTHOR:false}]-(coauthor)
                    """)
        print('Journal authors loaded...')

    def generate_random_citations(self):
        print('Generating random citations between papers...')
        with self._driver.session() as session:
            session.run("""
                    MATCH (p1:Paper)
                    WITH p1
                    MATCH (p2:Paper) WHERE p1 <> p2 AND rand() < 0.01
                    MERGE (p1)-[:CITED_BY]->(p2)
                    RETURN p1, p2
            """)
            print('Citations generated.')

    def add_reviewers_to_confernece_papers(self):
        print('Assigning conference paper reviewers...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/conference_reviews.csv' AS row FIELDTERMINATOR ';' 
                MATCH (p:Paper {key:row.key})
                UNWIND split(row.reviewer, '|') AS rev
                MATCH (a:Author {name: rev})
                MERGE (a)-[:REVIEWS]->(p)
            """)
        print('Conference paper reviewers assigned.')

    def add_reviewers_to_journal(self):
        print('Assigning reviewers to journals...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/journal_reviews.csv' AS row FIELDTERMINATOR ';' 
                MATCH (p:Paper {key:row.key})
                UNWIND split(row.reviewer, '|') AS rev
                MATCH (a:Author {name: rev})
                MERGE (a)-[:REVIEWS]->(p)
            """)
        print('Journal  paper reviewers assigned.')

    def evolve_conference_paper_reviews(self):
        print('Evoloving conference paper reviews...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/conference_reviews.csv' AS row FIELDTERMINATOR ';' 
                MATCH (p:Paper{key:row.key})
                UNWIND split(row.reviewer, '|') AS rev
                MATCH (a:Author {name: rev})
                MERGE (a)-[r:REVIEWS]->(p)
                SET r.decision = "accept"
                SET r.textual_description = row.textual_description
            """)
        print('Conference paper reviewers evolved.')

    def load_evolve_journal_paper_reviews(self):
        print('Loading journal paper reviewers...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/journal_reviews.csv' AS row FIELDTERMINATOR ';' 
                MATCH (p:Paper{key:row.key})
                UNWIND split(row.reviewer, '|') AS rev
                MATCH (a:Author {name: rev})
                MERGE (a)-[r:REVIEWS]->(p)
                SET r.decision = "accept"
                SET r.textual_description = row.textual_description
            """)
        print('Journal paper reviews evolved.')

    def evolve_authors_affiliations(self):
        print('Evolving Authors with their affiliations...')
        with self._driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output/authors_affiliation.csv' AS row FIELDTERMINATOR ';' 
                MERGE (o:Organization{name:row.affiliation})
                with row, o
                MATCH (a:Author {name: row.author})
                MERGE (a)-[:IS_AFFILIATED_TO]->(o)
            """)
        print("Authors affiliation evolved...")


