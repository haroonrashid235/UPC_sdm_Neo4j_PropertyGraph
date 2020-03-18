from recommender import *

if __name__ == "__main__":

    # Get the Database community conferences
    communities = get_DB_communities(database_keywords)

    papers_community_confs = []
    for com in communities:
        papers_community_confs.extend(com[1])

    # Get the top 100 papers within the community
    top_papers_result = get_top_papers(papers_community_confs)
    top_100_papers = []
    for paper in top_papers_result:
        top_100_papers.append(paper[0])

    # Get the Gurus
    gurus_result = get_gurus(top_100_papers)

    gurus = gurus_result.single()[0]
    print("Gurus...")
    for i, guru in enumerate(gurus):
        print(f"{i}: {guru}")