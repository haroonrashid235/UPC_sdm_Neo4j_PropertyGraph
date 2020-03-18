# UPC_sdm_Neo4j_PropertyGraph
Lab 1 related to the Property Graphs for Semantic Data Management course at UPC

# DBLP Data Analysis using Propert Graphs

## Instructions

1. Converting the XML to CSV files, using [ThomHurks Repo](https://github.com/ThomHurks/dblp-to-csv)
```
python XMLToCSV.py xml_filename dtd_filename outputfile
```

2. Run ```dblp_loader.py``` to extract the relevant data from CSV files.

3. Copy the CSV files to the import/output directory of your Neo4j database.

4. Run ```PartA.2_RashidLim.py``` to load the data into neo4J.
```
python PartA.2_RashidLim.py --uri <uri> --user <neo4j-user> --password <neo4j-password>
```

5. Run ```PartA.3_RashidLim.py``` to evolve the graph.
```
python PartA.3_RashidLim.py --uri <uri> --user <neo4j-user> --password <neo4j-password>
```

6. Finally run the ```PartD_RashidLim.py``` to find the review recommendations.
```
python PartD_RashidLim.py --uri <uri> --user <neo4j-user> --password <neo4j-password>
```

