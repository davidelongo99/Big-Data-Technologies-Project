# Big-Data-Technologies - University Ranking

Project run by Patrick Montanari and Davide Longo, carried as an exercise for the class of Big Data Technologies as a part of our Master's Degree in Data Science.

The main goal is to provide a mean through which any user can have access to rankings of all italian universities for a given year, visualized through a front-end application and corresponding interactive interface.

> Index:

- data_ingestion.py

Is used to store all the datasets found in the dedicated miur website in our Google Cloud Storage Bucket. 
A class is introduced in order to reiterate this process on a fixed interval of 15 days.

- dataproc.py

Creates clusters on Google Dataproc. We chose the server europe-west8 as it is the closest and within our area of interests, the italian country.


- pyspark_job.py

Is our main data processing script; starting from the original data, we create dataframes containing the columns we are interested in, with the appropriate attributes and data type.

- scheduler.py

Allows us to repeat the three steps above on a weekly basis, every monday.

- urlfinder.py 

Consists in a simple script used to visualize the rankings, produced externally using BigQuery and Google's Datastudio. 
When run, it asks the user to choose the ranking he wants to see and, if given one of the four keywords (with or withour capital letters), it opens the corresponding ranking in a new browser page. These rankings are interactive and updated in realt time, and can even be filtered by university size or type of entity (public or private).


The three final rankings can be accessed using urlfinder.py, or visiting the links below:
https://datastudio.google.com/reporting/f0250ab0-d94f-4cb4-b2e1-66198b98ff7b
https://datastudio.google.com/reporting/c0912393-9e85-4097-a7cc-94a0ea40cd6a
https://datastudio.google.com/reporting/99d44256-d1e6-432a-aea0-bc86dace4c90


