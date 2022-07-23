# Big-Data-Technologies - University Ranking

Project run by Patrick Montanari and Davide Longo, carried as an exercise for the class of Big Data Technologies as a part of our Master's Degree in Data Science.

The main goal is to provide a mean through which any user can have access to rankings of all italian universities for a given year, visualized through a front-end application and corresponding interactive interface.

- Index:
> get_ministry_data.ipynb is used to produce a list of datasets published by Italian's Ministry of  University and Scientific Research. 
What it produces is a dictionary with each database being a key with corresponding url subsection as value.

> google_cloud_storage.ipynb is used to store all the datasets present within the dictionary produced by get_ministry_data in our Google Cloud Storage Bucket, temporarily downloading it and removing it immediately after. A class is introduced in order to reiterate this process on a fixed interval of 15 days.
