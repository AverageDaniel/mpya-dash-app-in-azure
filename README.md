# mpya-dash-app-in-azure

## Repo for mpya-dashboard to be deployed in Azure

To install dependencies run:
```
pip install -r requirements.txt
```

To start app locally, set the variable ```local=True``` in **app.py** and run:

```
python app.py
```
Depending on what the user inputs to the different fields in the dashboard, different datasets are loaded from the folder **"local-data/Data"** and presented to the user.

The profiles are defined in **profiles.json** in a JSON-object with the following structure:
```
profiles = {"Team": {"Profile": {"name": "profile-name",
                                 "keywords": [["search-phrase", "search-phrase"], ["search-phrase", "search-phrase"]]}}}
```
Where the lists in "keywords" are used to build a search query when filtering ads from AF. The queries are built from the list by adding OR between every phrase in the innermost list, and adding AND between each list. So for example the profile *Quality Life Science-Medtech* is defined as:
```
"keywords": [["Quality", "Kvalitet", "QA", "Quality Assurance"], ["Medtech", "Life Science"]]
```
Which will be transformed into the search query:

("Quality" OR "Kvalitet" OR "QA" OR "Quality Assurance") AND ("Medtech" OR "Life Science")

New profiles can be created by defining and adding them to **profiles.json**. Then run:
```
python utils.py
```
This will pull the necessary data from the [AF data source](https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/index.html), filter it and store datasets for the profiles. **utils.py** can also be ran to check for new data from AF. If there is new data available this data will be pulled, filtered and stored for all profiles(old and new). The data source is updated every quarter. The file **processed.json** kepps track of what data has been pulled, filtered and stored for each profile. It is updated by **utils.py**
