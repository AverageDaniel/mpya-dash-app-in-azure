# mpya-dash-app-in-azure

## Repo for mpya-dashboard to be deployed in Azure

The dashboard has been built using the Python library Dash. To learn more about Dash, check their tutorials and documentation at https://dash.plotly.com/ . It's fairly simple and straightforward. 
Daniel Jakobsson has some knowledge about the structure of this repo/app so further questions about the dashboard can be directed to him(or David Rüdel and Cecilia Karlsson about the project in general).


To install dependencies run:
```
pip install -r requirements.txt
```

To start the app locally, set the variable ```local=True``` in **app.py** (at the bottom of the script) and run:

```
python app.py
```
The dashboard should then be running on: http://localhost:8050/

Depending on what the user inputs to the different fields in the dashboard, different datasets are loaded from the folder **local-data/Data** and presented to the user. It should look something like:

![image](https://user-images.githubusercontent.com/113591842/203738415-afa62d3b-31a4-4159-9da7-5e942ab6ac03.png)

The profiles are defined in **profiles.json** in a JSON-object with the following structure:
```
profiles = {"Team": {"Profile": {"name": "profile-name",
                                 "keywords": [["search-phrase", "search-phrase"], ["search-phrase", "search-phrase"]]}}}
```
Where the lists in "keywords" are used to build a search query when filtering ads from AF. The queries are built from the list by adding OR between every phrase in the innermost list, and adding AND between each list. So for example "keywords" for the profile *Quality Life Science-Medtech* is defined as:
```
"keywords": [["Quality", "Kvalitet", "QA", "Quality Assurance"], ["Medtech", "Life Science"]]
```
Which will be transformed into the search query:
```
("Quality" OR "Kvalitet" OR "QA" OR "Quality Assurance") AND ("Medtech" OR "Life Science")
```

New profiles can be created by defining and adding them to **profiles.json** that can be found in **local-data/Data**. Then run:
```
python utils.py
```
This will pull the necessary data from the [AF data source](https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/index.html), filter the ads using the search queries of the profiles, scrape company info from [Alla Bolag](https://www.allabolag.se/), extract titles and competences for the ads using the [JobAd Enrichments API](https://jobad-enrichments-api.jobtechdev.se/)
and store all of this in **local-data/Data** where each profile has its own zip-folder. **utils.py** can also be ran to check for new data from AF. If there is new data available this data will be pulled, filtered and stored for all profiles(old and new). If no new data is available and no new profiles have been added, the script will say so. The data source is updated every quarter. The file **processed.json** can be found in **local-data/Data** and it keeps track of what data has been pulled, filtered and stored for each profile, and it is continously updated by **utils.py**

If you want to make changes to a profile just update it in **profiles.json** and when you run **utils.py** it will notice the changes and re-filter the data. If you want to remove a profile completely, remove its zip-folder in **local-data/Data** and also make sure to remove the profile from **profiles.json** so that it is not available in the front-end.
