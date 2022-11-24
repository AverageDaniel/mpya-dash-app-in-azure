import pandas as pd
import numpy as np
import json
import requests
import os
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm
import base64
import bs4 as bs
from requests_html import HTMLSession
from ast import literal_eval

#from memory_profiler import profile

##########################################################################################################

# Helper functions

def first_to_upper(s):
    res = ""
    for w in str(s).split(' '):
        if w != []:
            res += w[:1].upper()+w[1:]+" " 
    return res[:-1]

def rgb_adder(rgb, percent):
    nums = [str(int(c)*(1+percent)) for c in rgb[4:-1].split(",")]
    return "rgb("+nums[0]+", "+nums[1]+", "+nums[2]+")"

def check_update_need(profile, team, profiles):
    if team not in profiles.keys():
        return True
    elif profile['name'] not in profiles[team].keys():
        return True
    curr_prof = profiles[team][profile['name']]
    if set(curr_prof['keywords']) == set(profile['keywords']):
        return False
    else:
        return True

def get_titles_from_comp(df, comp):
    headlines = df[df.company == comp].headline.value_counts()
    return headlines

##########################################################################################################

# Functions to read data from local folders

local_data_path = "local-data/Data/"

def get_local_resources():
    assets = {}
    files = os.listdir(local_data_path)
    for f in files:
        if '.json' in f:
            with open(local_data_path+f, 'r') as fp:
                data = json.load(fp)
            assets[f[:-len('.json')]] = data
        elif '.txt' in f:
            with open(local_data_path+f, 'r') as fp:
                data = fp.read().split('\n')
            assets[f[:-len('.txt')]] = data
    
    return assets['profiles'], assets['company_info_map'], assets['ignore_competences'], assets['ignore_titles'], assets['cities']

def get_local_dataframe(profile, cities):
    fp = local_data_path + profile['name'].replace(' ', '_')+".zip"
    df = pd.read_csv(fp)
    df['Jobbtitlar'] = df.Jobbtitlar.apply(lambda x: literal_eval(x.replace("\' \'", "\', \'")))
    df['Kompetenser'] = df.Kompetenser.apply(lambda x: literal_eval(x.replace("\' \'", "\', \'")))
    df['location'] = df.location.apply(lambda x: first_to_upper(x))
    #df = df.drop(["headline"], axis=1)
    df.name = profile['name']
    df.locs = [l for l in df.location.value_counts().index.values if str(l) in cities]
    df.years = list(df.year.unique())
    return df

def get_local_profiles():
    with open(local_data_path+"profiles.json", 'r') as f:
        data = json.load(f)
    return data

def update_local_profiles(profile, team, profiles):
    if check_update_need(profile, team, profiles):
        if team not in profiles.keys():
            profiles[team] = {}
        profiles[team][profile['name']] = profile
        with open(local_data_path+"profiles.json", 'w') as f:
            json.dump(profiles, f)
        return True, profiles
    else:
        return False, profiles

def get_local_processed():
    with open(local_data_path+"processed.json", 'r') as f:
        data = json.load(f)
    return data

def update_local_processed(processed, profile, z):
    if profile['name'] not in processed.keys():
        processed[profile['name']] = [z]
    else:
        processed[profile['name']].append(z)
    with open(local_data_path+"processed.json", 'w') as f:
        json.dump(processed, f)
    return processed

def get_local_company_info_map():
    with open(local_data_path+"company_info_map.json", 'r') as f:
        data = json.load(f)
    return data

def update_local_company_info_map(df):
    companies = df.company.unique()    
    company_info_map = get_local_company_info_map()
    new = [str(c) for c in companies if c not in company_info_map.keys()]
    if new == []:
        return company_info_map
    for company in tqdm(new):
        try:
            company_info_map[company] = get_industry_and_link(company)
        except Exception as e:
            company_info_map[company] = "Okänd", "Okänd", "https://www.allabolag.se/what/"+company.replace(" ", "%20")
    with open(local_data_path+"company_info_map.json", 'w') as f:
        json.dump(company_info_map, f)
    return company_info_map

##########################################################################################################

# Functions for interacting with external data sources

def get_available_datasets():
    zips = pd.read_html("https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/index.html")[0]['File Name ↓'].values
    return list({s.split("_")[0][:4]: s for s in zips}.values())

def get_af_file_data_and_filter(filename, profile):
    full_filename = "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"+filename
    with requests.get(full_filename) as response:
        with ZipFile(BytesIO(response.content), 'r') as zip_ref:
            for name in zip_ref.namelist():
                with BytesIO(zip_ref.open(name).read()) as data:
                    #data = BytesIO(zip_ref.open(name).read())
                    df = pd.DataFrame([json.loads(line) for line in data if line_checker(line, profile['keywords'])])

                    df['location'] = df.workplace_address.apply(lambda x: x['municipality'] if x['municipality'] is not None else (x['city'] if 'city' in x.keys() else None))
                    df['location'] = df['location'].str.replace('[^a-zA-Zåäö]', '').str.lower()
                    try:
                        df['company'] = df.keywords.apply(lambda x: first_to_upper(x['extracted']['employer'][0]))
                    except:
                        df['company'] = df.employer.apply(lambda x: first_to_upper(x['name']))

                    df['description'] = df.description.apply(lambda x: x['text'])
                    df['month'] = df['publication_date'].apply(lambda x: x[5:7])
                    df['year'] = df['publication_date'].apply(lambda x: x[:4]).astype(int)
                    df['quarter'] = df.month.apply(lambda x: int(int(x) / 4)+1)
                    break

    return df[['headline', 'location', 'company', 'description', 'month', 'year', 'quarter', 'id']]#, 'detected_language'

def get_title_and_comp(df, lim=0.7):
    job_texts = list(df.apply(lambda x: {'doc_headline': x['headline'], 'doc_text': x['description']}, axis=1).values)
    job_texts = [j if j["doc_text"] != None else {'doc_headline': j['doc_headline'], 'doc_text': ""} for j in job_texts]

    url = "https://jobad-enrichments-api.jobtechdev.se/enrichtextdocuments"
    headers = {'accept': 'application/json',}
    json_data = {'documents_input': [],
                 'include_terms_info': True,
                 'include_sentences': False,
                 'sort_by_prediction_score': 'DESC',}
    responses = []
    for i in tqdm(range(0, int(len(job_texts)), 100)):
        json_data['documents_input'] = job_texts[i:i+100]
        response = requests.post(url, headers=headers, json=json_data)
        responses = responses + response.json()
        
    headlines = [r['doc_headline'] for r in responses]
    occs = [np.unique([o['concept_label'] for o in r['enriched_candidates']['occupations'] if o['prediction'] >= lim]) for r in responses]
    comps = [np.unique([o['concept_label'] for o in r['enriched_candidates']['competencies'] if o['prediction'] >= lim]) for r in responses]
    res_df = pd.DataFrame([headlines,occs, comps]).T
    res_df.columns = ["Rubrik", "Jobbtitlar", "Kompetenser"]
    return res_df

def get_industry_and_link(company):
    if type(company) != str:
        return "Okänd", "Okänd", "Okänd"
    comp = company.replace(" ", "%20")

    url = "https://www.allabolag.se/what/"+comp

    session = HTMLSession()
    r = session.get(url)

    soup = bs.BeautifulSoup(r.text, 'html.parser')

    div = soup.find_all("search")
    try:
        h_ind = json.loads(div[0][':search-result-default'])[0]['abv_hgrupp']
        u_ind = json.loads(div[0][':search-result-default'])[0]['abv_ugrupp']            
        orgnr = json.loads(div[0][':search-result-default'])[0]['orgnr'].replace("-", "").lower()
        name = json.loads(div[0][':search-result-default'])[0]['jurnamn'].replace(" ", "-").lower() 

        link = "https://www.allabolag.se/"+orgnr+"/"+name
    except:
        h_ind, u_ind, link = "Okänd", "Okänd", url
    
    return h_ind, u_ind, link

##########################################################################################################

# Functions for running updates of data
    
def get_profiles_to_run(file, profiles, processed):
    prof_to_run = []
    for t in profiles:
        for p in profiles[t]:
            if p not in processed.keys():
                prof_to_run.append(profiles[t][p])
            else:
                if file not in processed[p]:
                    prof_to_run.append(profiles[t][p])
    return prof_to_run

def line_checker(line, kws):
    b = 0
    for kw in kws:
        if type(kw) == list:
            if sum([str.encode(w).lower() in line.lower() for w in kw]):
                b +=1                
        else:
            if str.encode(kw).lower() in line.lower():
                b += 1
    return b == len(kws)

def filter_data(data, profile):    
    df = pd.DataFrame([json.loads(line) for line in data if line_checker(line, profile['keywords'])])
    df['location'] = df.workplace_address.apply(lambda x: x['municipality'] if x['municipality'] is not None else (x['city'] if 'city' in x.keys() else None))
    df['location'] = df['location'].str.replace('[^a-zA-Zåäö]', '').str.lower()
    try:
        df['company'] = df.keywords.apply(lambda x: first_to_upper(x['extracted']['employer'][0]))
    except:
        df['company'] = df.employer.apply(lambda x: first_to_upper(x['name']))

    df['description'] = df.description.apply(lambda x: x['text'])
    df['month'] = df['publication_date'].apply(lambda x: x[5:7])
    df['year'] = df['publication_date'].apply(lambda x: x[:4]).astype(int)
    df['quarter'] = df.month.apply(lambda x: int(int(x) / 4)+1)

    return df[['headline', 'location', 'company', 'description', 'month', 'year', 'quarter', 'id']]#, 'detected_language'

def run_profile(p, z, years):
    try:
        filtered = get_af_file_data_and_filter(z, p)
        filtered.reset_index(inplace=True, drop=True)

        title_comps = get_title_and_comp(filtered)

        filtered_with_t_c = pd.concat([filtered, title_comps], axis=1)
        # Update company_info_map.json
        update_local_company_info_map(filtered_with_t_c)

        fp = p['name'].replace(' ', '_')
        if fp + ".zip" in os.listdir(local_data_path):
            final = pd.concat([pd.read_csv(local_data_path + fp + ".zip"), filtered_with_t_c], axis=0, ignore_index=True)
            final.drop_duplicates(subset=['id'], inplace=True)

            compression_options = dict(method='zip', archive_name=fp + ".csv")
            final[final.year.isin(years)].to_csv(local_data_path+fp+".zip", compression=compression_options, index=False)
        else:
            compression_options = dict(method='zip', archive_name=fp + ".csv")
            filtered_with_t_c.to_csv(local_data_path+fp+".zip", compression=compression_options, index=False)
        return True
    except Exception as e:
        print("Error:", e, ", ignoring profile", p, "for file", z)
        print("If memory error, try to run the script again and it should work better that time.")
        return False

def run_local_update(profiles, processed):
    last_years = 5
    available_zips = get_available_datasets()[-last_years:]
    years = [int(z[:4]) for z in available_zips]
    failed = []
    for z in available_zips:    
        prof_to_run = get_profiles_to_run(z, profiles, processed)
        if prof_to_run == []:
            print("No profile needs to read from zip-file:", z)
            continue
        print("Getting data for file", z, "and profiles:")
        for p in prof_to_run:
            print(p)
        print()
        
        for p in prof_to_run:
            print("Current profile:")
            print(p)
            completed = run_profile(p, z, years)
            # Update processed.json
            if completed:
                processed = update_local_processed(processed, p, z)
            else:
                failed.append((p, z))
    return failed

##########################################################################################################

# Update datasets if possible/needed
if __name__ == "__main__":
    profiles, processed =  get_local_profiles(), get_local_processed()
    failed = run_local_update(profiles, processed)
    if failed != []:
        print("Profiles/years that failed")
        for f in failed:
            print(p['name'], z[:4])
    
##########################################################################################################

# För att printa sök-query:
#query = " AND ".join([utils.first_to_upper(kw) if isinstance(kw, str) else "("+str(' OR '.join([utils.first_to_upper(k) for k in kw]))+")" for kw in kws])

#profiles =  get_local_profiles()

#new_profiles = [{'name': 'Embedded HW', 'keywords': [['Embedded HW', 'Embedded Hardware']]}]
#team = "Deep Purple"

#for p in new_profiles:
#    print(p)
#    s, profiles = update_local_profiles(p, team, profiles)
#    if not s:
#        print("Failed for profile:", p)
