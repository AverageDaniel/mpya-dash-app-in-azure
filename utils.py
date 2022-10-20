import pandas as pd
import numpy as np
import json
from github import Github
import requests
import os
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm
import base64
import bs4 as bs
from requests_html import HTMLSession
from ast import literal_eval
import git

#from memory_profiler import profile

local = True

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

# Functions to interact with github repository

def get_gh_repo():
    if local:
        with open("../gh_access_token.txt", 'r') as f:
            access_token = f.read()
    else:
        access_token = os.environ['GITHUB_ACCESS_TOKEN']
    gh = Github(access_token, retry = 10)

    repository = gh.get_user().get_repo('mpya-dashboard-data')
    return repository

def get_gh_resources():
    repository = get_gh_repo()

    contents = repository.get_contents("Data")

    dataframes = {}
    assets = {}
    for i in range(len(contents)):
        content_file = contents[i]
        if '.json' in content_file.path:
            try:
                assets[content_file.path[len("Data/"):-len('.json')]] = requests.get(content_file.download_url).json()
            except:
                contents = repository.get_contents("Data")
                content_file = contents[i]
                assets[content_file.path[len("Data/"):-len('.json')]] = requests.get(content_file.download_url).json()
        elif '.txt' in content_file.path:
            try:
                assets[content_file.path[len("Data/"):-len('.txt')]] = requests.get(content_file.download_url).text.split('\n')
            except:
                contents = repository.get_contents("Data")
                content_file = contents[i]
                assets[content_file.path[len("Data/"):-len('.txt')]] = requests.get(content_file.download_url).text.split('\n')
                
    return assets['profiles'], assets['company_info_map'], assets['ignore_competences'], assets['ignore_titles'], assets['cities']

def get_gh_dataframe(profile, cities):
    repository = get_gh_repo()
    content = repository.get_contents("Data/"+profile['name'].replace(' ', '_')+".zip")
    if content == []:
        df = pd.DataFrame([])
        df.locs = []
        df.years = []
        return df
    response = requests.get(content.download_url).content
    with ZipFile(BytesIO(response), mode='r') as zip_ref:
        for name in zip_ref.namelist():
            df = pd.read_csv(BytesIO(zip_ref.open(name).read()))
            df['Jobbtitlar'] = df.Jobbtitlar.apply(lambda x: literal_eval(x.replace("\' \'", "\', \'")))
            df['Kompetenser'] = df.Kompetenser.apply(lambda x: literal_eval(x.replace("\' \'", "\', \'")))
            df['location'] = df.location.apply(lambda x: first_to_upper(x))
            df.name = profile['name']
    df.locs = [l for l in df.location.value_counts().index.values if str(l) in cities]
    df.years = list(df.year.unique())
    return df

def get_profiles():
    repository = get_gh_repo()
    content = repository.get_contents("Data/profiles.json")
    profiles = requests.get(content.download_url).json()
    return profiles

def update_profiles(profile, team, profiles):
    if check_update_need(profile, team, profiles):
        if team not in profiles.keys():
            profiles[team] = {}
        profiles[team][profile['name']] = profile
        repository = get_gh_repo()
        content = repository.get_contents("Data/profiles.json")
        commit_message = "Profile "+str(profile['name'])+" added"
        repository.update_file(content.path, commit_message, json.dumps(profiles), content.sha)
        return True, profiles
    else:
        return False, profiles

def get_processed():
    repository = get_gh_repo()
    content = repository.get_contents("Data/processed.json")
    processed = requests.get(content.download_url).json()
    return processed

def update_processed(processed, profile, z):
    if profile['name'] not in processed.keys():
        processed[profile['name']] = [z]
    else:
        processed[profile['name']].append(z)
    repository = get_gh_repo()
    content = repository.get_contents("Data/processed.json")
    commit_message = "File "+str(z)+" processed for profile "+str(profile['name'])
    repository.update_file(content.path, commit_message, json.dumps(processed), content.sha)
    return processed    

def get_company_info_map():
    repository = get_gh_repo()
    content = repository.get_contents("Data/company_info_map.json")
    company_info_map = requests.get(content.download_url).json()
    return company_info_map

def update_company_info_map(df):        
    companies = df.company.unique()    
    company_info_map = get_company_info_map()
    new = [str(c) for c in companies if c not in company_info_map.keys()]
    if new == []:
        return company_info_map
    for company in tqdm(new):
        try:
            company_info_map[company] = get_industry_and_link(company)
        except Exception as e:
            company_info_map[company] = "Okänd", "Okänd", "https://www.allabolag.se/what/"+company.replace(" ", "%20")
    repository = get_gh_repo()
    content = repository.get_contents("Data/company_info_map.json")
    commit_message = "Updated with "+str(len(new))+" new companies"
    repository.update_file(content.path, commit_message, json.dumps(company_info_map), content.sha)            
    return company_info_map

def pull_gh_resources():
    '''
    
    Pull everything from the data-repo and put "temporarily" put it in the heroku filesystem.
    
    '''
    return

#def pull_repo():
#    git.Repo('mpya-dashboard-data').remotes.origin.pull()

##########################################################################################################

# Functions to read data from local version of data repository


local_data_path = "local-data/Data/"
local_repo_path = "mpya-dashboard-data/Data/"

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
    #fp = local_data_path + profile['name'].replace(' ', '_') + ".csv"
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
    return pd.read_html("https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/index.html")[0]['File Name ↓'].values

def get_af_file_data(filename):
    full_filename = "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"+filename
    response = requests.get(full_filename)
    with ZipFile(BytesIO(response.content), 'r') as zip_ref:
        for name in zip_ref.namelist():
            d = BytesIO(zip_ref.open(name).read())   
    return d

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

def line_checker(line, kws, AND):
    b = 0
    for kw in kws:
        if type(kw) == list:
            if sum([str.encode(w).lower() in line.lower() for w in kw]):
                b +=1                
        else:
            if str.encode(kw).lower() in line.lower():
                b += 1
    if AND:
        return b == len(kws)
    else:
        return b > 0

def filter_data(data, profile):
    #f = pd.DataFrame([json.loads(line) for line in data if sum([str.encode(kw).lower() in line.lower() for kw in profile['keywords']])])
    
    df = pd.DataFrame([json.loads(line) for line in data if line_checker(line, profile['keywords'], True)])
    #print(df.columns)
    df['location'] = df.workplace_address.apply(lambda x: x['municipality'] if x['municipality'] is not None else (x['city'] if 'city' in x.keys() else None))
    df['location'] = df['location'].str.replace('[^a-zA-Zåäö]', '').str.lower()
    try:
        df['company'] = df.keywords.apply(lambda x: first_to_upper(x['extracted']['employer'][0]))
    except:
        df['company'] = df.employer.apply(lambda x: first_to_upper(x['name']))

    df['description'] = df.description.apply(lambda x: x['text'])
    df['month'] = df['publication_date'].apply(lambda x: x[5:7])
    df['year'] = df['publication_date'].apply(lambda x: x[:4])
    df['quarter'] = df.month.apply(lambda x: int(int(x) / 4)+1)

    return df[['headline', 'location', 'company', 'description', 'month', 'year', 'quarter']]#, 'detected_language'

def run_local_update(profiles, processed):
    available_zips = get_available_datasets()
    for z in available_zips:
        #if "2016" in z:
        #    continue
        prof_to_run = get_profiles_to_run(z, profiles, processed)
        if prof_to_run == []:
            print("No profile needs to read from zip-file:", z)
            continue
        print("Getting data for file", z, "and profiles:")#, prof_to_run)
        for p in prof_to_run:
            print(p)
        print()
        #data = get_af_file_data(z)
        
        for p in prof_to_run:
            print(p)
            #print(data)
            data = get_af_file_data(z)
            filtered = filter_data(data, p)

            title_comps = get_title_and_comp(filtered)

            final = pd.concat([filtered.reset_index(drop=True), title_comps], axis=1)
            # Update company_info_map.json
            update_local_company_info_map(final)
            
            fp = p['name'].replace(' ', '_') + ".csv"
            if fp in os.listdir(local_data_path):
                prev_df = pd.read_csv(local_data_path + fp)
                new_df = pd.concat([prev_df, final], axis=0, ignore_index=True)
                
                new_df.to_csv(local_data_path + fp, index=False)
            else:
                final.to_csv(local_data_path + fp, index=False)
                
            # Update processed.json
            processed = update_local_processed(processed, p, z)
        
    return
    
def run_update(profiles, processed):
    repository = get_gh_repo()
    
    available_zips = get_available_datasets()
    for z in available_zips:
        prof_to_run = get_profiles_to_run(z, profiles, processed)
        if prof_to_run == []:
            print("No profile needs to read from zip-file:", z)
            continue
        print("Getting data for file", z, "and profiles:", prof_to_run)
        data = get_af_file_data(z)
        
        for p in prof_to_run:
            print(p)
            filtered = filter_data(data, p)

            title_comps = get_title_and_comp(filtered)

            final = pd.concat([filtered.reset_index(drop=True), title_comps], axis=1)
            # Update company_info_map.json
            update_company_info_map(final)
            
            contents = repository.get_contents("Data")
            if "Data/"+p['name'].replace(' ', '_')+".zip" in [c.path for c in contents]:
                content = repository.get_contents("Data/"+p['name'].replace(' ', '_')+".zip")
                response = requests.get(content.download_url).content
                with ZipFile(BytesIO(response), mode='r') as zip_ref:
                    for name in zip_ref.namelist():
                        prev_df = pd.read_csv(BytesIO(zip_ref.open(name).read()))
                    
                new_df = pd.concat([prev_df, final], axis=0, ignore_index=True)
                
                byte_stream = BytesIO()
                with ZipFile(byte_stream, mode='w') as zf:
                    zf.writestr(p['name'].replace(' ', '_')+".csv", new_df.to_csv(index=False).encode('utf-8'))
                print(byte_stream.getbuffer().nbytes)
                repository.update_file(content.path, "Data from file "+str(z)+" added", byte_stream.getvalue(), content.sha)
            else:
                byte_stream = BytesIO()
                with ZipFile(byte_stream, mode='w') as zf:
                    zf.writestr(p['name'].replace(' ', '_')+".csv", final.to_csv(index=False).encode('utf-8'))
                repository.create_file("Data/"+p['name'].replace(' ', '_')+".zip", "Data from file "+str(z)+" added", byte_stream.getvalue())
            
            #ToDo:
            # Update processed.json
            processed = update_processed(processed, p, z)
    return

##########################################################################################################

#profiles, processed =  get_local_profiles(), get_local_processed()

#run_update(profiles, processed)

#, "C" , ["Quality", "Kvalitet"]

#[["Electronics engineer", "Elektronikingenjör"], "Radar", "Power electronics", "Battery", ["Embedded software", "Embedded SW"], ["Embedded HW, Embedded Hardware"], ["HW", "Hardware"], "PCB", "C++", "Python", "Test developer", ["Quality", "Kvalitet"], "Quality Medtech", "Quality Electro" , "Quality Automotive", ["Project leader", "Projektledare"]]


#d_p_list = [["Electronics engineer", "Elektronikingenjör"], "Radar", "Power electronics", "Battery", ["Embedded software", "Embedded SW"], ["Embedded HW, Embedded Hardware"], ["HW", "Hardware"], "PCB", "C++", "Python", "Test developer", "Quality Medtech", "Quality Electro" , "Quality Automotive", ["Project leader", "Projektledare"]]

#new_profiles = [{"name": n[0], "keywords": n} if type(n) == list else {"name": n, "keywords": [n]} for n in d_p_list]

#for p in new_profiles:
#    print(p)
#    s, profiles = update_local_profiles(p, "Deep Purple", profiles)
#    if not s:
#        print("Failed for profile:", p)
        
#print(profiles)
#for t in profiles:
#    print()
#    print(t)
#    #print(profiles[t])
#    for p in profiles[t]:
#        print(profiles[t][p])

#run_local_update(profiles, processed)


'''

#############################################
Hantera AND för dessa:

"Quality": {"name": "Quality", "keywords": ["Quality", "Kvalitet"]}, "Quality Medtech": {"name": "Quality Medtech", "keywords": ["Quality Medtech"]}, "Quality Electro": {"name": "Quality Electro", "keywords": ["Quality Electro"]}, "Quality Automotive": {"name": "Quality Automotive", "keywords": ["Quality Automotive"]},

# Fixa minnesproblem för Project lead

# Fixa de sista DP-profilerna/kompetenserna

{"name": "Quality Life Science/Medtech", "keywords": [["Quality", "Kvalitet", "QA", "Quality Assurance", "GMP", "Good Manufacturing Practice"], ["Medtech", "Life Science"]]}


# Fixa Sthlms profiler och kompetenser


############################################

'''