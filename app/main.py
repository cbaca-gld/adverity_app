import os
import requests
import json
from flask import Flask
from flask import request, render_template, redirect, url_for
from .config import BASE_URL

app = Flask(__name__)
url = BASE_URL

def make_request(url):
    ### FUNCTION GOAL: MAKE THE URL REQUEST
    ### RETURN A CLEAN RESPONSE JSON

    from .config import READ_TOKEN
    headers = {'Authorization': READ_TOKEN}

    response = requests.request('GET', url, headers=headers)
    clean_response = json.loads(response.text)

    return clean_response

def check_pagination(clean_res):
    ### FUNCTION GOAL: TAKE IN THE REQUEST'S CLEAN RESPONSE
    ### RETURN A BOOLEAN

    if clean_res['next']:
        return True
    else:
        return False

def parse_job_data(job_data):
    ### FUNCTION GOAL: TAKE IN MAIN RESPONSE AND
    ### RETURN A LIST OF ONLY ACTIVE FETCH JOBS FROM CURRENT RESPONSE

    job_list = []
    for job in job_data["results"]:
        active_job = {}
        if job["type"] == "Fetch":
            active_job["id"] = job["id"]
            active_job["workspace"] = job["datastream"]["stack"]["name"]
            active_job["datastream"] = job["datastream"]["name"]
            active_job["type"] = job["type"]
        else:
            active_job["type"] = job["type"]
        job_list.append(active_job)
    return job_list

def call_job_endpoint(job_url):
    ### FUNCTION GOAL: TAKE IN MAIN JOB URL AND 
    ### RETURN A list of DICTIONARY OF ACTIVE FETCH JOBS LIKE THIS:        
        # [{"id": 11111, 
        # "workspace": "Client Name", 
        # "datastream": "Datastream Name", 
        # "type": "fetch"}, 
        #{"id": 2222222, 
        # "workspace": "Client Name", 
        # "datastream": "Datastream Name", 
        # "type": "fetch"}]

    active_fetch_jobs = []
    page_number = 1

    job_data = make_request(job_url)
    
    current_request_job_list = parse_job_data(job_data)
    for job in current_request_job_list:
        active_fetch_jobs.append(job)

    paginated = check_pagination(job_data)

    while paginated:
        current_request_job_list = []
        page_number += 1
        new_page_url_string = f'?page={page_number}'
        next_url = f'{job_url}/{new_page_url_string}'
        job_data = make_request(next_url)
        current_request_job_list = parse_job_data(job_data)
        for job in current_request_job_list:
            active_fetch_jobs.append(job)
        paginated = check_pagination(job_data)

    active_fetch_jobs_dict = {}
    active_fetch_jobs_dict["Current Fetch Jobs"] = active_fetch_jobs

    with open('jobs.json', 'w') as file:
        json.dump(active_fetch_jobs_dict, file)

    return active_fetch_jobs

def count_active_fetch_jobs(active_fetch_jobs):
    ### FUNCTION GOAL: TAKE IN ACTIVE FETCH JOBS LIST AND
    ### RETURN AN INTEGER OF THE COUNT OF CURRENTLY ACTIVE FETCH JOBS

    count = 0
    for job in active_fetch_jobs:
        count += 1 

    return count

def kill_fetch_jobs(active_fetch_jobs):
    ### FUNCTION GOAL: TAKE IN ACTIVE FETCH JOBS LIST IDS AND
    ### MAKE A POST REQUEST TO THE JOB KILL URL (https://{{INSTANCE}}/api/jobs/{{JOB_ID}}/stop/)

    from .config import WRITE_TOKEN
    
    killed_jobs = []
    
    headers = {'Authorization': WRITE_TOKEN}

    for active_job in active_fetch_jobs:
        final_response = requests.request('POST', f'{url}/{active_job}/stop', headers=headers)
        final_clean_response= json.loads(final_response.text)
        killed_jobs.append(final_clean_response)

    return killed_jobs

count = 0
jobs = []

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        global count
        global jobs
        global url

        jobs = call_job_endpoint(url)
        count = count_active_fetch_jobs(jobs)

        return render_template("base.html", count=count, jobs=jobs)
    count = 0
    jobs = []
    return render_template("base.html", count=count, jobs=jobs)

@app.route("/kill-jobs")
def kill_jobs():
    with open("jobs.json", "r") as file:
        job_list = []
        job_dicts = json.load(file)
        for fetch_job in job_dicts["Current Fetch Jobs"]:
            if fetch_job["type"] == "Fetch":
                job_list.append(fetch_job["id"])
        killed_jobs = kill_fetch_jobs(job_list)
    
    return render_template("kill-jobs.html", killed_jobs=killed_jobs)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)