#!/usr/bin/env python3

import requests
from typing import List
from bs4 import BeautifulSoup
from common import parse_message_id

COMMITFEST_URL = "https://commitfest.postgresql.org"
COMMITED_STATUS_URL_ADDITION = "status=4"

INFO_FILE = "info.crawling.txt"

response = requests.get(COMMITFEST_URL)

class CommitGroup():
    def __init__(self, header:str):
        self.header = header
        self.commit_urls = []

soup = BeautifulSoup(response.text, "lxml")
commitfests = soup.find_all("li")

def get_commitfest_url(list_entry):
    if "(Closed -" in list_entry.text:
        a_tag = list_entry.find("a")
        return COMMITFEST_URL + a_tag["href"] 
    return None

def get_commitfest_commits(url, header="Bug Fixes"):
    response = requests.get(url + "?" + COMMITED_STATUS_URL_ADDITION)
    soup = BeautifulSoup(response.text, "lxml")
    table_rows = soup.find_all("tr")
    
    commit_group: CommitGroup = None
    for table_row in table_rows:
        table_header = table_row.find("th")
        if table_header:
            if commit_group is None and table_header.text == header:
                # Found the header of interest. create it and append the URLs
                commit_group = CommitGroup(table_header.text)
            elif commit_group is not None:
                # We have a new section. We can abort now.
                return commit_group
        elif commit_group:
            a_tag = table_row.find("a")
            commit_group.commit_urls.append(url + a_tag["href"])
    return commit_group


def get_commit_info(url: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    table_rows = soup.find_all("tr")
    for table_row in table_rows:
        table_header = table_row.find("th")
        if table_header and table_header.text == "Emails":
            mailing_list_a_tags = table_row.select("td > dl > dt > a")
            if len(mailing_list_a_tags) == 0:
                print(mailing_list_a_tags)
                print("Too few a tags!")
                exit(1)
            return [parse_message_id(a_tag["href"]) for a_tag in mailing_list_a_tags]
    return None

all_message_ids = []
for list_entry in commitfests:
    url = get_commitfest_url(list_entry)
    if url is None:
        continue
    print(url)
    commit_group: CommitGroup = get_commitfest_commits(url)
    if commit_group is None:
        continue
    print("-" * 30)
    for commit_url in commit_group.commit_urls:
        print(commit_url)
        message_ids = get_commit_info(commit_url)
        if message_ids:
            all_message_ids.extend(message_ids)

with open("postgresql-message-ids.txt", "w") as f:
    f.write("\n".join(all_message_ids))

