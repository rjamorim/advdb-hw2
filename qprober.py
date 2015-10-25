# Web Database Classification - hw2
# Advanced Database Systems
# Pedro Ferro Freitas - pff2108
# Roberto Jose de Amorim - rja2139

import urllib2
import base64
import json
from collections import defaultdict

site = "hardwarecentral.com"
t_es = 0.6
t_ec = 100
category = "Root"


def run_query(query):
    # Execute query
    # print "Query being executed: " + query
    query_url = urllib2.quote("'site:" + site + " " + query + "'")
    bing_url = 'https://api.datamarket.azure.com/Bing/SearchWeb/v1/Composite?Query=' + query_url + '&$top=10&$format=json'
    account_key = 'hTvGEgXTQ8lDLYr8nnHocn7n9GSwF5antgnogEhNDTc'
    # account_key = bing

    account_key_enc = base64.b64encode(account_key + ':' + account_key)
    headers = {'Authorization': 'Basic ' + account_key_enc}
    req = urllib2.Request(bing_url, headers=headers)
    response = urllib2.urlopen(req)
    content = response.read()
    # content contains the xml/json response from Bing.
    tree = json.loads(content)
    return int(tree['d']['results'][0]['WebTotal'])


def process_final_coverage(coverage, specificity_parent):
    global category
    max_coverage = 0
    sum_coverage = 0
    for key in coverage.keys():
        results = coverage[key]
        sum_coverage += coverage[key]
        if results > max_coverage:
            max_coverage = results
            winner = key
    # Normalization to calculate specificity
    specificity = (float(max_coverage) / sum_coverage) * specificity_parent
    print specificity
    print coverage
    if specificity > t_es and max_coverage > t_ec:
        category = category + "/" + winner


def process_sub_list(coverage):
    global category
    max_coverage = 0
    sum_coverage = 0
    for key in coverage.keys():
        results = coverage[key]
        sum_coverage += coverage[key]
        if results > max_coverage:
            max_coverage = results
            winner = key
    # Normalization to calculate specificity
    specificity = float(max_coverage) / sum_coverage

    coverage = defaultdict(int)
    print specificity
    if specificity > t_es and max_coverage > t_ec:
        category = category + "/" + winner
        #try:
        with open(winner.lower() + ".txt") as f:
            for line in f:
                value = line.split(' ', 1)
                coverage[value[0]] += run_query(value[1].strip())
            process_final_coverage(coverage, specificity)
        #except IOError:
        #    print "List file not located. Processing will stop"
        #    exit(1)


def process_root_list():
    coverage = defaultdict(int)
    #try:
    with open("root.txt") as f:
        for line in f:
            value = line.split(' ', 1)
            coverage[value[0]] += run_query(value[1].strip())
        print coverage
        process_sub_list(coverage)
    #except IOError:
    #    print "List file not located. Processing will stop"
    #    exit(1)


process_root_list()
print site + ": " + category
