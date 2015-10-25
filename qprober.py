# Web Database Classification - hw2
# Advanced Database Systems
# Pedro Ferro Freitas - pff2108
# Roberto Jose de Amorim - rja2139

import urllib2
import base64
import json


def run_query(query):
    # Execute query
    site = "fifa.com"
    query = "premiership"
    query_url = urllib2.quote("'site:" + site + " " + query + "'")
    #bing_url = 'https://api.datamarket.azure.com/Data.ashx/Bing/SearchWeb/v1/Composite?Query=' + query_url + '&$top=10&$format=json'
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
    # return tree['d']['results']
    return tree

result = run_query('oi')
print result
