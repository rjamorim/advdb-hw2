# Web Database Classification - hw2
# Advanced Database Systems
# Pedro Ferro Freitas - pff2108
# Roberto Jose de Amorim - rja2139

import urllib2
import base64
import json
from collections import defaultdict


site = 'fifa.com'
t_es = 0.6
t_ec = 100
category = 'Root'


def run_query(query):
    # Execute query
    # print 'Query being executed: ' + query
    query_url = urllib2.quote("'site:" + site + " " + query + "'")
    bing_url = 'https://api.datamarket.azure.com/Bing/SearchWeb/v1/Composite?Query='+query_url+'&$top=4&$format=json'
    account_key = 'hTvGEgXTQ8lDLYr8nnHocn7n9GSwF5antgnogEhNDTc'
    # account_key = bing

    account_key_enc = base64.b64encode(account_key + ':' + account_key)
    headers = {'Authorization': 'Basic ' + account_key_enc}
    req = urllib2.Request(bing_url, headers=headers)
    response = urllib2.urlopen(req)
    content = response.read()
    # content contains the xml/json response from Bing.
    tree = json.loads(content)

    return tree['d']['results'][0]['Web'], int(tree['d']['results'][0]['WebTotal'])


class DatabaseClassifier(object):

    def __init__(self):
        self.coverage = defaultdict(int)
        self.sum_coverage = 0

    def process_root_list(self):
        # try:
        with open('root.txt') as f:
            i = 1
            for line in f:
                value = line.split(' ', 1)
                cat = value[0]
                current_query = value[1].strip()
                result_top4, result_count = run_query(current_query)
                self.coverage[cat] += result_count
                self.sum_coverage += result_count
                for result in result_top4:
                    output_text.write('root\t%i\t%s\n' % (i, result['Url']))
                    output_text.flush()
                i += 1
            self.process_sub_list()
        # except IOError:
        #    print 'List file not located. Processing will stop'
        #    exit(1)

    def process_sub_list(self):
        global category
        winner = ''
        max_coverage = 0
        for key in sorted(self.coverage.keys()):
            result_count = self.coverage[key]
            print 'Specificity for category ' + str(key) + ' is ' + str(float(result_count) / self.sum_coverage)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_count)
            if result_count > max_coverage:
                max_coverage = result_count
                winner = key
        # Normalization to calculate specificity
        specificity = float(max_coverage) / self.sum_coverage

        self.coverage = defaultdict(int)
        self.sum_coverage = 0
        if specificity > t_es and max_coverage > t_ec:
            category += '/' + winner
            # try:
            with open(winner.lower() + '.txt') as f:
                i = 1
                for line in f:
                    value = line.split(' ', 1)
                    cat = value[0]
                    current_query = value[1].strip()
                    result_top4, result_count = run_query(current_query)
                    self.coverage[cat] += result_count
                    self.sum_coverage += result_count
                    for result in result_top4:
                        output_text.write('%s\t%i\t%s\n' % (winner, i, result['Url']))
                        output_text.flush()
                    i += 1
                self.process_final_coverage(specificity)
            # except IOError:
            #    print 'List file not located. Processing will stop'
            #    exit(1)

    def process_final_coverage(self, spec_parent):
        global category
        winner = ''
        max_coverage = 0
        for key in sorted(self.coverage.keys()):
            result_count = self.coverage[key]
            print 'Specificity for category ' + str(key) + ' is ' + str(spec_parent * result_count / self.sum_coverage)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_count)
            if result_count > max_coverage:
                max_coverage = result_count
                winner = key
        # Normalization to calculate specificity
        specificity = (float(max_coverage) / self.sum_coverage) * spec_parent
        if specificity > t_es and max_coverage > t_ec:
            category += '/' + winner


class ContentSummarizer(object):

    def __init__(self):
        self.url_list = defaultdict(set)
        self.url_read = defaultdict(int)

    def read_file(self, filename):
        # Read stored file...
        with open(filename) as f:
            for line in f:
                value = line.strip().split('\t')
                cat = value[0]
                i = value[1]
                url = value[2]
                if self.url_read[url] == 0:
                    self.url_read[url] = 1
                    self.url_list[(cat, i)].add(url)


# print 'Classifying for website ' + site + '\n'
# output_text = file('FIFA.txt', 'w')
# db_classifier = DatabaseClassifier()
# db_classifier.process_root_list()
# print '\nClassification for ' + site + ': ' + category

c_summarizer = ContentSummarizer()
c_summarizer.read_file('FIFA.txt')
for category, count in sorted(c_summarizer.url_list.keys()):
    print category, count, len(c_summarizer.url_list[(category, count)]), c_summarizer.url_list[(category, count)]
