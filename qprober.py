# encoding=utf8

# Web Database Classification - hw2
# Advanced Database Systems
# Pedro Ferro Freitas - pff2108
# Roberto Jose de Amorim - rja2139

import urllib2
import base64
import json
import subprocess
from collections import defaultdict

bing = 'hTvGEgXTQ8lDLYr8nnHocn7n9GSwF5antgnogEhNDTc'
t_es = 0.6
t_ec = 100
site = 'yahoo.com'

# bing = argv[1]
# t_es = argv[2]
# t_ec = argv[3]
# site = argv[4]


def run_query(query):
    # Execute query
    query_url = urllib2.quote("'site:" + site + " " + query + "'")
    bing_url = 'https://api.datamarket.azure.com/Bing/SearchWeb/v1/Composite?Query='+query_url+'&$top=4&$format=json'
    account_key = bing

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
        self.sum_coverage = defaultdict(int)
        self.coverage = defaultdict(int)
        self.specificity = defaultdict(int)
        self.sum_coverage_sub = defaultdict(int)
        self.coverage_sub = defaultdict(int)
        self.specificity_sub = defaultdict(int)
        self.category = ['Root']
        self.probe_count = defaultdict(int)
        self.url_list = defaultdict(list)

    def process_root_list(self):
        winner = 'Root'
        with open(winner.lower() + '.txt') as f:
            i = 1
            for line in f:
                value = line.split(' ', 1)
                cat = value[0]
                current_query = value[1].strip()
                result_top4, result_count = run_query(current_query)
                self.probe_count[winner] += 1
                self.coverage[cat] += result_count
                self.sum_coverage[winner] += result_count
                for result in result_top4:
                    self.url_list[(winner, i)].append(result['Url'].encode('utf-8'))
                i += 1
            sub_winner = self.process_sub_list(winner)
        winner = (winner, sub_winner)
        self.category = winner

    def process_sub_list(self, parent):
        winner = []
        for key in sorted(self.coverage.keys()):
            result_coverage = self.coverage[key]
            # Normalization to calculate specificity
            result_specificity = float(result_coverage) / self.sum_coverage[parent]
            self.specificity[key] = result_specificity
            print 'Specificity for category ' + str(key) + ' is ' + str(result_specificity)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_coverage)
            if result_specificity > t_es and result_coverage > t_ec:
                self.sum_coverage_sub = defaultdict(int)
                self.coverage_sub = defaultdict(int)
                self.specificity_sub = defaultdict(int)
                with open(key.lower() + '.txt') as f:
                    i = 1
                    for line in f:
                        value = line.split(' ', 1)
                        cat = value[0]
                        current_query = value[1].strip()
                        result_top4, result_count = run_query(current_query)
                        self.probe_count[key] += 1
                        self.coverage_sub[cat] += result_count
                        self.sum_coverage_sub[key] += result_count
                        for result in result_top4:
                            self.url_list[(key, i)].append(result['Url'].encode('utf-8'))
                        i += 1
                    sub_key = self.process_final_coverage(key)
                key = (key, sub_key)
                winner.append(key)
        return winner

    def process_final_coverage(self, parent):
        winner = []
        for key in sorted(self.coverage_sub.keys()):
            result_coverage = self.coverage_sub[key]
            # Normalization to calculate specificity
            result_specificity = self.specificity[parent] * result_coverage / self.sum_coverage_sub[parent]
            self.specificity_sub[key] = result_specificity
            print 'Specificity for category ' + str(key) + ' is ' + str(result_specificity)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_coverage)
            if result_specificity > t_es and result_coverage > t_ec:
                winner.append(key)
        return winner

    def print_categories(self):
        if len(self.category[1]) > 0:
            for name1 in self.category[1]:
                if len(name1[1]) > 0:
                    for name2 in name1[1]:
                        print self.category[0] + '/' + name1[0] + '/' + name2
                else:
                    print self.category[0] + '/' + name1[0]
        else:
            print self.category[0]


class ContentSummarizer(object):

    def __init__(self):
        self.word_count = defaultdict(int)
        self.word_count_sub = defaultdict(int)
        self.url_list = defaultdict(list)
        self.url_read = defaultdict(int)
        self.categories = []
        self.probe_count = defaultdict(int)

    def load_classifier(self, classifier):
        # Load relevant information form classifier to content summarizer
        self.probe_count = classifier.probe_count
        self.categories = classifier.category
        self.url_list = classifier.url_list

    def load_file(self, filename):
        # Read stored file..
        with open(filename) as f:
            for line in f:
                value = line.strip().split('\t')
                cat = value[0]
                i = int(value[1])
                url = value[2]
                self.url_list[(cat, i)].append(url)

    def summary(self):
        for entry in self.categories[1]:
            category = entry[0]
            print '\nCreating Content Summary for: ' + category
            self.word_count_sub = defaultdict(int)
            for count in range(1, self.probe_count[category] + 1):
                listing = self.url_list[(category, count)]
                if listing:
                    print str(count) + '/' + str(self.probe_count[category])
                    for url in listing:
                        print '\tGetting page: ' + url
                        if self.url_read[url] == 0:
                            self.url_read[url] = 1
                            self.process_text(url, category == 'Root')
            output_text_3 = file(category + '-' + site + '.txt', 'w')
            for word in sorted(self.word_count_sub.keys()):
                output_text_3.write('%s#%i\n' % (word, self.word_count_sub[word]))
                output_text_3.flush()
            output_text_3.close()

        category = self.categories[0]
        print '\nCreating Content Summary for: ' + category
        for count in range(1, self.probe_count[category] + 1):
            listing = self.url_list[(category, count)]
            if listing:
                print str(count) + '/' + str(self.probe_count[category])
                for url in listing:
                    print '\tGetting page: ' + url
                    if self.url_read[url] == 0:
                        self.url_read[url] = 1
                        self.process_text(url, category == 'Root')
        output_text_2 = file('Root' + '-' + site + '.txt', 'w')
        for word in sorted(self.word_count.keys()):
            output_text_2.write('%s#%i\n' % (word, self.word_count[word]))
            output_text_2.flush()
        output_text_2.close()

    def process_text(self, url, root_flag):
        p = subprocess.Popen("lynx '" + url + "' --dump", stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()

        reading = True
        for line in output.split('\n'):
            if line == 'References':
                break
            if line:
                current = 0
                if reading:
                    while line.find('[', current) > 0:
                        current = line.find('[', current)
                        if line.find(']', current) > 0:
                            c2 = line.find(']', current)
                            line = line[:current] + line[c2 + 1:]
                        else:
                            line = line[:current]
                            reading = False
                else:
                    if line.find(']', current) > 0:
                        current = line.find(']', current)
                        line = line[current + 1:]
                        current = 0
                        reading = True
                        while line.find('[', current) > 0:
                            current = line.find('[', current)
                            if line.find(']', current) > 0:
                                c2 = line.find(']', current)
                                line = line[:current] + line[c2 + 1:]
                            else:
                                line = line[:current]
                                reading = False
                    else:
                        line = ''

                if line:
                    new_line = ''
                    for character in line:
                        if character.isalpha():
                            new_line += character.lower()
                        else:
                            new_line += ' '
                    phrase = new_line.split()
                    for word in phrase:
                        self.word_count[word] += 1
                        if root_flag is False:
                            self.word_count_sub[word] += 1


print 'Classifying for website ' + site + '\n'
db_classifier = DatabaseClassifier()
db_classifier.process_root_list()
print '\n\nClassification for ' + site + ': '
db_classifier.print_categories()

print '\n\n\nExtracting topic content summaries...'
c_summarizer = ContentSummarizer()
c_summarizer.load_classifier(db_classifier)
c_summarizer.summary()
