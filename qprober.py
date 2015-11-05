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
from sys import argv

# bing = 'hTvGEgXTQ8lDLYr8nnHocn7n9GSwF5antgnogEhNDTc'
# t_es = 0.6
# t_ec = 100
# site = 'hardwarecentral.com'

bing = argv[1]
t_es = float(argv[2])
t_ec = int(argv[3])
site = argv[4]

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
        # Performs Bing searches for all keywords in the root file
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
                    # This stores the top four results for each query to later extract the summaries
                    self.url_list[(winner, i)].append(result['Url'].encode('utf-8'))
                i += 1
            sub_winner = self.process_sub_list(winner)
        winner = (winner, sub_winner)
        self.category = winner

    def process_sub_list(self, parent):
        # Performs Bing searches for all keywords in the category(ies) most significant in the previous searches
        winner = []
        for key in sorted(self.coverage.keys()):
            result_coverage = self.coverage[key]
            # Normalization to calculate specificity
            result_specificity = float(result_coverage) / self.sum_coverage[parent]
            self.specificity[key] = result_specificity
            print 'Specificity for category ' + str(key) + ' is ' + str(result_specificity)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_coverage)
            if result_specificity > t_es and result_coverage > t_ec:
                # If specificity and coverage are larger than thresholds, we run the queries for that subcategory
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
                            # This stores the top four results for each query to later extract the summaries
                            self.url_list[(key, i)].append(result['Url'].encode('utf-8'))
                        i += 1
                    sub_key = self.process_final_specificity(key)
                key = (key, sub_key)
                winner.append(key)
        return winner

    def process_final_specificity(self, parent):
        # Calculates the specificity for the leaf categories
        winner = []
        for key in sorted(self.coverage_sub.keys()):
            result_coverage = self.coverage_sub[key]
            # Normalization to calculate specificity
            result_specificity = self.specificity[parent] * result_coverage / self.sum_coverage_sub[parent]
            self.specificity_sub[key] = result_specificity
            print 'Specificity for category ' + str(key) + ' is ' + str(result_specificity)
            print 'Coverage for category ' + str(key) + ' is ' + str(result_coverage)
            if result_specificity > t_es and result_coverage > t_ec:
                # If specificity and coverage are larger than thresholds, that leaf category is chosen for the database
                winner.append(key)
        return winner

    def print_categories(self):
        # Prints the detected category(ies) for each database
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

    def summary(self):
        # This first part extracts summaries for the categories
        for entry in self.categories[1]:
            category = entry[0]
            print '\nCreating Content Summary for: ' + category
            self.word_count_sub = defaultdict(int)
            for count in range(1, self.probe_count[category] + 1):
                # Get the URL list of the top 4 resulta in each probe query
                listing = self.url_list[(category, count)]
                if listing:
                    print str(count) + '/' + str(self.probe_count[category])
                    for url in listing:
                        # For each URL, extract all text from the page
                        print '\tGetting page: ' + url
                        if self.url_read[url] == 0:
                            self.process_text(url, category == 'Root')
                            self.url_read[url] = 1
            output_text_3 = file(category + '-' + site + '.txt', 'w')
            for word in sorted(self.word_count_sub.keys()):
                # Outputs the file as instructed in the assignment
                output_text_3.write('%s#%i\n' % (word, self.word_count_sub[word]))
                output_text_3.flush()
            output_text_3.close()

        # This second part extracts summaries for the root part
        category = self.categories[0]
        print '\nCreating Content Summary for: ' + category
        for count in range(1, self.probe_count[category] + 1):
            # Get the URL list of the top 4 resulta in each probe query
            listing = self.url_list[(category, count)]
            if listing:
                print str(count) + '/' + str(self.probe_count[category])
                for url in listing:
                    # For each URL, extract all text from the page
                    print '\tGetting page: ' + url
                    if self.url_read[url] == 0:
                        self.process_text(url, category == 'Root')
                        self.url_read[url] = 1
        output_text_2 = file('Root' + '-' + site + '.txt', 'w')
        for word in sorted(self.word_count.keys()):
            # Outputs the file as instructed in the assignment
            output_text_2.write('%s#%i\n' % (word, self.word_count[word]))
            output_text_2.flush()
        output_text_2.close()

    def process_text(self, url, root_flag):
        # lynx is used to extract text from HTML files, as instructed in the assignment
        p = subprocess.Popen("lynx '" + url + "' --dump", stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()

        reading = True
        for line in output.split('\n'):
            if line == 'References':
                # The References keywork means that the whole textual content has been processed already
                break
            if line:
                current = 0
                # This whole routine is meant to discard all information surrounded by brackets
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
                # Non-alphabetic characters are considered word separators
                if line:
                    new_line = ''
                    for character in line:
                        if character.isalpha():
                            new_line += character.lower()
                        else:
                            new_line += ' '
                    phrase = new_line.split()
                    for word in phrase:
                    # The routine that counts word frequencies
                        self.word_count[word] += 1
                        if root_flag is False:
                            self.word_count_sub[word] += 1

# Call the methods for database classification
print 'Classifying for website ' + site + '\n'
db_classifier = DatabaseClassifier()
db_classifier.process_root_list()
print '\n\nClassification for ' + site + ': '
db_classifier.print_categories()

# Call the methods for summary creation
print '\n\nExtracting topic content summaries...'
c_summarizer = ContentSummarizer()
c_summarizer.load_classifier(db_classifier)
c_summarizer.summary()
