#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import gzip
import json

def filter_hour_file(hour_file_name, mentioned_statuses=set(), users_who_reply=set()):
    print('\nFile:\t\t\t\t\t' + hour_file_name)
    # collection ids of previously mentioned statuses is needed to grab the first tweets in threads
    with gzip.open(hour_file_name, mode='rb') as input_file:
        file_content = input_file.readlines()
        print('File length:\t\t\t\t' + str(len(file_content)))

        not_single_tweets = {}
        tweets_by_repliers = {}

        entries_count = 0
        replies_count = 0

        for line in reversed(file_content): # reversed order needed to collect the first tweets in threads
            line = line.decode('utf-8')
            if line[0] == '{':
                entries_count += 1
                data_dict = json.loads(line)
                if data_dict['in_reply_to_status_id'] != None:
                    replies_count += 1
                    not_single_tweets[data_dict['id']] = data_dict
                    mentioned_statuses.update([data_dict['in_reply_to_status_id']])
                    users_who_reply.update([data_dict['user']['id']])
                elif data_dict['id'] in mentioned_statuses:
                    not_single_tweets[data_dict['id']] = data_dict
                elif data_dict['user']['id'] in users_who_reply:
                    tweets_by_repliers[data_dict['id']] = data_dict

    print('Real entries:\t\t\t\t' + str(entries_count))
    print('Tweets in threads:\t\t\t' + str(len(not_single_tweets)))
    print('Tweets by repliers not in thread:\t' + str(len(tweets_by_repliers))) # tweets which are not in any thread in given hour
    print('First tweets in threads:\t\t' + str(len(not_single_tweets) - replies_count)) # first tweets in threads
    return mentioned_statuses, users_who_reply, not_single_tweets, tweets_by_repliers

# filter_hour_file('../data/20.hour.gz')

list_of_original_gzip_files = []
for root, dirs, files in os.walk('./'):
    for name in files:
        if name.endswith('.gz'):
            list_of_original_gzip_files.append(root + '/' + name)

with open('list_of_files.txt', 'w') as f:
    for n in list_of_original_gzip_files:
        f.write(n + '\n')

mentioned_statuses=set()
users_who_reply=set()
not_single_tweets = {}
tweets_by_repliers = {}

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    import unicodedata
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    value = unicode(re.sub('[-\s]+', '-', value))
    return value


for file_name in reversed(sorted(list_of_original_gzip_files)):
    if len(not_single_tweets) > 250000:
        with gzip.open('./not_single_tweets/' + slugify(file_name) + '.gz', 'wb') as f:
            f.write(json.dumps(not_single_tweets))
        not_single_tweets = {}
    if len(tweets_by_repliers) > 250000:
        with gzip.open('./tweets_by_repliers/' + slugify(file_name) + '.gz', 'wb') as f:
            f.write(json.dumps(tweets_by_repliers))
        tweets_by_repliers = {}
    mentioned_statuses, users_who_reply, not_single_tweets_hour, tweets_by_repliers_hour = filter_hour_file(file_name, mentioned_statuses, users_who_reply)
    not_single_tweets = {**not_single_tweets, **not_single_tweets_hour}
    tweets_by_repliers = {**tweets_by_repliers, **tweets_by_repliers_hour}

    print('TWEETS BY REPLIERS NOT IN THREAD:\t' + str(len(tweets_by_repliers))) # tweets which are not in any thread in given hour
    print('FIRST TWEETS IN THREADS:\t\t' + str(len(not_single_tweets))) # first tweets in threads

with gzip.open('../not_single_tweets/first.gz', 'wt') as f:
    f.write(json.dumps(not_single_tweets))
with gzip.open('../tweets_by_repliers/first.gz', 'wt') as f:
    f.write(json.dumps(tweets_by_repliers))