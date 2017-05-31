#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import re

users = []
with open('users_balanced.txt', 'r') as fh:
  users = set([int(re.findall('\d+', a.strip())[0]) for a in fh])		# Users IDs are INT64, be careful when comparing!


list_of_gzip_files = []

for root, dirs, files in os.walk('./exthdd/kribrum/twitter_1/not_single_tweets'):
    for name in files:
        if name.endswith('.gz'):
            list_of_gzip_files.append(root + '/' + name)


n = 0
#for file_name in sorted(list_of_gzip_files):

out_file = './user_info.txt'

for file_name in reversed(sorted(list_of_gzip_files)):
    with open(out_file, 'a') as out:
        with gzip.open(file_name, mode='rb') as input_file:
            file_content = input_file.readlines()
            print('File read: ' + file_name)

            for line in file_content:
                if len(users) == 0:
                    break
                line = line.decode('utf-8')
                data_dict = json.loads(line)
                #print(repr(data_dict)[:10000])
                for k in reversed(sorted(data_dict.keys())):	# не в том месте reversed был
                    if len(users) == 0:
                        break
                    v = data_dict[k]
                    k = int(k)
                    if v['user']['id'] in users:
                        out.write(json.dumps([{v['user']['id']: v['user']}]) + '\n')
                        users.remove(v['user']['id'])
                        print(str(len(users)) + ' users left')