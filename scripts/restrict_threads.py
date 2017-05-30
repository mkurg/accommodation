#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import json
import re

user_tweets = {}
user_threads = {}

def process(line, out_file):
    global user_threads
    global user_tweets
#    print(line.strip())
    line = re.sub('\]\[', ']\n[', line)
#    lines = line.split('\n')
    lines = line.splitlines()
    for i in lines[:5]:
        users = set()
        dat = json.loads(i)
        for j in dat:
            users.add(j[2])
            if len(users) > 2:
                continue
            if j[2] not in user_tweets:
                user_tweets[j[2]] = 1
            else:
                user_tweets[j[2]] += 1
#            print(j)
#        print(i)
        if len(users) <= 2:
            out_file.write(i)
            for user in users:
                if user not in user_threads:
                    user_threads[user] = 1
                else:
                    user_threads[user] += 1
with open('./threads.txt', 'r') as f, open('./threads_out.txt', 'a') as out:
    for line in f:
        process(line, out)
