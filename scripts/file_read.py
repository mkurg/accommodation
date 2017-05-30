#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import re
import numpy as np
from lemmatizer import lemmatize


def get_file_list(fpath):
    list_of_gzip_files = []

    for root, dirs, files in os.walk(fpath):
        for name in files:
            if name.endswith('.gz'):
                list_of_gzip_files.append(root + '/' + name)
    return list_of_gzip_files


def process_tweets(callback, fpath='./'):
    list_of_gzip_files = get_file_list(fpath)
    for file_name in sorted(list_of_gzip_files)[:2]:
        with gzip.open(file_name, mode='rb') as input_file:
            file_content = input_file.readlines()
            print('File read: ' + file_name)
            for line in file_content:
                line = line.decode('utf-8')
                data_dict = json.loads(line)
                for k, v in data_dict.items():
                    callback(k, v)
