#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import re



list_of_gzip_files = []

for root, dirs, files in os.walk('./exthdd/kribrum/twitter_1/not_single_tweets'):
    for name in files:
        if name.endswith('.gz'):
            list_of_gzip_files.append(root + '/' + name)

#print(list_of_gzip_files)

flat_dict = {}
big_dict = {}

def find_msg(m_id):
    if not m_id in flat_dict:
        return m_id
    if flat_dict[m_id] < 680000000: 
        return m_id
    else:
        return find_msg(flat_dict[m_id])
    # return m_id if flat_dict[m_id] < 680000000 else find_msg(flat_dict[m_id])

n = 0
#for file_name in sorted(list_of_gzip_files):
for file_name in reversed(sorted(list_of_gzip_files)):
    with gzip.open(file_name, mode='rb') as input_file:
        file_content = input_file.readlines()
        print('File read: ' + file_name)

        for line in file_content:
            line = line.decode('utf-8')
            data_dict = json.loads(line)
            #print(repr(data_dict)[:10000])
            for k in reversed(sorted(data_dict.keys())):	# не в том месте reversed был
                v = data_dict[k]
                k = int(k)
                m_prev = v['in_reply_to_status_id']	# номер сообщения, на которое ответили
                if m_prev == None:
                  if not k in flat_dict.keys(): # Если сообщение не ответ и на него не ссылались раньше
                    continue
                  else:
                    big_dict[flat_dict[k]].append([v['id'], m_prev, v['user']['id'], v['created_at']])
                #if len(flat_dict.keys()) % 1000 == 0:
                #  print("Success! %d \t %s" % (len(flat_dict.keys()), ""))
                orig_msg_id = find_msg(m_prev)		# номер исходного треда в big_dict
                #if not orig_msg_id == m_prev:
                #  print("Ura! %d \t %d" % (orig_msg_id, m_prev))
                t_id = 1
                if k in flat_dict.keys():
                    t_id = flat_dict[k]
                    flat_dict[m_prev] = t_id
                else:
                    n += 1
                    t_id = n
                    flat_dict[m_prev] = n
                flat_dict[k] = m_prev
                big_dict[n] = [[v['id'], m_prev, v['user']['id'], v['created_at']]]

    print(list(flat_dict.keys())[:15])
    print("Total messages referenced: %d" % len(flat_dict))
    #print(repr(big_dict)[:10000])
    #print(big_dict)
    for k, v in big_dict.items():
        if len(v) > 2:
            print(str(k) + ': ' + repr(v))
    #break