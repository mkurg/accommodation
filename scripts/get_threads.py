#!/usr/bin/python3
# -*- coding: utf-8 -*-

from dateutil import parser
import os
import gzip
import json
import re

users = []
with open('users_balanced.txt', 'r') as fh:
  users = set([int(re.findall('\d+', a.strip())[0]) for a in fh][:50])
with open('user_threads.txt', 'r') as fh:
  users.update(set([int(re.findall('\d+', a.strip())[0]) for a in fh][:50]))
users = list(users)
print(len(users))
# Users IDs are INT64, be careful when comparing!

handlers = [open(str(a)+"_threads.txt", 'w') for a in users]
h_tsv = [open("tsv_"+str(a)+"_threads.txt", 'w') for a in users]

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

out_file = './threads.txt'

def write_threads_tsv(fh, thr):
    out = "" #json.dumps([{'id': tweet[0], 'uid': tweet[2], 'reply_to': tweet[1], 'text': tweet[4], 'created_at': tweet[5]}  for tweet in thr]) + "\n")
    for i in reversed(range(1, len(thr))):
      prevmsg = thr[i-1]
      msg = thr[i]
      out += "%s\t%s\t%s\t%s\t%s\t%s\n" % (str(prevmsg[0]), str(prevmsg[2]), str(prevmsg[4]), str(msg[0]), str(msg[2]), str(msg[4]))
    fh.write(out)

for file_name in reversed(sorted(list_of_gzip_files)):
    with open(out_file, 'a') as out:
        with gzip.open(file_name, mode='rb') as input_file:
            file_content = input_file.readlines()
            print('File read: ' + file_name)

            for line in file_content:
                line = line.decode('utf-8')
                data_dict = json.loads(line)
                #print(repr(data_dict)[:10000])
                n = 0
                for k in reversed(sorted(data_dict.keys())):	# не в том месте reversed был
                    n += 1
                    v = data_dict[k]
                    k = int(k)
                    m_prev = v['in_reply_to_status_id'] 	# номер сообщения, на которое ответили
                    m_prev = int(m_prev) if not m_prev == None else None
                    if m_prev == None:
                        if not k in flat_dict.keys(): # Если сообщение не ответ и на него не ссылались раньше
                            pass
                        else:
                            try:
                              big_dict[flat_dict[k]].append([v['id'], m_prev, v['user']['id'], v['created_at'], v['text'], v['created_at']])
                            except:
                              print("Gluing message %s to thread %d failed"% (v['id'], flat_dict[k]))
                        # Saving thread to file
                        # TODO: check if at least 1 user in users
                        if not k in flat_dict.keys():
                            print("Tweet %s not found while should be" % str(k))
                            continue
                        t_id = flat_dict[k]
                        if not t_id in big_dict.keys():
                            print("Tweet %s not found while should be in big_dict, FIXING" % str(k))
                            big_dict[t_id] = []
                            big_dict[t_id].append([v['id'], m_prev, v['user']['id'], v['created_at'], v['text'], v['created_at']])
                            #continue
                        u_thread = [u[2] for u in big_dict[t_id]]
                        for u in set(users) & set(u_thread):
                            if not t_id in big_dict.keys():
                                print("Tweet %s not found while should be" % str(k))
                                continue
                            # Saving threads for each user id
                            d = big_dict[t_id]
                            handlers[users.index(u)].write(json.dumps([{'id': tweet[0], 'uid': tweet[2], 'reply_to': tweet[1], 'text': tweet[4]} for tweet in d]) + "\n")
                            write_threads_tsv(h_tsv[users.index(u)], d)
                            # CLeaning big_dict and flat_fict
#                            for msg in d:
#                                del flat_dict[int(msg[0])]
                                #flat_dict.remove(int(msg[0]))
#                            del big_dict[t_id]
                        try:
                            out.write(json.dumps(big_dict[flat_dict[k]]))
                            del big_dict[flat_dict[k]]
                            del flat_dict[k]
                        except KeyError:
                            print(str(k) + ' not found')
                    if n % 10000 == 0:
                        ts = parser.parse(v['created_at'])
                        for t_id, thr in big_dict.items():
                            tm = ts
                            for msg in thr:
                                t1 = parser.parse(msg[5])
                                if (t1 - ts).days > (tm - ts).days:
                                  tm = t1
                            if (tm - ts).days > 7:
                                print("Dumping old thread ")
                                handlers[users.index(u)].write(json.dumps([{'id': tweet[0], 'uid': tweet[2], 'reply_to': tweet[1], 'text': tweet[4], 'created_at': tweet[5]}  for tweet in thr]) + "\n")
                                write_threads_tsv(h_tsv[users.index(u)], thr)
#                                for msg in thr:
#                                    flat_dict.remove(int(msg[0]))
#                                    del flat_dict[int(msg[0])]
#                                del d[t_id]
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
                        big_dict[t_id] = []
                    try:
                        big_dict[t_id].append([v['id'], m_prev, v['user']['id'], v['created_at'], v['text'], v['created_at']])
                    except:
                        print('Error adding %s: %s at %s' % (str(v['id']), v['text'], v['created_at']))
                    #if k in flat_dict.keys():
                    #  print('ura! %d from thread %d' % (k, t_id))
                    flat_dict[k] = m_prev

        #print(list(flat_dict.keys())[:15])
        print("Total messages referenced: %d" % len(flat_dict))
        #print(repr(big_dict)[:10000])
        #print(big_dict)
    #    for k, v in big_dict.items():
    #        if len(v) > 2:
    #            print(str(k) + ': ' + repr(v))
        # for value in list(big_dict.values()):
        #     if len(value) > 2:
        #         print(repr(value))
        #break

for h in handlers + h_tsv:
    h.close()