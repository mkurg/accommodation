#!/usr/bin/python3
# -*- coding: utf-8 -*-

import signal
import sys
import time
import numpy as np
from lemmatizer import *
from file_read import *
# @title

words_file = 'tags.1.txt'
users_file = 'users.txt'

users = [int(l.strip()) for l in open(users_file).readlines()]
words = [l.strip() for l in open(words_file).readlines()]

def get_word_id(word):
    return words.index(word)

def signal_handler(signal, frame):
    np.savetxt('wu.csv', wu, delimiter=',')
    np.savetxt('wu_self.csv', wu_self, delimiter=',')
    print('Interrupt catched')
    sys.exit(0)



# print(users[:5])
wu = np.zeros((len(users), len(words)))
# Matrix words by users for tweets which are NOT a response to anyone
wu_self = np.zeros((len(users), len(words)))

def count_words_callback(k, v):
    # k - key (string, tweet #) 
    # v - dict (twitter api response)
    global wu
    global wu_self
    if v['user']['id'] in users:
        print('wu sum: %d' % wu.sum())
        print('wu_self sum: %d' % wu_self.sum())
        #print(wu)
        uid = users.index(v['user']['id'])
        for word in tokenize(v['text']):
            # Incrementing word-user matrix
            if word in words:
                if v['in_reply_to_status_id'] != None:
                    wu[uid][get_word_id(word)] += 1
                else:
                    wu_self[uid][get_word_id(word)] += 1
            # if v['in_reply_to_status_id'] != None:
            #         # Adding to output file to scan again for the thread and build count


def count_words_general(users, words):
    # Matrix all word counts by users
    #wu = np.zeros((len(users), len(words)))
    # Matrix words by users for tweets which are NOT a response to anyone
    #wu_self = np.zeros((len(users), len(words)))

    # Boilerplate for iterating over all files
    process_tweets(count_words_callback)
    return wu, wu_self


def get_conversations(userlist, out_folder='./threads'):
    # calling get_threads.py with restriction on user_ids when outputting threads?
    # Saving threads/USER_ID.txt in JSON format, a record for conversation
    pass


def count_conversation_word_mention(userid, wordlist):
    pass


if __name__ == '__main__':
    # строим словарь частотности маркерных слов для всех пользователей
    signal.signal(signal.SIGINT, signal_handler)
    wu, wu_self = count_words_general(users, words)
    np.savetxt('wu.csv', wu, delimiter=',')
    np.savetxt('wu_self.csv', wu_self, delimiter=',')
    get_conversations(users)  # вынимаем все переписки указанных пользователей
    # Дальше либо считаем все упоминания слова в переписках
    # и вычитаем из общего количества, либо берём wu_self 
    # (упоминания слова вне переписок) и делим на упоминание в переписках
