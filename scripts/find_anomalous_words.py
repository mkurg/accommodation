import numpy as np
# @title 

dict_file = 'words.txt'
users_file = 'users.txt'

users = [u for l in open(users_file).readlines()]
words = [u for l in open(words_file).readlines()]

def count_words_general(users, words):
  wu = np.zeros((len(users), len(words)))        # Matrix all word counts by users
  wu_self = np.zeros((len(users), len(words)))   # Matrix words by users for tweets which are NOT a response to anyone

  # Boilerplate for iterating over all files
  for t in tweets:
    if not t.user_id in users:
      continue
    uid = users.index(t.user_id)
    for word in t.text.split(' '):
      # Incrementing word-user matrix
      wu[uid][word_id]
    if t.in_reply_to != None:
      # Adding to output file to scan again for the thread and build count
  return wu, wu_self
 
def get_conversations(userlist, out_folder = './threads'):
  # calling get_threads.py with restriction on user_ids when outputting threads?
  # Saving threads/USER_ID.txt in JSON format, a record for conversation
  pass

def count_conversation_word_mention(userid, wordlist):
  pass

if __name__ == '__main__':
  wu, wu_self = count_words_general(users, words)     # строим словарь частотности маркерных слов для всех пользователей
  get_conversations(users)  # вынимаем все переписки указанных пользователей
  # Дальше либо считаем все упоминания слова в переписках и вычитаем из общего количества, либо берём wu_self (упоминания слова вне переписок) и делим на упоминание в переписках
  
    