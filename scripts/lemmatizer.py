#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re
import pymorphy2


def tokenize(line):
    #tokens = line.split()
    tokens = re.findall('[\w*\u0300-\u0304\u0384-]+', line, flags=re.UNICODE)
    return(tokens)

def normalize(token):
    return pymorphy2.MorphAnalyzer().parse(token)[0].normal_form

test_string = """это такая проверочка-проверка досто́йный бж҃е́ственнагѡ ца́рствїѧ
Или вот, скажем, такое предложение с местоимениями я, мы, они"""

def lemmatize(text):
    """Input: string
    Returns list of tokens"""
    
    tokens = tokenize(text)
    tokens2 = []
    for token in tokens:
        tokens2.append(normalize(token))
    return tokens2