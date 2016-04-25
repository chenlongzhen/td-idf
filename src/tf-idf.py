# -*- coding: utf-8 -*-
'''
@author: BF-algo,longzhen.chen
'''
import sys
#sys.path.append('../')
from math import log
import re
import jieba
import jieba.analyse
from optparse import OptionParser

ROOT_PATH = sys.path[0]
DATA_PATH = ROOT_PATH + "/../data/"

#USAGE = "usage:    python extract_tags_idfpath.py [file name] -k [top k]"
#jieba.load_userdict(DATA_PATH + 'dic_nospace_tag') 
#parser = OptionParser(USAGE)
#parser.add_option("-k", dest="topK")
#opt, args = parser.parse_args()


#if len(args) < 1:
#    print(USAGE)
#   sys.exit(1)

#file_name = args[0]

if opt.topK is None:
    topK = 240
else:
    topK = int(opt.topK)
class IrIndex:
    """An in-memory inverted index"""
    
    pattern = re.compile("^\s+|\s*,*\s*|\s+$")
    
    def __init__(self):
        self.index = {}
        self.documents_num = 0 
        self.tf = {}
    

    def index_document(self, document):
        ## split
        terms = [word for word in self.pattern.split(document)]
        
        ## add to documents
        self.documents_num += 1
        document_pos = self.documents_num 
        ## add posts to index, updating tf
        for term in terms:
            if term not in self.index:
                self.index[term] = []
                self.tf[term] = []
            self.index[term].append(document_pos) # word: document_postion 
            self.tf[term].append(terms.count(term)/float(len(terms))) # word : frequency
        
    
    def tf_idf(self, term):#wrong
        ## get tf for each document and calculate idf
        if term in self.tf:
            res = []
            for tf, post in zip(self.tf[term], self.index[term]):
                idf = log( float( self.documents_num ) / float( len(self.tf[term]) ) )
                #res.append((tf*idf, self.documents[post]))
                res.append(tf*idf)
            return max(res)
        else:
            return ['no']

def main():
    
    index = IrIndex()
    chunk = 100
    #content = open('user_corpus.txt', 'rb').read()
    #jieba.analyse.set_stop_words("./stop_words.txt")
    #jieba.analyse.set_idf_path("./weibo_tfidf_1029.txt")
    file_path = DATA_PATH + "id_post0_sample.txt" 
    id = []
    with open(file_path,"r") as rf:
        for n,line enumerate(rf):
            # print 
            if n % chunk == 0        
                print "{} line data have processed".format(n)
            tokens  = line.strip().split(" ")
            # format :id content, if not continue
            if not len(tokens) == 2:
                continue
            # get id and content
            id.append(tokens[0])
            content = tokens[1]
            # count tf and idf
            index.index_document(content)
            
            
            
            


