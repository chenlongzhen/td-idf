# -*- coding: utf-8 -*-
'''
Created on 2015年10月29日
抽取用户级的关键词及tf-idf
@author: BF-algo
'''
#计算每个keyword的TF-IDF值
import sys
#sys.path.append('../')
from math import log
import re
import jieba
import jieba.analyse
from optparse import OptionParser

ROOT_PATH = sys.path[0]
DATA_PATH = ROOT_PATH + "/../data/"

USAGE = "usage:    python extract_tags_idfpath.py [file name] -k [top k]"
jieba.load_userdict(DATA_PATH + 'dic_nospace_tag') 
parser = OptionParser(USAGE)
parser.add_option("-k", dest="topK")
opt, args = parser.parse_args()


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
        self.documents = []
        self.tf = {}
    

    def index_document(self, document):
        ## split 将document拆分成词
        terms = [word for word in self.pattern.split(document)]
        
        ## add to documents
        self.documents.append(document)
        document_pos = len(self.documents)-1
        ## add posts to index, updating tf
        for term in terms:
            if term not in self.index:
                self.index[term] = []
                self.tf[term] = []
            self.index[term].append(document_pos) # word: document_postion 
            self.tf[term].append(terms.count(term)) # word : frequency
        
    
    def tf_idf(self, term):
        ## get tf for each document and calculate idf
        if term in self.tf:
            res = []
            for tf, post in zip(self.tf[term], self.index[term]):
                idf = log( float( len(self.documents) ) / float( len(self.tf[term]) ) )
                #res.append((tf*idf, self.documents[post]))
                res.append(tf*idf)
            return max(res)
        else:
            return ['no']
        
index = IrIndex()

#content = open('user_corpus.txt', 'rb').read()
jieba.analyse.set_stop_words("./stop_words.txt")
jieba.analyse.set_idf_path("./weibo_tfidf_1029.txt")
## 加载每个word对应的id？
def load_dict(path):
    f=open(path,'rb')
    words_dict = dict()
    for line in f:
        words=line.strip().split(" ")
        word=words[1]
        id=words[0]
        words_dict[word] = int(id)
    return words_dict
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
## 输出前两个 
def load_corpus(path):
    f = open(path,'rb')
    fw=open('ir2.txt','w')
    withWeight = True
    for line in f.readlines():
        parts = line.strip().split("\t")
        user = parts[0]
        content = parts[1]
        tags = jieba.analyse.extract_tags(content,topK=topK,withWeight=True)
        #id_map=load_dict('id_word.txt')
        #fw.write(str(user)+' '+str(tags)+'\n')
        tag_map={}
        tag_map.setdefault(float)
        if withWeight is True:
            for tag in tags:
                fw.write(str(user)+' '+str(tag[0])+' '+str(tag[1])+'\n')
                #print(" tag: %s\t\t weight: %f" % (tag[0],tag[1]))
        else:
            print(",".join(tags))
    f.close()
    fw.close()
#load_corpus('user_corpus.txt')
def load_corpus2(path):
    f = open(path,'rb')
    fw=open('tfidf_post0_20160126.txt','w')
    #fw=open('test_idf.txt','w')
    withWeight = True
    dic={}
    for line in f.readlines():
        #dic.setdefault(line.strip().split('\t')[0],[]).append(line.strip().split('\t')[1])  
        id=line.strip().split('\t')[0]
        conten=line.strip().split('\t')[-1]
        if id in dic:
            dic[id]+=str(conten)
        else:
           dic[id]=str(conten)

    for key,value in dic.iteritems():
        #strr=''
        #for i in value:
        #    strr+=i
        #print key,value
        tags = jieba.analyse.extract_tags(value,topK=topK,withWeight=True)#allowPOS=['ns', 'n', 'vn', 'v','nr','eng'])
        tag_map={}
        tag_map.setdefault(float)
        if withWeight is True:
            for tag in tags:
                fw.write(str(key)+' '+str(tag[0])+' '+str(tag[1])+'\n')
                #print(" tag: %s\t\t weight: %f" % (tag[0],tag[1]))
        else:
            print(",".join(tags))
    f.close()
    fw.close()
    
load_corpus2('id_post0.txt')
#load_corpus2('test.txt')
