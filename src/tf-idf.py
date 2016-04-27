#!/home/chenlongzhen/python-miniconda
# encoding=utf-8
#@author: BF-algo,longzhen.chen
#sys.path.append('../')

from logging.handlers import TimedRotatingFileHandler
import logging
from math import log
import re,codecs,sys,os
import glob
from multiprocessing import Pool
import jieba
import jieba.analyse
from optparse import OptionParser
import pandas as pd
from collections import defaultdict
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

ROOT_PATH = sys.path[0]
DATA_PATH = ROOT_PATH + "/../data/"

index = defaultdict(list) 
documents_num = 0 
tf = defaultdict(float)
idf = defaultdict(float)
word_list = []

def _str_replace(content):
    
    """replace Punctuation and characters """
    #pattern = re.compile("^\s+|\s*,*\s*|\s+$")
    pattern = re.compile("[a-z]|[1-9]|.|:")
    words = pattern.sub("",content.encode("utf-8"))
    #terms = [word for word in pattern.split(content)]
    #processed_content =  "".join(terms)
    #print words
    return words

def _unicodelist_to_str(word_list):
    
    word_list_encode = map(lambda x:x.encode("utf-8"),word_list)
    word_line = " ".join(word_list_encode)
    #print word_line
    return word_line
    
    
def split_words(infile):
    """split words and return id [words] dataframe"""
    def concat_str(data):
        """ concat same id's content"""
        #print content
        content_list =  map(unicode,data['content'])
        return "".join(content_list)
    
    # read
    logger.info("read data ")
    data = pd.read_table(infile,sep = "\t",names=['id','content'],encoding='utf-8',dtype={'id':np.str,'content':np.str})
    #print data
    logger.info("combine same id's content ")
    data = pd.read_table(infile,sep = "\t",names=['id','content'],encoding='utf-8')
    combined_data = data.groupby('id').apply(concat_str)
    # sub
    ##################data_subbed = combined_data.apply(_str_replace)
    contents_combined = combined_data.values
    ids_combined = combined_data.index.values
    data_combined = pd.DataFrame({"id":ids_combined,"content":contents_combined}) 
    data_combined = data_combined[['id','content']]
    data = data_combined

    # use jieba to split words 
    word_generater =  data['content'].apply(jieba.cut)
    word_cut_list = word_generater.apply(list) # unicode type

    # save
    #data['content'] = word_cut_list.apply(_unicodelist_to_str)
    #save_path = infile + "_splited"
    #data.to_csv(save_path,sep = "\t",encodeing='utf-8',header = False,index = False)
    #print map(lambda x: x.encode("utf-8"),word_cut_list[0])[1]

    data['content'] = word_cut_list
    return data 


def index_document(dataframe):
    """construct index_document"""
    index_document_dict = defaultdict(list)
    documents_num = 0
    word_list = []

    word_lists = dataframe['content']
    for terms in word_lists:
        ## add to documents
        documents_num += 1
        document_pos = documents_num  - 1 # -1
        ## add posts to index, updating tf
        for term in terms:
            # index document
            index_document_dict[term].append(document_pos)
            # append word to list 
            if not term in word_list:
                word_list.append(term) 
    return (index_document_dict,documents_num,word_list)    
    
def idf(word_list,documents_num,index_document_dict,save_path):
    
    # idf
    idf_dict = defaultdict(float) 
    for term in word_list: 
        idf_value = float(documents_num) / len(index_document_dict[term])
        idf_dict[term] = idf_value
    # save
    idf_dict_dataframe = pd.DataFrame.from_dict(idf_dict,orient = 'index') 
    sorted_file = idf_dict_dataframe.sort_values(by=0,ascending=False)
    sorted_file.to_csv(save_path ,header = False)
    #print sorted_file
    return dict(idf_dict)
        
def tf_idf(dataframe,idf_dict,tf_idf_path,topk = 5):
   
    def _tf_idf(word_list):
        tf_idf_dict = defaultdict(float)
        for word in word_list:
            words_num = len(word_list)
            if word in tf_idf_dict:
                continue 
            tf = word_list.count(word) / float(words_num)
            tf_idf_dict[word] = tf * idf_dict[word]
            # sort
            sorted_tf_idf = sorted(tf_idf_dict.iteritems(),key=lambda d:d[1],reverse=True)  
            # get topK
            key_value_str = ""
            count = 0
            for key,value in sorted_tf_idf: 
                if not count < topk:
                    break
                count += 1
                key_value_str += key + ":" + str(value) + " " 
        return key_value_str
     
    dataframe['content'] = dataframe['content'].apply(_tf_idf)
    dataframe.to_csv(tf_idf_path,sep="\t",header = False,index = False)
    return dataframe
    

def process(file_path):
    # parent parent dir 
    path = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    idf_path = path + "/../idf/"+file_name+"_idf.data"
    tf_idf_path =path + "/../tf_idf/"+file_name+"_tf_idf.data"
    word_dict_path = path + "/../word_dict/"+"id_word.data"

    # load dict
    logger.info("load word dict ")
    jieba.load_userdict(word_dict_path) 

    # step 1. split content
    logger.info("begin to read and split ")
    dataframe = split_words(file_path) 
    #logger.info dataframe

    logger.info("construct index document ")
    # step 2. get index document
    index_document_dict,documents_num,word_list = index_document(dataframe) 
    #print index_document_dict.items()[:10]
    #print documents_num
    #print word_list[:10]

    # step 3. idf
    logger.info(" begin to count idf ")
    idf_dict =  idf(word_list,documents_num,index_document_dict,idf_path)

    # step 4. tf-idf
    logger.info(" begin to count tfidf ")
    tf_idf_dict = tf_idf(dataframe,idf_dict,tf_idf_path) 
    #print _str_replace("ä½ å¥½,123,ä½ å¥½,asdq")
    
            
if __name__ == "__main__":
    ##  logging
    ROOTPATH = sys.path[0] 
    # set logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # create console handler and set level to debug
    ch = TimedRotatingFileHandler(ROOTPATH + "/../log/main.log",when='D')
    ch.setLevel(logging.DEBUG)
    
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    
    ## thread pool
    file_res_path = "/home/chenlongzhen/IdeaProjects/td-idf/data/id_post"     
    file_list = glob.glob(file_res_path + "/*")
    logger.info("id_post files:")
    logger.info("\n".join(file_list))
    
    pool = Pool(2)
    processes = pool._pool
    pool.map(process,file_list)
    pool.close()

    
    
