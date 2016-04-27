#!/home/chenlongzhen/python-miniconda
# encoding=utf-8
#@author: BF-algo,longzhen.chen
#sys.path.append('../')

from math import log
import re,codecs,sys
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

#USAGE = "usage:    python extract_tags_idfpath.py [file name] -k [top k]"
#jieba.load_userdict(DATA_PATH + 'dic_nospace_tag') 
#parser = OptionParser(USAGE)
#parser.add_option("-k", dest="topK")
#opt, args = parser.parse_args()


#if len(args) < 1:
#    print(USAGE)
#   sys.exit(1)

#file_name = args[0]

#if opt.topK is None:
#    topK = 240
#else:
#    topK = int(opt.topK)

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
    #ids = []
    #contents = []
    # test
    #with codecs.open(infile,'r') as infile:
    #    for n,line in enumerate(infile):
    #        #print line
    #        if (n+1) % 3 == 0:
    #            print "{} lines processed".format(n)      
    #            exit -1
    #        print _str_replace(line)
    #        tokens = line.strip().split('\t')
    #        print tokens
    #        if not len(tokens) == 2:
    #            continue
    #        ids.append(tokens[0])
    #        print tokens.encode
    #        print "str {}".format(_str_replace(tokens[1]))
    #        contents.append(str(_str_replace(tokens[1])))
    #        print contents
    #       
            
    #words_generaters = []
    def concat_str(data):
        """ concat same id's content"""
        #print content
        print map(unicode,data['content'])
        return "".join(data['content'].values)
    
    # read
    print "--- read data ---"
    data = pd.read_table(infile,sep = "\t",names=['id','content'],encoding='utf-8',dtype={'id':np.str,'content':np.str})
    #print data
    print "--- combine same id's content ---"
    data = pd.read_table(infile,sep = "\t",names=['id','content'],encoding='utf-8')
    combined_data = data.groupby('id').apply(concat_str)
    # sub
    ##################data_subbed = combined_data.apply(_str_replace)
    contents_combined = combined_data.values
    ids_combined = combined_data.index.values
    data_combined = pd.DataFrame({"id":ids_combined,"content":contents_combined}) 
    data_combined = data_combined[['id','content']]
    data = data_combined
    #print data[:3]
   

    # use jieba to split words 
    
    word_generater =  data['content'].apply(jieba.cut)
    word_cut_list = word_generater.apply(list) # unicode type
    print word_cut_list

    # save
    data['content'] = word_cut_list.apply(_unicodelist_to_str)
    save_path = infile + "_splited"
    data.to_csv(save_path,sep = "\t",encodeing='utf-8',header = False,index = False)
    #print map(lambda x: x.encode("utf-8"),word_cut_list[0])[1]

    # return id [words]
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
    
def idf(word_list,documents_num,index_document_dict):
    
    # idf
    idf_dict = defaultdict(float) 
    for term in word_list: 
        idf_value = float(documents_num) / len(index_document_dict[term])
        idf_dict[term] = idf_value
    # save
    idf_dict_dataframe = pd.DataFrame.from_dict(idf_dict,orient = 'index') 
    sorted_file = idf_dict_dataframe.sort_values(by=0,ascending=False)
    sorted_file.to_csv(DATA_PATH + "idf.data",header = False)
    #print sorted_file
    return dict(idf_dict)
        
def tf_idf(dataframe,idf_dict,topk = 5):
   
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
    save_path = DATA_PATH + "tf_idf.data"
    dataframe.to_csv(save_path,sep="\t",header = False,index = False)
             
    return dataframe
    

def main():
    # step 1. split content
    file_path = DATA_PATH + "id_post0_sample.txt" 
    dataframe = split_words(file_path) 
    #print dataframe
    # step 2. get index document
    index_document_dict,documents_num,word_list = index_document(dataframe) 
    #print index_document_dict.items()[:10]
    #print documents_num
    #print word_list[:10]
    # step 3. idf
    idf_dict =  idf(word_list,documents_num,index_document_dict)
    # step 4. tf-idf
    tf_idf_dict = tf_idf(dataframe,idf_dict) 
    #print _str_replace("ä½ å¥½,123,ä½ å¥½,asdq")
    
    
    
    
    
    
    
#    index = IrIndex()
#    chunk = 100
#    #content = open('user_corpus.txt', 'rb').read()
#    #jieba.analyse.set_stop_words("./stop_words.txt")
#    #jieba.analyse.set_idf_path("./weibo_tfidf_1029.txt")
#    file_path = DATA_PATH + "id_post0_sample.txt" 
#    id = []
#    with open(file_path,"r") as rf:
#        for n,line enumerate(rf):
#            # print 
#            if n % chunk == 0        
#                print "{} line data have processed".format(n)
#            tokens  = line.strip().split(" ")
#            # format :id content, if not continue
#            if not len(tokens) == 2:
#                continue
#            # get id and content
#            id.append(tokens[0])
#            content = tokens[1]
#            # count tf and idf
#            index.index_document(content)
            
            
if __name__ == "__main__":
    main()
            


