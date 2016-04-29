#encoding=utf-8
'''
Created on 2015年10月31日

@author: lipd
'''
from __future__ import absolute_import
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import Pool
import re,sys,logging,codecs
import jieba
import jieba.posseg
import os
from operator import itemgetter
import pickle,glob
from collections import defaultdict
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
print sys.getdefaultencoding()
trunk = 1000000
def _glob_files(DATA_PATH):
    """Get all files in DATA_PATH, return a file list"""
    FILE_LIST = glob.glob(DATA_PATH + "/*")
    return FILE_LIST 


def file_process(FILE_PATH):
    '''
    To calculate  the file's idf and tf  , save idf file to lOCALPATH(data/idf,tf)
    !!! Here , idf is the frequecy in the post file,  so tf-idf = tf / idf * the number of ids(set to 1)
    '''
    idf_dict = defaultdict(int) # word : document(id) num
    tf_dict = defaultdict(list) # id : [words num of document, {word:num}]
    id_num = 0  # we should combine contents have same id, so one id is one document
    logger.info("Start processing,  the file names is \n {file}".format(file = FILE_PATH))
    with codecs.open(FILE_PATH,'r','utf-8','ignore') as rfile:
        for n,line in enumerate(rfile):
            if n % trunk == 0:
                logger.info("{num} lines processed".format(num = n))

            tokens = line.strip().split('\t')
            if not len(tokens) == 2:
                continue
            id_index = tokens[0]
            content = tokens[1]
 
            # cut words
            words = list(jieba.cut(content))
            words_num = len(words)
            # drop duplicate 
            words_set = list(set(words))
            # count id's words
            if not id_index in tf_dict:
                # init 
                tf_dict[id_index] = [0,{}]     
            tf_dict[id_index][0] += words_num
             
            for word in words_set:
                # if word comes from sameid , do not add
                if not word in tf_dict[id_index][1]:
                    # if the word in this id has not been count , add
                    idf_dict[word] += 1

                tf_dict[id_index][1][word] = tf_dict[id_index][1].get(word,0) + words.count(word)
                
        
    file_base_name = os.path.basename(FILE_PATH)
    idf_path = os.path.dirname(FILE_PATH) + "/../idf/" + file_base_name + "_idf.pkl"
    tf_path = os.path.dirname(FILE_PATH) + "/../tf/" + file_base_name + "_tf.pkl"
    #print idf_dict
    #print tf_dict.keys()

    idf_file = open(idf_path,'wb')
    pickle.dump(idf_dict,idf_file)
    idf_file.close()

    tf_file =  open(tf_path,'wb')
    pickle.dump(tf_dict,tf_file)
    tf_file.close()
    return 0

def _get_pickle(file_path):
    with open(file_path,'rb') as f:
        dic = pickle.load(f)
    return dic

def _save_pickle(data,file_path):
    with open(file_path,'wb') as f:
        pickle.dump(data,f)
    return 0
                 
def combine_idf(IDF_PATH):
    '''
    Combine every file's idf data 
    '''
    IDF_FILE_LIST = glob.glob(IDF_PATH + "/*_idf.pkl") 
    logger.info("Start combining,  the file names is \n {file}".format(file = ",".join(IDF_FILE_LIST)))

    idf_dict_final = _get_pickle(IDF_FILE_LIST[0]) 

    if len(IDF_FILE_LIST) == 1:
        logger.info("Start combining {file}".format(file = file_name))
        _save_pickle(idf_dict_final,IDF_PATH+"/idf.pkl") 
        return idf_dict_final 

    for file_name in IDF_FILE_LIST[1:]:
        logger.info("Start combining {file}".format(file = file_name))
        dif_dict =  _get_pickle(file_name)
        for key,value in dif_dict.items():
            
            idf_dict_final[key] += value
    _save_pickle(idf_dict_final,IDF_PATH+"/idf.pkl") 
    return idf_dict_final 
     
            
def tf_idf(TF_PATH , IDF_PATH , topK = 5, idf_dict_final=None,weight = True):
    '''
    
    '''
    #tf_idf_dict = defaultdict(dict)
    if not idf_dict_final:
        idf_dict_final = _get_pickle(IDF_PATH) 
    TF_FILE_LIST = _glob_files(TF_PATH) 
    logger.info("TF_FILE: \n {files}".format(files=",".join(TF_FILE_LIST)))
    #print("TF_FILE: \n {files}".format(files=",".join(TF_FILE_LIST)))
    wfile = codecs.open(TF_IDF_PATH + "/tf_idf.data","w",'utf-8','ignore')
    for file_name in TF_FILE_LIST:
        logger.info("Start calculating tf-idf,  the file name is \n {file}".format(file = file_name))
        tf_dict = _get_pickle(file_name) # id : [numm , {word:num ...}]
        #print tf_dict
        count = 0
        for id, value in tf_dict.iteritems():
            if count % trunk == 0:
                logger.info("{num} ids processed".format(num = count))

            tf_idf_dict = defaultdict(float) 
            # id's words num 
            total = value[0]
            for word,num in value[1].iteritems():
                tf = num / float(total)
                tf_idf_dict[word] = tf /  idf_dict_final[word] # idf = 1 / idf,
            #print tf_idf_dict

            if  weight:
                tags = sorted(tf_idf_dict.items(), key=lambda d:d[1],reverse=True)        
                topK_tags = tags[:topK] 
                strs = ''
                for tupl in topK_tags:
                    k,v = tupl
                    strs += k + ":" + str(v) + " " 
                str_line = id + "\t" + strs + "\n"
                #print str_line.encode("utf-8")
                wfile.write(str_line.encode("utf-8",'ignore'))
            else:
                tags = sorted(tf_idf_dict, key=tf_idf_dict.__getitem__,reverse=True)        
                topK_tags = tags[:topK] 
                str_line = id + "\t" + " ".join(topK_tags) + "\n"
                wfile.write(str_line.encode('utf-8','ignore'))
                #print (str_line.encode('utf-8'))
    wfile.close()
                 

if __name__ == "__main__":
    
    ## set path
    ROOT_PATH = sys.path[0]
    DATA_PATH = ROOT_PATH + "/../data/"
    DICT_PATH = DATA_PATH + "/word_dict/"
    ID_POST_PATH = DATA_PATH + "/id_post/"
    IDF_PATH = DATA_PATH + "/idf/"
    TF_PATH = DATA_PATH + "/tf/"
    TF_IDF_PATH = DATA_PATH + "/tf_idf/"
    LOG_PATH = ROOT_PATH + "/../log/main.log"

    ## logging 
    # set logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = TimedRotatingFileHandler(LOG_PATH,when='D')
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
 
    FILE_LISTS = _glob_files(ID_POST_PATH)
    print FILE_LISTS
    ## 1.process file
    map(file_process,FILE_LISTS)

    ## 2. combine_idf
    #combine_idf(IDF_PATH)
    ## 3. tf_idf
    tf_idf(TF_PATH=TF_PATH, IDF_PATH = IDF_PATH + '/idf.pkl',weight=True)

