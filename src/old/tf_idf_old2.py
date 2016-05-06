#coding=utf-8
'''
Created on 2015年10月31日

@author: lipd
'''
from __future__ import absolute_import
import re
import jieba
import jieba.posseg
import os
from operator import itemgetter
import pickle

class IrIndex:
    """An in-memory inverted index"""

    pattern = re.compile("^\s+|\s*,*\s*|\s+$")
    
    STOP_WORDS = set((
        "the", "of", "is", "and", "to", "in", "that", "we", "for", "an", "are",
        "by", "be", "as", "on", "with", "can", "if", "from", "which", "you", "it",
        "this", "then", "at", "have", "all", "not", "one", "has", "or", "that"
    ))

    def __init__(self, idf_path=None):
        self.index = {}
        self.documents = []
        self.vectors = []
        self.index_docs={}
        self.path = ""
        self.idf_freq = {}
        self.word_id={}
        self.id_word={}
        self.median_idf = 0.0
        self.stop_words=self.STOP_WORDS.copy()
        self.tokenizer = jieba.dt
        self.postokenizer = jieba.posseg.dt
        if idf_path:
            self.set_new_path(idf_path)
    
    def set_stop_words(self, stop_words_path):
        abs_path = stop_words_path
        if not os.path.isfile(abs_path):
            raise Exception("keyword extraction: file does not exist: " + abs_path)
        content = open(abs_path, 'rb').read().decode('utf-8')
        for line in content.splitlines():
            self.stop_words.add(line)
    def get_stopwords(self):
        return self.stop_words


    def set_new_path(self, new_idf_path):
        if self.path != new_idf_path:
            self.path = new_idf_path
            content = open(new_idf_path, 'rb').read().decode('utf-8')
            self.idf_freq = {}
            for line in content.splitlines():
                word, freq = line.strip().split(' ')
                self.idf_freq[word] = float(freq)
            self.median_idf = sorted(
                self.idf_freq.values())[len(self.idf_freq) // 2]

    def get_idf(self):
        return self.idf_freq, self.median_idf
    
    def extract_tags(self, sentence, topK=50, withWeight=True,allowPOS=()):
        """
        Extract keywords from sentence using TF-IDF algorithm.
        Parameter:
            - topK: return how many top keywords. `None` for all possible words.
            - withWeight: if True, return a list of (word, weight);
                          if False, return a list of words.
            - allowPOS: the allowed POS list eg. ['ns', 'n', 'vn', 'v','nr'].
                        if the POS of w is not in this list,it will be filtered.
        """
        if allowPOS:
            allowPOS = frozenset(allowPOS)
            words = self.postokenizer.cut(sentence)
        else:
            words = self.tokenizer.cut(sentence)
        freq = {}
        #将抽取的keyword转化成数字ID
        self.word2index('id_word.txt')
        for w in words:
            if allowPOS:
                if w.flag not in allowPOS:
                    continue
                else:
                    w = w.word
            if len(w.strip()) < 2 or w.lower() in self.stop_words:
                continue
            if w not in self.word_id.keys():
                continue
            freq[self.word_id[w]] = freq.get(w, 0.0) + 1.0
        total = sum(freq.values())
        for k in freq:
            wd=self.id_word[k]
            freq[k] *= self.idf_freq.get(wd, self.median_idf) / total

        if withWeight:
            tags = sorted(freq.items(), key=itemgetter(1), reverse=True)
        else:
            tags = sorted(freq, key=freq.__getitem__, reverse=True)
        if topK:
            return tags[:topK]
        else:
            return tags

    def index_document(self, id,document, allowPOS=()):
        # split
        #terms = [word for word in self.pattern.split(document)]
        # add to documents
        self.documents.append(document)
        document_pos = len(self.documents) - 1
        # add posts to index, while creating document vector
        if id not in self.index_docs:
            self.index_docs[id]=' '
        #self.index_docs[id].append(document)
        self.index_docs[id]+=str(document)
    #将KNN生成的keyword和ID之间的映射关系互相转化
    def word2index(self,id_word):
        wordslist=open(id_word,'rb').read().decode('utf-8')
        for line in wordslist.splitlines():
            word_id,word = line.strip().split(' ')
            self.word_id[word] = int(word_id)
            self.id_word[int(word_id)] =word
    
    def dumpUser_idf(self, userIdf, pathDump):
        try:
            file = open(pathDump, "wb")
            pickle.dump(userIdf, file)
            file.close()
        except IOError as e:
            print(e)
    def loadExtUser_idf(self, pathDump):
        print("Loading external useridf data...")
        try:
            file = open(pathDump, "rb")
            model = pickle.load(file)
            file.close()
            print("\tDone!")
            return model
        except:
            print("\tFailed!")
            return None
                
    def create_tfidf_list(self):
        #print self.index_docs
        self.user_idf={}
        self.set_new_path('weibo_tfidf_1029.txt')
        #self.idf_freq, self.median_idf = self.get_idf()
        for id in self.index_docs.keys():
            x=self.extract_tags(self.index_docs[id])
            self.user_idf[id]=x
        #print self.user_idf
        self.dumpUser_idf(self.user_idf,'user_idf_1101.pkl')
        #print self.loadExtUser_idf('user_idf_1031.pkl')
        #print self.idf_freq, self.median_idf

def tf_idf_process(FILE_PATH):
    '''
    Process a id_post file, and save id word:tf-idf

    '''
    logger.info("Start ")
    with codecs.open(FILE_PATH,encoding = 'utf-8') as infile:
        for line in infile:
            tokens = line.strip().split('\t')
            if not len(tokens) == 2:
                continue
            id_index_doc = tokens[0]
            words = tokens[1] 
            index.index_document(id_index_doc,words)
        index.create_tfidf_list()
    

if __name__ == "__main__":
    
    ## set path
    ROOT_PATH = sys.path[0]
    DATA_PATH = ROOT_PATH + "/../data/"
    DICT_PATH = DATA_PATH + "/word_dict/"
    ID_POST_PATH = DATA_PATH + "/id_post/"
    IDF_PATH = DATA_PATH + "/idf/"
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
 
    ## 1.get post file
    POST_FILE_PATH_LIST = glob.glob(ID_POST_PATH)
    logger.info("id_post files :") 
    logger.info("\n".join(POST_FILE_PATH_LIST)) 

    
    index = IrIndex()
    #index.index_document('01',"非常")
    #index.index_document('01',"姚明  垃圾 很好机器人口 Clos de Beze 2005, Bourgogne, France")
    #index.index_document('02',"Bruno Clair Chambertin Clos de Beze 2005, Bourgogne, France")
    
    #inClient = open(,'r')
