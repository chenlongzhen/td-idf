#!/home/chenlongzhen/python-miniconda
#-*-encoding=utf-8-*-

from __future__ import unicode_literals
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import Pool
from collections import defaultdict
import os,ConfigParser,codecs,logging,glob
import sys,re,argparse
import jieba
import jieba.posseg as pseg
import time
reload(sys)
sys.setdefaultencoding('utf-8')
trunk = 10000

def _glob_files(DATA_PATH):
    """Get all files in DATA_PATH, return a file list"""
    FILE_LIST = glob.glob(DATA_PATH + "/*")
    return FILE_LIST

def save_dict(my_dict,save_path,mode = 'tf'):
    """
    save dict to local
    idf : word\tidf
    tf : id\tword1:tf word2:tf
    """
    logger.info("begin to save {}".format(mode))
    with codecs.open(save_path,'w','utf-8','ignore') as wfile:
        count = 0 
        if not mode == 'tf':
            for k,v in my_dict.items():
                count += 1
                if count % (trunk/10) == 0:
                    logger.info("{} lines idf processed".format(count))
                strs = k + "\t" + str(v) + "\n"
                strs=strs.encode('utf-8')
                wfile.write(strs)    
        else:
            for k,v in my_dict.items():
                count += 1
                if count % (trunk/10) == 0:
                    logger.info("{} lines if processed".format(count))
                strs = str(k) + "\t"
                total_words = v[0]
                for ik,iv in v[1].items():
                    strs += ik +":"+str(iv/float(total_words)) + " "
                strs += "\n"
                wfile.write(strs.encode('utf-8'))
    return 0

def read_idf_dict(read_path):
    """

    """
    
    with codecs.open(read_path,'r','utf-8','ignore') as rfile:
        idf_dict = defaultdict(int)
        for line in rfile:
           tokens = line.split("\t")        
           idf_dict[tokens[0]] = int(tokens[1])            
    return idf_dict

def find_chinese(content):
    ''' '''
    pattern = re.compile("http.+")
    result,number = pattern.subn(u'',content)
    if result:
        #cont = m.string
        #print cont.encode("utf-8")
        return result 
    else:
        return None
    
 
def get_newfile_list(file_list,TIME_STAMP_PATH):
    '''get new file for processing '''
    try:
        timestamp = float(open(TIME_STAMP_PATH,'r').read())
    except :
        logger.error("no timestamp! timestamp is setted to 0")
        print("no timestamp! timestamp is setted to 0")
        timestamp = 0
         
    print "timestamp:" + str(timestamp)
    new_file_list = []
    for name in file_list:
        if os.path.getmtime(name) > timestamp:
            new_file_list.append(name)       
    return new_file_list
        
        

def file_process(FILE_PATH,noPOS = [u'x',u'd',u'f',u'ws',u'wp',u'o',u'm',u'u',u'uj',u'q',u'y',u'p',u'c',u'r']):
    '''
    To calculate  the file's idf and tf  , save idf file to lOCALPATH(data/idf) return tf dict
    !!! Here , idf is the frequecy in the post file,  so tf-idf = tf / idf * the number of ids(set to 1)
    '''
    idf_dict = defaultdict(int) # word : document(id) num
    tf_dict = defaultdict(list) # id : [words num of document, {word:num}]
    id_num = 0  # we should combine contents have same id, so one id is one document
    logger.info("Start processing,  the file names is \n {file}".format(file = FILE_PATH))
    ## test
    #rfile = codecs.open(FILE_PATH,'r','utf-8','ignore').read()
    #print rfile.encode("utf-8")
    #lines = rfile.split("\r\n")
    #print lines[:10]
    with codecs.open(FILE_PATH,'r','utf-8','ignore') as rfile:
        count = 0
        for line in rfile:
            count += 1
            if count % trunk == 0:
                logger.info("{num} lines processed".format(num = count))

            tokens = line.strip().split('\t')
            if not len(tokens) == 2:
                continue
            id_index = tokens[0]
            content = tokens[1]
            # replace http.+
            content = find_chinese(content)
            if not content:
                continue
            # cut words
           # cut_words = pseg.cut(content)
           # words = []
           # cut_words =  list(cut_words)
           # for word,flag in  cut_words:
           #     if not flag in noPOS:
           #         words.append(word)
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
    idf_path = os.path.dirname(FILE_PATH) + "/../idf/" + file_base_name + "_idf.data"
    tf_path = os.path.dirname(FILE_PATH) + "/../tf/" + file_base_name + "_tf.data"
    save_dict(idf_dict,idf_path,'idf')    
    save_dict(tf_dict,tf_path,'tf')    
    return 0  

def combine_idf(IDF_PATH):
    '''
    Combine every file's idf data
    '''
    IDF_FILE_LIST = glob.glob(IDF_PATH + "/*_idf.data")
    #IDF_FILE_LIST =  get_newfile_list(IDF_FILE_LIST)
    logger.info("Start combining,  the file names is \n {file}".format(file = ",".join(IDF_FILE_LIST)))

    if len(IDF_FILE_LIST) == 1:
        logger.info("Only 1  {file}, no need to combine".format(file = IDF_FILE_LIST[0]))
        #_save_pickle(idf_dict_final,IDF_PATH+"/idf.pkl")
        #cmd = "cp {ffile} {tfile}".format(ffile=IDF_FILE_LIST[0],tfile=IDF_PATH + "/idf.pkl")
        #logger.info(cmd)
        #status = os.system(cmd)
        idf_dict = read_idf_dict(IDF_FILE_LIST)
        return idf_dict

    logger.info("Start combining {file}".format(file = IDF_FILE_LIST[0]))
    idf_dict_final = read_idf_dict(IDF_FILE_LIST[0])
    for file_name in IDF_FILE_LIST[1:]:
        logger.info("Start combining {file}".format(file = file_name))
        idf_dict =  read_idf_dict(file_name)
        for key,value in idf_dict.items():
            idf_dict_final[key] += value
    return idf_dict_final

def tf_idf(TF_PATH ,idf_dict_final, topK = 150, weight = True):
    '''

    '''
    TF_FILE_LIST = _glob_files(TF_PATH)
    TF_FILE_LIST =  get_newfile_list(TF_FILE_LIST,TIME_STAMP_PATH_TF_IDF)
    if len(TF_FILE_LIST) == 0:
        logger.info("NO NEW TF FILES , EXIT!")
        print("NO NEW TF FILES , EXIT!")
        exit(0)
    
    logger.info("TF_FILE: \n {files}".format(files=",".join(TF_FILE_LIST)))
    #print("TF_FILE: \n {files}".format(files=",".join(TF_FILE_LIST)))
    wfile = codecs.open(TF_IDF_PATH + "/tf_idf.data","w",'utf-8','ignore')
    for file_name in TF_FILE_LIST:
        logger.info("Start calculating tf-idf,  the file name is \n {file}".format(file = file_name))
        with codecs.open(file_name,'r','utf-8','ignore') as rfile:
            for n,line in enumerate(rfile):
                if n % trunk == 0:
                    logger.info("{} ids has been processed! ".format(n)) 
                tf_idf_dict = defaultdict(float)
                tokens = line.split("\t")        
                if len(tokens) != 2:
                    continue
                id = tokens[0]
                word_dict = tokens[1]
                for word_tf in word_dict.split(" "):
                    word_tf =  word_tf.strip().split(":")
                    if len(word_tf) != 2:
                        continue
                    #print word_tf[0].encode('utf-8')
                    #print idf_dict_final[word_tf[0]]
                    #print word_tf[1].encode('utf-8')
                    idf_value =  float(idf_dict_final[word_tf[0]])
                    if idf_value == 0:
                        continue
                    tf_idf_dict[word_tf[0]] = float(word_tf[1])/idf_value
        
                if  weight:
                    tags = sorted(tf_idf_dict.items(), key=lambda d:d[1],reverse=True)
                    topK_tags = tags[:topK]
                    strs = ''
                    for tupl in topK_tags:
                        k,v = tupl
                        str_line = id + " " + k + " " + str(v)  + "\n"
                        #print str_line.encode("utf-8")
                        wfile.write(str_line.encode("utf-8",'ignore'))
                else:
                    tags = sorted(tf_idf_dict, key=tf_idf_dict.__getitem__,reverse=True)
                    topK_tags = tags[:topK]
                    for tag in topK_tags:
                        str_line = id + " " + tag + "\n"
                        wfile.write(str_line.encode('utf-8','ignore'))
                        #print (str_line.encode('utf-8'))
    wfile.close()


if __name__ == "__main__":
    ## sys args
    ## Parse arguments
    parser = argparse.ArgumentParser(description = "td idf ")
    parser.add_argument('-t','--topK', action ='store',type = int,
                        default = 150, help = "tf idf topK ")
    parser.add_argument('-p','--processes', action ='store',type = int,
                        default = 3, help = "processes' num")
    parser.add_argument('-w','--weight', action ='store',type = bool,
                        default = 1, help = "if print weight")
    parser.add_argument('-s','--step', action ='store',type = int,
                        default = 0, help = "process steps, 0: from process to tf_idf,1:process every files tf and idf,2:calculating tf_idf")
    args = parser.parse_args()
    topK = args.topK
    N_PROCESSES = args.processes
    weight = args.weight
    step = args.step

    ## set path
    ROOT_PATH = sys.path[0]
    DATA_PATH = ROOT_PATH + "/../data/"
    DICT_PATH = DATA_PATH + "/word_dict/"
    ID_POST_PATH = DATA_PATH + "/id_post/"
    IDF_PATH = DATA_PATH + "/idf/"
    TF_PATH = DATA_PATH + "/tf/"
    TF_IDF_PATH = DATA_PATH + "/tf_idf/"
    LOG_PATH = ROOT_PATH + "/../log/main.log"
    CONFIG_PATH = ROOT_PATH + "/../config/"
    #LOG_PATH = ROOT_PATH + "/home/chenlongzhen/IdeaProjects/td-idf/log/main.log"
    TIME_STAMP_PATH_TF_IDF = DATA_PATH + "/timestamp_tf_idf"
    TIME_STAMP_PATH_PROCESS = DATA_PATH + "/timestamp_process"

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

    #print FILE_LISTS
    FILE_LISTS = _glob_files(ID_POST_PATH)
    ## config
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_PATH)
    ## param
    logger.info("************************************************************")
    logger.info("Parameters")
    logger.info("    step: %d" % step)
    logger.info("    topK: %d" % topK)
    logger.info("    weight: %d" % weight)
    logger.info("    N_processes: %d" % N_PROCESSES)
    logger.info("    files:")
    for name in FILE_LISTS:
        logger.info("    %s" %name)
    logger.info("************************************************************")
    
    if step == 0 or step == 1:
        #print FILE_LISTS
        FILE_LISTS = get_newfile_list(FILE_LISTS,TIME_STAMP_PATH_PROCESS)
        if len(FILE_LISTS) == 0:
            logger.info("NO NEW POST FILES , EXIT!")
            print("NO NEW POST FILES , EXIT!")
            exit(0)
        # 1.process file
        map(file_process,FILE_LISTS)
        pool = Pool(N_PROCESSES)
        processes = pool._pool
        pool.map(file_process,FILE_LISTS)
        pool.close()
        time.sleep(1)
        with open(TIME_STAMP_PATH_PROCESS,'w') as infile:
            infile.write(str(time.time()))
        print 'finish processing!'
    if step ==0 or step == 2:
        ## 2. combine_idf
        idf_dict = combine_idf(IDF_PATH)
        ## 3. tf_idf
        tf_idf(TF_PATH=TF_PATH, idf_dict_final = idf_dict,weight=weight,topK=topK)

        # refresh stamp
        with open(TIME_STAMP_PATH_TF_IDF,'w') as infile:
            infile.write(str(time.time()))
        logger.info("*"*80)
        print "finish all!"
