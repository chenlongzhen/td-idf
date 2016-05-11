#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <map>
#include "../include/cppjieba/Jieba.hpp"
#include "../include/cppjieba/KeywordExtractor.hpp"

using namespace std;
const char* const DICT_PATH = "../res/dict/jieba.dict.utf8";
const char* const HMM_PATH = "../res/dict/hmm_model.utf8";
const char* const USER_DICT_PATH = "../res/dict/user.dict.utf8";
const char* const IDF_PATH = "../res//dict/idf.utf8";
const char* const STOP_WORD_PATH = "../res/dict/stop_words.utf8"; 

typedef struct tf_info_s{
    int total_words;
    map<string,int> word_freq;
}tf_info_t;

void split(const string line, char delim, vector<string> *elems)
{
    stringstream strs(line);
    string elem;
    while(getline(strs, elem, delim))
    {
        elems->push_back(elem);
    }
}

    

void file_process(const string FILE_PATH, map<string,int> &idf_dict, map<string,tf_info_t> &tf_dict)
{
    ifstream ifs(FILE_PATH.c_str());
    // init jieba
    cppjieba::Jieba jieba(DICT_PATH,HMM_PATH,USER_DICT_PATH);
    vector<string> words;

    if (ifs.is_open()) {
        int count = 0;
        string line;
        char delim = ' ';
        while (getline(ifs, line)) {
            count += 1;
            vector<string> elems;
            split(line, delim, &elems);
            if (elems.size() != 2){
                continue;
            }
            string id = elems[0];
            string content= elems[1];
            //cout << count <<": id: "  << id << "\t" << "content: " << content;
            // cut words
            jieba.Cut(content, words, true);
            // print
            vector<string>::iterator iter;
            for (iter=words.begin(); iter!=words.end();++iter){
                cout << *iter << "/";
            }
            cout << endl; 
            
         }
     }
}

int main(int argc, char ** argv){
    string file_path = "../data/id_post/id_post0_sample.txt";
    map <string, int> idf_dict;
    map <string, tf_info_t> tf_dict;
    file_process(file_path,idf_dict,tf_dict);

    return 0;
}
