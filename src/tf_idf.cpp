#include <string>
#include <regex>
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
//const char* const IDF_PATH = "../res//dict/idf.utf8";
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

string get_chinese(const string content)
{
    //const regex pattern("[^\u4E00-\u9FA5]");
    const regex pattern("[^\u4E00-\u9FA5][^\uFE30-\uFFA0]"); //chinese characters
    return regex_replace(content,pattern,"");
}
    

void file_process(const string FILE_PATH, map<string,int> &idf_dict, map<string,tf_info_t> &tf_dict)
{
    ifstream ifs(FILE_PATH.c_str());
    // init jieba
    cppjieba::Jieba jieba(DICT_PATH,HMM_PATH,USER_DICT_PATH);
    vector<string> words;
    string reg_string;
    map<string, tf_info_t> tf_it;
    map<string, int> word_it;

    if (ifs.is_open()) {
        int count = 0;
        string line;
        char delim = '\t';
        while (getline(ifs, line)) {
            count += 1;
            vector<string> elems;
            split(line, delim, &elems);
            if (elems.size() != 2){
                continue;
            }
            string id = elems[0];
            string content= elems[1];
            if (count % 10000 == 0)
                cout << count << " lines processed" << endl;
            //cout << count <<": id: "  << id << "\t" << "content: " << content << endl;
            // cut words
            //jieba.Cut(content, words, true);
            //cout << limonp::Join(words.begin(),words.end(),"/") << endl;
            reg_string = get_chinese(content); // get chinese
            jieba.Cut(reg_string, words, true); // split

            //  
            for (vector<string>::iterator iter = words.begin();
                 iter != words.end();++iter)
            {
                // tf total words
                if (tf_dict.find(id) == tf_dict.end())
                {
                    tf_dict[id].total_words = 1;
                }else{
                    ++tf_dict[id].total_words;
                }
 
                if (tf_dict[id].word_freq.find(*iter) == tf_dict[id].word_freq.end())
                {
                   // if it is a new word for this id , idf_dict + 1
                   if (idf_dict.find(*iter) == idf_dict.end())
                   {
                       idf_dict[*iter] = 1; 
                   }else{
                       ++idf_dict[*iter]; 
                   }
                   // tf word + 1
                   tf_dict[id].word_freq[*iter] = 1;
		}else{
                   ++tf_dict[id].word_freq[*iter];
                }
            }
            
            
         }
     }
}

void tf_idf(map<string,int> idf_dict, map<string,tf_info_t> tf_dict, string file, int topK = 150)
{
    ofstream ofs(file.c_str());
    if (ofs.is_open())
    {
        float tf_idf; 
        string word,id;
        int total;
        float yu;
        for (const auto &it : tf_dict)   
        {
            
            id = it.first; 
            total = it.second.total_words;
            vector<float> value;
            map<string,float> tf_idf_dict;
            for (const auto &itt : it.second.word_freq) 
            {
                word = itt.first;
                tf_idf = itt.second/float(total)/idf_dict[word];
                value.push_back(tf_idf);
                tf_idf_dict[word]= tf_idf;
            }
            // save topK
            sort(value.begin(),value.end(),greater<float>());
            
            if (topK > value.size()) yu = 0; else yu = value[topK];
            for (const auto &itt : tf_idf_dict)
            {
                if (itt.second >= yu)
                {
                ofs << id << " " << itt.first << " " << itt.second <<  endl;
                //cout << id << " " << itt.first << " " << itt.second << endl;
                }
            }
        }
        ofs.close();
    }    
} 
   
          
int main(int argc, char ** argv){
    string id_post_file_path = "../data/id_post/id_post0.txt";
    // 1. prcessing id post file get idf and tf 
    map <string, int> idf_dict;
    map <string, tf_info_t> tf_dict;
    file_process(id_post_file_path,idf_dict,tf_dict);
    // 2. calculating tf_idf
    string save_idf_file = "../data/tf_idf/c_tf_idf.data";
    tf_idf(idf_dict,tf_dict,save_idf_file);
    return 0;
}
