#include <sys/time.h>
#include <string>
#include <string.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <sstream>
#include <stdlib.h>//strtod
#include <algorithm>//sort
#include <utility>



using namespace std;
bool TEST_MODE = 1;

const char * const PRO_NAME = "user labels tools";
const char * const PRO_CMD = "usrlabel";
static int g_yu = 3;



void split(std::string& s, std::string& delim,vector< string >* ret)
{
    size_t last = 0;
    size_t index=s.find_first_of(delim,last);
    while (index!=std::string::npos)
    {
        ret->push_back(s.substr(last,index-last));
        last=index+1;
        index=s.find_first_of(delim,last);
    }
    if (index-last>0)
    {
        ret->push_back(s.substr(last,index-last));
    }
}

typedef struct conf_info_s{
    string id_word_file;
    string user_words_weight;
    string topn_sim_word;
    string label_map;
}conf_info_t;
typedef struct id_weight_s{
    int wordID;
    double weight;
}id_weight_t;
typedef struct key_info_s{
    string key;
    double weight;
}key_info_t;
typedef struct weight_count_pair_s{
    double weight;
    int count;
}w_c_pair_t;
typedef struct user_label_key_s{
    map<int, w_c_pair_t> maplabel;
    map<int, double> mapkey;
}user_label_key_t;
static conf_info_t g_confinfo;
bool initconf(string conffile)
{
    ifstream ifs(conffile.c_str());
    if(ifs.is_open())
    {
        string line;
        string delim = " ";
        while(getline(ifs, line))
        {
            vector<string> vec;
            split(line,delim, &vec);
            if(vec.size() == 2)
            {
                if(!strcmp("WORDID",vec[0].c_str()))
                {
                    g_confinfo.id_word_file = vec[1];
                }
                else if(!strcmp("USERWORDWEIGHT",vec[0].c_str()))
                {
                    g_confinfo.user_words_weight= vec[1];
                }
                else if(!strcmp("TOPNSIMWORD",vec[0].c_str()))
                {
                    g_confinfo.topn_sim_word= vec[1];
                }
                else if(!strcmp("LABELMAP",vec[0].c_str()))
                {
                    g_confinfo.label_map= vec[1];
                }
                else
                {
                    printf("unknown config item %s:%s\r\n",vec[0].c_str(),vec[1].c_str());
                }
            }
            else
            {
                printf("unknown config item size %d : %s\r\n",vec.size(),vec[0].c_str());
            }
        }
        ifs.close();
        return true;
    }
    else
    {
        return false;
    }
}
bool initkeymap(map<string, int> &mapkey, string file)
{
    ifstream ifs(file.c_str());
    if(ifs.is_open())
    {
        string line;
        string delim = " ";
        int count = 0;
        while(getline(ifs, line))
        {
            count += 1;
            vector<string> vec;
            split(line,delim, &vec);
            if(vec.size() == 2)
            {
                mapkey.insert(pair<string, int>(vec[1], atoi(vec[0].c_str())));

            }
            else
            {
                printf("error: init key map, unknown key and value, line %n \r\n", count);
            }
        }
        //printf("init key map, %n key added!\r\n", count);
        ifs.close();
        return true;
    }
    else
    {
        return false;
    }
}
bool updatelabelmap(map<int,vector<int> > &maplabel, map<string,int> &mapkey, string file)
{
    ifstream ifs(file.c_str());
    if(ifs.is_open())
    {
        string line;
        string delim = "\t";
        int count = 0;
        while(getline(ifs, line))
        {
            count += 1;
            vector<string> vec;
            split(line,delim, &vec);
            //printf("updatelabelmap count %d: %s\r\n",count,line.c_str());
            if(vec.size() >= 2)
            {
                map<string, int>::iterator it = mapkey.find(vec[0]);
                if(it != mapkey.end())
                {
                    string subdelim = ",";
                    vector<string> subvec;
                    split(vec[1],subdelim, &subvec);
                    if(subvec.size() >= 1)
                    {
                        vector<int> vid;
                        for(int i = 0; i < subvec.size(); ++i)
                        {
                            vid.push_back(atoi(subvec[i].c_str()));
                        }
                        maplabel.insert(pair<int, vector<int> >(it->second, vid));
                    }
                }
                //mapkey.insert(pair<string, int>(vec[1], atoi(vec[0].c_str())));

            }
            else
            {
                printf("error in line %d : init label map, unknown key and value, \r\n", count);
            }
        }
        ifs.close();

        /*ofstream ofs("./test_label_map.txt");
        if(ofs.is_open())
        {
            map<int,vector<int> >::iterator it = maplabel.begin();
            for(;it != maplabel.end(); ++it)
            {
                ofs << it->first <<":";
                for(int i = 0; i < it->second.size(); ++i)
                {
                    ofs << it->second[i] << ",";
                }
                ofs << endl;
            }
            ofs.close();
        }*/
        return true;
    }
    else
    {
        return false;
    }
}
bool initsimilarmap(map<int, vector<id_weight_t> > &mapsim, string file)
{
    ifstream ifs(file.c_str());
    if(ifs.is_open())
    {
        string line;
        string delim = "\t";
        int count = 0;
        while(getline(ifs, line))
        {
            count += 1;
            vector<string> vec;
            split(line,delim, &vec);
            if(vec.size() == 3)
            {
                id_weight_t word;
                word.wordID = atoi(vec[1].c_str());
                word.weight = strtod(vec[2].c_str(),NULL);
                map<int, vector<id_weight_t> >::iterator it = mapsim.find(atoi(vec[0].c_str()));
                if(it != mapsim.end())
                {
                    it->second.push_back(word);
                    mapsim[atoi(vec[0].c_str())] = it->second;
                }
                else
                {
                    vector<id_weight_t> vsim;
                    vsim.push_back(word);
                    mapsim.insert(pair<int, vector<id_weight_t> >(atoi(vec[0].c_str()),vsim));
                }
                //mapkey.insert(pair<string, int>(vec[1], atoi(vec[0].c_str())));
            }
            else
            {
                printf("error in line %n : init similar words map, unknown key and value, \r\n", count);
            }
        }
        ifs.close();
        return true;
    }
    else
    {
        return false;
    }
}
void addUserlabel(//map<int,map<int, double> > &mapUser,
                  map<int,user_label_key_t> &mapUser,
                  int uid, int labelid, double weight, int keyid, double keyweight)
{
    //map<int, map<int, double> >::iterator ituser = mapUser.find(uid);
    map<int, user_label_key_t>::iterator ituser = mapUser.find(uid);
    if(ituser != mapUser.end())//find
    {
        map<int,w_c_pair_t>::iterator itlabel = ituser->second.maplabel.find(labelid);
        if(itlabel != ituser->second.maplabel.end())//find
        {
            //cout << "find labelid : " << labelid <<", weight: " << itlabel->second << endl;
            itlabel->second.weight += weight;
            itlabel->second.count += 1;
            //cout << "after weight: " << itlabel->second;
            ituser->second.maplabel[labelid] = itlabel->second;
            ituser->second.mapkey[keyid] = keyweight;
        }
        else//not find
        {
            w_c_pair_t wc;
            wc.weight = weight;
            wc.count = 1;
            ituser->second.maplabel.insert(pair<int, w_c_pair_t>(labelid, wc));
            ituser->second.mapkey.insert(pair<int, double>(keyid, keyweight));
        }
    }
    else//not find
    {
        user_label_key_t ulabel;
        w_c_pair_t wc;
        wc.weight = weight;
        wc.count = 1;
        ulabel.maplabel.insert(pair<int,w_c_pair_t>(labelid,wc));
        ulabel.mapkey.insert(pair<int, double>(keyid, keyweight));
        mapUser.insert(pair<int,user_label_key_t>(uid, ulabel));
    }

}
bool initUserlabelmap(//map<int,map<int, double> > &mapUser,
                      map<int,user_label_key_t> &mapUser,
                      map<string, int> mapKey,
                      map<int, vector<int> > mapLabel,
                      map<int, vector<id_weight_t> > mapSim,
                      string file)
{
    ifstream ifs(file.c_str());
    if(ifs.is_open())
    {
        string line;
        string delim = " ";
        int count = 0;
        while(getline(ifs, line))
        {
            count += 1;
            vector<string> vec;
            split(line,delim, &vec);
            //cout << count << ":" << line << endl;
            if(vec.size() == 3)
            {
                int uid = atoi(vec[0].c_str());
                string word = vec[1];
                double weight = strtod(vec[2].c_str(),NULL);

                //get the key id
                map<string, int>::iterator itkeyid = mapKey.find(word);
                if(itkeyid == mapKey.end())
                {
                    //printf();
                    continue;
                }
                int keyid = itkeyid->second;

                //get the label list
                map<int, vector<int> >::iterator itlabel = mapLabel.find(keyid);
                if(itlabel != mapLabel.end())//find the label
                {
                    vector<int> vlabel = itlabel->second;
                    for(int i = 0; i < vlabel.size(); ++i)
                    {
                        //cout << "adduserlabel:" << uid << ":" << vlabel[i] << ":" << weight*1 << endl;
                        addUserlabel(mapUser, uid, vlabel[i], weight*1,keyid, weight);
                    }
                }
                else//can't find the label
                {
                    //find similar words
                    map<int, vector<id_weight_t> >::iterator itsim = mapSim.find(keyid);
                    if(itsim == mapSim.end())
                    {
                        printf("no similar keys of %s\r\n",word.c_str());
                        continue;
                    }
                    vector<id_weight_t> vsim = itsim->second;
                    for(int i = 0; i < vsim.size(); ++i)
                    {
                        double w1 = vsim[i].weight;
                        map<int, vector<int> >::iterator itsimlabel = mapLabel.find(vsim[i].wordID);
                        if(itsimlabel == mapLabel.end())//similar word has no label
                        {
                            continue;
                        }
                        else
                        {
                            vector<int> vsimlabel = itsimlabel->second;
                            for(int k = 0; k < vsimlabel.size(); ++k)
                            {
                                //cout << "adduserlabel:" << uid << ":" << vsimlabel[k] << ":" << weight*w1*1 << endl;
                                addUserlabel(mapUser, uid, vsimlabel[k], weight*w1*1, vsim[i].wordID, w1);
                            }
                        }
                    }
                }
            }
            else
            {
                printf("error in line %n : init similar words map, unknown key and value, \r\n", count);
            }
        }
        ifs.close();
        return true;
    }
    else
    {
        return false;
    }
}
/*typedef struct key_info_s{
    string key;
    double weight;
}key_info_t;
typedef struct user_label_key_s{
    map<int, double> maplabel;
    map<int,double> mapkey;
}user_label_key_t;
*/
bool outputresult(/*map<int,map<int, double> > &mapUser*/map<int,user_label_key_t> &mapUser, string file)
{
    ofstream ofs(file.c_str());
    if(ofs.is_open())
    {
        map<int,user_label_key_t>::iterator ituser = mapUser.begin();
        ofs << "[";
        for(; ituser != mapUser.end(); ++ituser)
        {
            if(ituser != mapUser.begin())
            {
                ofs << ",";
            }
            ofs << "{\"uid\":" << (unsigned int)(ituser->first) << ",\"labarr\":[" ;
            //txt output ofs << (unsigned int)(ituser->first) << "\t";
            map<int, w_c_pair_t>::iterator itlabel = ituser->second.maplabel.begin();
            bool isFirst = true;
            for(; itlabel != ituser->second.maplabel.end(); ++itlabel)
            {
                if(itlabel->second.count < g_yu)
                {
                    continue;
                }
                if(isFirst)
                {
                    isFirst = false;
                }
                else
                //if(itlabel != ituser->second.maplabel.begin())
                {
                    ofs << ",";
                    //ofs << ",";
                }
                ofs << "{\"lid\":" << itlabel->first << ",\"weight\":" << itlabel->second.weight
                    << ",\"count\":" << itlabel->second.count <<"}";
            }
            ofs << "],\"karr\":[";
            map<int, double>::iterator itkey = ituser->second.mapkey.begin();
            for(; itkey != ituser->second.mapkey.end(); ++itkey)
            {
                if(itkey != ituser->second.mapkey.begin())
                {
                    ofs << ",";
                }
                ofs << "{\"kid\":"<< itkey->first << ",\"weight\":" << itkey->second << "}";
            }
            ofs << "]}";
            //ofs << endl;
        }
        ofs << "]" << endl;
        ofs.close();
        return true;
    }
    else
    {
        return false;
    }
}
bool userlabel(string out_file,string conf_file)
{
    bool res = false;
    //string conf_file = "/hdfs/sdd/mqtlab/usrlabel/config.conf";
    res = initconf(conf_file);
    map<string, int> mapKeyword;//<key, keyid>
    res = initkeymap(mapKeyword,g_confinfo.id_word_file);
    if(!res)
    {
        cout << "file: " << g_confinfo.id_word_file << endl;
        cout << "initkeymap error!" << endl;
        return res;
    }
    else
    {
        cout << "file: " << g_confinfo.id_word_file << endl;
        cout << "mapKeyword : " << mapKeyword.size() << " items added!" << endl;
    }

    map<int, vector<int> > mapLabel;//<keyid,vector<labelid>>
    res = updatelabelmap(mapLabel,mapKeyword,g_confinfo.label_map);
    if(!res)
    {
        cout << "file: " << g_confinfo.label_map << endl;
        cout << "updatelabelmap error!" << endl;
        return res;
    }
    else
    {
        cout << "file: " << g_confinfo.label_map << endl;
        cout << "mapLabel : " << mapLabel.size() << " items added!" << endl;
    }

    map<int, vector<id_weight_t> > mapSimilar;//<keyid,<keyid,weight>>
    res = initsimilarmap(mapSimilar,g_confinfo.topn_sim_word);
    if(!res)
    {
        cout << "file: " << g_confinfo.topn_sim_word << endl;
        cout << "initsimilarmap error!" << endl;
        return res;
    }
    else
    {
        cout << "file: " << g_confinfo.topn_sim_word << endl;
        cout << "mapSimilar : " << mapSimilar.size() << " items added!" << endl;
    }

    //map<int,map<int, double> > mapUserlabel;
    map<int,user_label_key_t> mapUserlabel;
    res = initUserlabelmap(mapUserlabel, mapKeyword, mapLabel, mapSimilar, g_confinfo.user_words_weight);
    if(!res)
    {
        cout << "file: " << g_confinfo.user_words_weight << endl;
        cout << "initUserlabelmap error!" << endl;
        return res;
    }
    else
    {
        cout << "file: " << g_confinfo.user_words_weight << endl;
        cout << "mapUserlabel : " << mapUserlabel.size() << " items added!" << endl;
    }

    res = outputresult(mapUserlabel, out_file);
    if(res)
    {
        cout << "result output ! check file \"" << out_file << "\"" << endl;
    }
    return res;
}

int main(int argc, char ** argv)
{
    struct timeval t_start;
    gettimeofday(&t_start, NULL);
    printf("t_start.tv_sec:%d\n", t_start.tv_sec);
    printf("t_start.tv_usec:%d\n", t_start.tv_usec);
    if(TEST_MODE)
    {
        cout << "main: argc= " << argc << " , argv[0]: " << argv[0] << endl;
    }
    if(1 == argc)
    {
        cout << "Welcome to "<<PRO_NAME<<", please input the output file path! " << endl
             << "More detail plese use command \"./" << PRO_CMD << " -help\" to find out!" << endl;

    }
    else if(2 == argc)
    {
        if(!strcmp(argv[1],"-help"))
        {
            cout << "SYNOPSIS" << endl
                << "\t./" << PRO_CMD << " [output path]" << endl
        		 << "OPTIONS" << endl
        		 << "\t -yu: the threshold value, the default value is 3" << endl
                 << "must contains the following option parameters:" << endl
                 << "\t./" << PRO_CMD << "-c [config file] [out file] " << endl
                 << "e.g." << endl
                 << "\t./" << PRO_CMD << " -yu 3 -c ./config.conf ./out.txt" << endl;
                 //<< "\t./" << PRO_CMD << " -k 3 -label 1 -in ./in.txt -out ./out.txt" << endl
                 //<< "\t./" << PRO_CMD << " -t -k 3 -label 1 -in ./in.txt -out ./out.txt" << endl;
        }
        else
        {
            /*char * out_file = argv[1];
            if(!userlabel(out_file))
            {
                cout << "user labels failed, please check your input parameter and the log file!" << endl;
            }*/
            cout << "error" << argc <<" option, please use \"./" << PRO_CMD << " -help\" for details!" << endl;
            for(int i = 0; i < argc; ++i)
            {
                cout << i << ":" << argv[i] << endl;
            }
        }
    }
    else if(4 <= argc && 6 >= argc)
    {
        char * out_file = argv[argc - 1];
        char * conf_file = NULL;
        for(int i = 0; i < argc;)
        {
            if(!strcmp(argv[i], "-yu"))
            {
                g_yu = atoi(argv[i + 1]);
                cout << "g_yu:" << g_yu << endl;
                i += 2;
            }
            else if(!strcmp(argv[i], "-c"))
            {
                conf_file = argv[i + 1];
                i += 2;
            }
            else
            {
                i += 1;
            }
        }
        if(!userlabel(out_file, conf_file))
        {
            cout << "user labels failed, please check your input parameter and the log file!" << endl;
        }
    }
    else
    {
        cout << "error" << argc <<" option, please use \"./" << PRO_CMD << " -help\" for details!" << endl;
        for(int i = 0; i < argc; ++i)
        {
            cout << i << ":" << argv[i] << endl;
        }
    }

    //test time
    //sleep(6);
    struct timeval t_end;
    gettimeofday(&t_end, NULL);

    if(TEST_MODE)
    {
        printf("t_start.tv_sec:%d\n", t_start.tv_sec);
        printf("t_start.tv_usec:%d\n", t_start.tv_usec);
        printf("t_end.tv_sec:%d\n", t_end.tv_sec);
        printf("t_end.tv_usec:%d\n", t_end.tv_usec);
    }
    time_t t = (t_end.tv_sec - t_start.tv_sec) * 1000000 + (t_end.tv_usec - t_start.tv_usec);
    cout << "start time :" << t_start.tv_sec << "." << t_start.tv_usec << endl
        << "end time :" << t_end.tv_sec << "." << t_end.tv_usec << endl
        << "using time : " << t_end.tv_sec - t_start.tv_sec << "."<< t_end.tv_usec - t_start.tv_usec << " s" << endl;
    return 0;
}
//}
