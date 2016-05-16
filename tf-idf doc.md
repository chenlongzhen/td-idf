#tf-idf
针对用户的微博内容进行用户的关键词提取，作为每个用户打标签的数据基础。
tf-idf原理参见[百度百科](http://baike.baidu.com/link?url=LxalR1Ll2ZeEKPEV6tQ5Vg9bEyt8sQz4MllseTl1PeDCK7yAINXrNfrlaBtIpCHcugyQKJbJ8usfWp8h9WVXL_)

#项目实现流程
整个项目实现流程主要有三步：
1. 遍历data文件夹下的所有最后一次调用时间戳后的id_post（id和微博内容）新文件，计算tf和idf，单独存入本地data/tf和idf下（一个id_post文件对应一个tf和idf文件）
2. 汇总每个idf文件成为一个最终的idf dict{word：frequency}，这里idf计算公式为：文档总数/该词出现文档数，由于文档总数是固定值不会影响到最终的tf-idf排名topK，所以将文档总数设为1.那么在计算tf-idf时只需：tf/frequency。
3. 计算tf-idf，取出topK。

![这里写图片描述](http://img.blog.csdn.net/20160516145501598)

# 项目结构
结构如下tree图，src/tf_idf.py 为代码，data目录下存储本地文件，log/日志文件

```
|-- data
|   |-- id_post
|   |-- idf
|   |-- tf
|   |-- tf_idf
|   |-- timestamp_process
|   |-- timestamp_tf_idf
|   `-- word_dict
|-- log
|-- src
|   |-- nohup.out 
|   `-- tf_idf.py
```

#数据格式以及调用方式
输入数据放在data/id_post下，格式：

```
9955218 #武汉城建#【武汉地铁5号线进行环评公示 明年8月开工】记者从中铁第四勘察设计院获悉，武汉轨道交通5号线已启动第二次环评，并计划于明年8月正式开工建设。该线全长33.57km，共设站25座，其中高架站6座，地下站19座。http://t.cn/RyVENo0
9955218 #今日土拍回顾#【保利底价拿卓刀泉地块 经开地块竞价】经历过2月延期、3月流拍的卓刀泉、白沙洲三宗打包地块将重新挂牌现场拍卖，三宗地块总价比之前便宜了1.5亿。除了三宗卓刀泉、白沙洲打包地块，本次还有包括经开区、江夏、黄陂、新洲等在内的十宗地块网上挂牌拍卖。http://t.cn/RyVEf8F
9955218 #楼市嘚吧嘚#【王石田朴珺订婚很嗨 武汉楼市这是嗨完空虚?】这真的很诡异！恩，说的是王石和田朴珺！原以为《挑战者联盟》这一播吧，范冰冰和李晨的花式虐狗大法又是妥妥地“艳压全网CP”。谁知一上网竟全是这两货买婚床的消息……详情猛戳：http://t.cn/RyVVC4M
```
最终输出为：

```
972760 是 0.0021494370522
972760 游戏 0.00162060730126
972760 郑化锋 0.00153531218015
```

调用方式及参数为：

```
1. -t --topK tf-idf的topK，默认150
2. -p --processes 多进程处理的进程数，默认3
3. -w --weight bool型 是否输出权重，默认1
4. -s -step int型 0:从计算tf和idf到tf-idf整个流程；1:只process文件，计算tf和idf存入本地；2:读取已经处理好的tf和idf文件计算tf-idf，默认0
```
调用例子
```
python ./tf_idf.py -s 0 -t 100 -p 3 -w 1
```


