# 数据分析项目

### 1.指定环境打包
~~~
mvn clean install -Ptest -- 打包test环境
mvn clean install -Ppro  -- 打包pro环境
mvn -f common clean install -Ptest -- 打包test环境common模块
~~~
### 2.window 运行
~~~
1.下载hadoop源码
http://archive.apache.org/dist/hadoop/core/hadoop-3.0.0

2.配置 Environment variables 或者 HADOOP_HOME  环境变量
HADOOP_HOME=..\hadoop-3.0.0

3.将 ..\doc\bin 目录下的文件 复制到 $HADOOP_HOME\bin\ 目录下
~~~
### 3.模块说明
~~~
common: 公用模块，用于不同环境配置和工具类
doris: doris任务
jobs: 离线任务(spark 离线任务，没有用)
stream: 离线任务(flink 实时任务,没有用)
doc : 文档,项目依赖等
~~~