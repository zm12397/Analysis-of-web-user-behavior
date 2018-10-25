# Analysis-of-web-user-behavior
## 网站用户行为分析

​	本项目通过对网站用户购物行为数据集进行数据预处理、存储、查询和可视化分析等数据处理全流程所涉及的各种典型操作，获得最终的数据分析结果，运用Hadoop、HBase、Hive、Sqoop、Java&Python等技术，可作为数据分析案例。

## **分析结果**



## **数据集**

​	网站用户购物行为数据集2000万条记录。

​		user_id（用户id）
​		item_id(商品id)
​		behaviour_type（包括浏览、收藏、加购物车、购买，对应取值分别是1、2、3、4）
​		user_geohash(用户地理位置哈希值，有些记录中没有这个字段值，所以后面我们会用脚本做数据预处理时把这个字段全部删除)
​		item_category（商品分类）
​		time（该记录产生时间）

## **项目步骤**

### 步骤零：项目环境准备

 	1.搭建Hadoop分布式集群环境；本项目在vmware虚拟机中进行搭建，包括一个主节点与两个从节点；

​	2.安装HBase、Hive、Sqoop等hadoop生态系统组件；

​	3.宿主机安装Java、Python，以及Idea与PyCharm；

​	4.安装Python的常用库，如numpy、matplotlib、pandas等，同时为了绘制中国地图，额外安装Basemap；

### 步骤一：本地数据集上传到数据仓库Hive

​	所需知识储备：Linux系统基本命令、Hadoop项目结构、分布式文件系统HDFS概念及其基本原理、数据仓库概念及其基本原理、数据仓库Hive概念及其基本原理
  任务清单：1. 安装Linux系统；2. 数据集下载与查看；3. 数据集预处理；4. 把数据集导入分布式文件系统HDFS中；5. 在数据仓库Hive上创建数据库

​	本项目中使用的数据集已经上传至百度云盘https://pan.baidu.com/s/1nuOSo7B。

​	在dataset文件夹中，名为user.zip，解压后可得到一个大规模数据集raw_user.csv（包含2000万条记录），和一个小数据集small_user.csv（只包含30万条记录）。

#### 1.数据集预处理

​	预处理包括为每行记录增加一个id字段，删除无关字段user_geohash，增加省份字段，截取time字段得到yyyy-MM-dd格式的日期date等。预处理的操作通过Python开发完成，具体代码见WebUserBehaviorAnalysis目录下的ProDeal.py文件。

#### 2.导入数据库

​	下面要把user_table.txt中的数据最终导入到数据仓库Hive中。为了完成这个操作，我们会首先把user_table.txt上传到分布式文件系统HDFS中，然后，在Hive中创建一个外部表，完成数据的导入。

a.创建hive的外部表：

CREATE EXTERNAL TABLE bigdata_user(id INT,uid STRING,item_id 		STRING,behavior_type INT,item_category STRING,visit_date DATE,province STRING) 
​	ROW FORMAT DELIMITED
​	FIELDS TERMINATED BY '\t'
​	STORED AS TEXTFILE
​	LOCATION 'hdfs://master:9000//usr/zm/webuser/dataset';

b.把创建HDFS目录并从本地上传文件

​	dfs -mkdir /usr/zm/webuser;
​	dfs -mkdir /usr/zm/webuser/dataset;
​	dfs -copyFromLocal /mnt/hgfs/toCentOs/input/user_table.txt /usr/zm/webuser/dataset;

### 步骤二：Hive数据分析

​	所需知识储备	数据仓库Hive概念及其基本原理、SQL语句、数据库查询分析
训练技能	 数据仓库Hive基本操作、创建数据库和表、使用SQL语句进行查询分析
任务清单	1. 启动Hadoop和Hive；2. 创建数据库和表；3. 简单查询分析；4. 查询条数统计分析；5. 关键字条件查询分析；6. 根据用户行为分析；7. 用户实时查询分析

#### 1.操作Hive

a.查看建表信息

​	SHOW CREATE TABLE bigdata_user;

b.查看表结构

​	DESC bigdata_user;

#### 2.简单查询分析

a.查询前10条数据

​	SELECT * FROM bigdata_user LIMIT 10;

b.查询前20位用户购买商品的种类

​	SELECT item_category FROM bigdata_user LIMIT 10;

#### 3.查询条数统计分析

a.查询表内数据的数量

​	SELECT COUNT(*)  FROM bigdata_user;

b.查询表内的用户数量

​	SELECT COUNT(DISTINCE uid) FROM bigdata_user;

#### 4.关键字条件查询分析

a.查询2014年12月10日到2014年12月13日有多少人浏览了商品

​	SELECT COUNT(*) FROM bigdata_user WHERE behavior_type = 1 AND visit_date < '2014-12-13' AND visit_date > '2014-12-10';

b.以月的第n天为统计单位，依次显示第n天网站卖出去的商品的个数

​	SELECT COUNT(*),day(visit_date) FROM bigdata_user
​		WHERE behavior_type = 4
​		GROUP BY day(visit_date);

#### 5.用户实时查询分析

查询每个地区的用户浏览网站的次数

​	CREATE TABLE user_provice_scan
​		ROW FORMAT DELIMITED
​		FIELDS TERMINATED BY '\t'
​		STORED AS TEXTFILE
​		AS SELECT province,COUNT(behavior_type) FROM bigdata_user
​		WHERE behavior_type = 1 GROUP BY province;

### 步骤三：Hive、MySQL、HBase数据互导

  所需知识储备	数据仓库Hive概念与基本原理、关系数据库概念与基本原理、SQL语句、列族数据库HBase概念与基本原理

#### 1.使用Sqoop将数据从Hive导入MySQL

a.在hive中另建立user_action表，该表为内嵌表

​	CREATE TABLE user_action(id STRING,uid STRING,item_id STRING,behavior_type STRING,item_category STRING,visit_date DATE,province STRING)
​	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
​	STORED AS TEXTFILE;

b.填充user_action数据
​	INSERT OVERWRITE TABLE user_action
​		SELECT * FROM bigdata_user;

c.在Mysql中创建表user_action

​	DROP TABLE IF EXISTS `user_action`;
​	CREATE TABLE `user_action` (
  		`id` varchar(50) DEFAULT NULL,
  		`uid` varchar(50) DEFAULT NULL,
  		`item_id` varchar(50) DEFAULT NULL,
  		`behavior_type` varchar(10) DEFAULT NULL,
  		`item_category` varchar(50) DEFAULT NULL,
  		`visit_date` date DEFAULT NULL,
  		`province` varchar(20) DEFAULT NULL
​	) ENGINE=InnoDB DEFAULT CHARSET=utf8;

d.使用Sqoop从hive导入到mysql

​	sqoop export --connect jdbc:mysql://192.168.206.1:3306/hadoopguide --username hive -P --table user_action --export-dir /user/hive/warehouse/user_action --fields-terminated-by '\t

#### 2.使用Sqoop将数据从MySQL导入HBase

a.在hbase中创建表user_action

​	create 'user_action',{NAME => 'userinfo',VERSIONS => 5}

b.导入数据

​	sqoop import --connect jdbc:mysql://192.168.206.1:3306/hadoopguide --username hive -P --table user_action --hbase-table user_action --column-family userinfo --hbase-row-key id --hbase-create-table -m 1

c.查看数据

​	scan 'user_action',{LIMIT=>10}

#### 3.使用Hbase Java API将数据从本地文件系统导入到HBase中

​	编写Java程序，在hadoop项目中创建主类com.zfm.hadoop.web.analysis.HBaseImporter.java和com.zfm.hadoop.web.analysis.UserAction.java，将项目打包成hbase_import.jar，然后运行命令：

​	hadoop jar /mnt/hgfs/toCentOs/hbase_import.jar com.zfm.hadoop.web.analysis.HBaseImporter /mnt/hgfs/toCentOs/input/user_action/000000_0 user_action

​	具体代码详见Java\src\main\java\com\zfm\hadoop\web\analysis路径下的java文件。

### 步骤四：利用R进行数据可视化分析

  所需知识储备：数据可视化、Python语言

  #### 1.安装第三方库

​	这里安装了Numpy、pandas、matplotlib、Basema，具体安装步骤见Python目录下的安装指南。

### 2.分析消费者对商品的行为

(注：所有分析的完整代码详见Python\WebUserBehaviorAnalysis\AnalysisAndView.py)

```python
def behavior_analysis(self):
    # 要导入的sql语句，获取每种消费行为对应的数量
    behavior_sql = "SELECT behavior_type,COUNT(id) FROM user_action GROUP BY behavior_type"
    # 导入数据（直接导入到dataframe中）
    behaviors = pd.read_sql(behavior_sql, self.hadoopguide_connect)
    # 使用dataframe的绘图功能绘制柱状图
    behaviors.plot(kind='bar')
    plt.show()
```



#### 3.分析被购买次数最多的10个商品

```python
def popular_item(self):
    # 导入sql，获取被购买次数最多的10个商品以及被购买的次数
    top_ten_sql = "SELECT item_id,COUNT(id) num FROM user_action WHERE behavior_type = '4' " \
                  "GROUP BY item_id ORDER BY num DESC LIMIT 10 "
    top_ten = pd.read_sql(top_ten_sql,self.hadoopguide_connect)
    # 设置刻度，否则点图不按照已经排序好的顺序显示
    plt.xticks(arange(len(top_ten.item_id)),top_ten.item_id)
    plt.scatter(arange(len(top_ten.item_id)), top_ten.num)
    # plt.scatter(top_ten.item_id, top_ten.num)
    plt.show()
```



#### 4.分析每个月份的每种购买行为的数量

```python
def popular_month(self):
    # 导入sql，获取每个月的每种消费行为的数量
    per_month_sql = "SELECT DATE_FORMAT(visit_date,'%Y-%m') date,behavior_type,COUNT(id) num FROM user_action GROUP BY date,behavior_type"
    per_month = pd.read_sql(per_month_sql, self.hadoopguide_connect)
    # 由于数据库中behavior_type字段是字符串类型，这里将其转换为int8类型
    per_month['behavior_type'] = pd.DataFrame(data=per_month['behavior_type'], dtype=np.int8)
    # 将dataframe再按月份分组，将消费行为列与消费数量列合并为list
    per_month = per_month.groupby('date').agg({'behavior_type': lambda x: list(x), 'num': lambda x: list(x)})
    # 读取时由于将月份作为了索引，因此这里要再重置索引
    per_month = per_month.reset_index()

    # 获取figure对象，用来创建子图
    fig = plt.figure()

    # 遍历每个月份
    for i in per_month.index:
        # 获取每个月份的各个属性值
        date, behavior_type, num = per_month.loc[i, ['date', 'behavior_type', 'num']]
        # 添加一个1x2的组图的各个（i+1）子图
        fig.add_subplot(1, 2, (i + 1))
        fig.suptitle(date)
        # 在有组图的时候，每次绘图会绘制在当前的子图中
        plt.bar(behavior_type, num)
    plt.show()
```



#### 5.获取国内每个省份的购买量分布地图

```python
def province_distribution(self):
    # 导入sql，获取每个省的购买量
    per_province_sql = 'SELECT province,COUNT(id) num FROM user_action WHERE behavior_type = \'4\' GROUP BY province'
    per_province = pd.read_sql(per_province_sql,self. hadoopguide_connect)

    plt.figure(figsize=(16, 8))  # 定义地图大小
    # 设定中国的经纬度，不设置则是世界地图
    map = Basemap(llcrnrlon=77,
                  llcrnrlat=14,
                  urcrnrlon=140,
                  urcrnrlat=51,
                  projection='lcc',
                  lat_1=33,
                  lat_2=45,
                  lon_0=100)
    map.drawcountries(linewidth=1.25)   # 描绘国界线
    map.drawcoastlines()                # 描绘大陆板块线

    # 导入行政区划包，下载后放入resources文件夹中
    # 31个省、直辖市、自治区
    map.readshapefile('resources/gadm36_CHN_shp/gadm36_CHN_1', 'states', drawbounds=True)
    # 香港特别行政区
    map.readshapefile('resources/gadm36_HKG_shp/gadm36_HKG_0', 'hongkong', drawbounds=True)
    # 澳门特别行政区
    map.readshapefile('resources/gadm36_MAC_shp/gadm36_MAC_0', 'macao', drawbounds=True)
    # 台湾省
    map.readshapefile('resources/gadm36_TWN_shp/gadm36_TWN_0', 'taiwan', drawbounds=True)

    # 渲染 获取红黄色调
    cmap = plt.cm.YlOrRd

    colors = {}         #给每个省保存颜色rbg的字典
    statenames = []     #省列表


    # 先遍历31个省、直辖市、自治区
    for shapedict in map.states_info:
        # 获取省的中文名
        statename = shapedict['NL_NAME_1']
        p = statename.split('|')
        if len(p) > 1:
            s = p[1]
        else:
            s = p[0]
        # 将省的中文名解析为数据库中使用的省名
        s = self.parse_province(s)
        statenames.append(s)
        # 找到该省对应的购买量
        value = per_province[per_province.province == s].num
        # 通过cmap函数将购买量转换为RGB值
        colors[s] = cmap(value)[:3]

    # 获取坐标轴 gca： Get Current Axes
    axis = plt.gca()

    # 再遍历每个省
    for nshape, seg in enumerate(map.states):
        # statenames[nshape]可以得到省名，根据省名获取字典colors中的rgb值，获得对应的color
        color = rgb2hex(colors[statenames[nshape]][0])
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        axis.add_patch(poly)

    plt.show()
```







