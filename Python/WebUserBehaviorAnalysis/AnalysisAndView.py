import matplotlib.pyplot as plt
from matplotlib.colors import rgb2hex
import pandas as pd
from mpl_toolkits.basemap import Basemap
import pymysql
import numpy as np
from datetime import datetime
from datetime import timedelta
from pylab import *

class AnalysisViewer:
    hadoopguide_connect = pymysql.connect(
        host="localhost",
        port=3306,  # 注意这里不能是字符串
        user="root",
        passwd="root",
        db="hadoopguide",
        charset="utf8"
    )

    # 1.分析消费者对商品的行为
    def behavior_analysis(self):
        # now1 = datetime.now()
        # behavior_sql = "SELECT behavior_type FROM user_action"
        # 要导入的sql语句，获取每种消费行为对应的数量
        behavior_sql = "SELECT behavior_type,COUNT(id) FROM user_action GROUP BY behavior_type"
        # 导入数据（直接导入到dataframe中）
        behaviors = pd.read_sql(behavior_sql, self.hadoopguide_connect)
        # behaviors['num'] = 0
        # plt_behaviors = behaviors.groupby('behavior_type').agg({'num':'count'})
        # plt_behaviors.plot(kind='bar')
        # 使用dataframe的绘图功能绘制柱状图
        behaviors.plot(kind='bar')
        # now2 = datetime.now()
        # print((now2 - now1).seconds)
        plt.show()

    # 2.分析被购买次数最多的10个商品
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

    # 3.分析每个月份的每种购买行为的数量
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
        fig.suptitle('Per_month behavior analysis')

        # 遍历每个月份
        for i in per_month.index:
            # 获取每个月份的各个属性值
            date, behavior_type, num = per_month.loc[i, ['date', 'behavior_type', 'num']]
            # 添加一个1x2的组图的各个（i+1）子图
            ax = fig.add_subplot(1, 2, (i + 1))
            ax.set_title(date)
            # 在有组图的时候，每次绘图会绘制在当前的子图中
            plt.bar(behavior_type, num)
        plt.show()

    # 4.获取国内每个省份的购买量分布地图
    def province_distribution(self):
        # 导入sql，获取每个省的购买量
        per_province_sql = 'SELECT province,COUNT(id) num FROM user_action WHERE behavior_type = \'4\' GROUP BY province'
        per_province = pd.read_sql(per_province_sql,self. hadoopguide_connect)

        plt.figure(figsize=(16, 8))  # 定义地图大小
        plt.title('China province consumption amount distribution')
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

    # 解析省名
    def parse_province(self,province):
        city_name = ['北京', '重庆', '上海', '天津']
        region_name = ['新疆维吾尔自治区', '广西壮族自治区', '宁夏回族自治区', '西藏自治区']
        specail_name = ['内蒙古自治区', '黑龍江省']
        if (province not in city_name and province not in region_name and province not in specail_name):
            return province
        elif province in city_name:
            return province + '市'
        elif province in region_name:
            return province[0:2]
        elif province == '内蒙古自治区':
            return '内蒙古'
        elif province == '黑龍江省':
            return '黑龙江'

if __name__ == '__main__':
    analysisViewer = AnalysisViewer()
    # analysisViewer.behavior_analysis()
    # analysisViewer.popular_item()
    # analysisViewer.popular_month()
    analysisViewer.province_distribution()