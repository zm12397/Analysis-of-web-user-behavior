import pandas as pd
import random



class UserDataOperator:
    'be used to operate user data such as small_user.csv and raw_user.csv'
    # 变量声明
    dataSetPath = "E:\\Temp\\toCentOs\\input\\raw_user.csv"  # 输入Path small_user
    dataTablePath = "E:\\Temp\\toCentOs\\input\\user_table.txt"  # 输出Path
    naValues = {'null', 'NULL', ''}  # 空值判断列表
    engine = 'python'  # 导入引擎
    encoding = 'utf-8'  # 指定编码
    sep = '\t'  # 指定分隔符
    provice = ["山东", "山西", "河南", "河北", "陕西", "内蒙古", "上海市", "北京市", "重庆市", "天津市", "福建", "广东", "广西", "云南",
               "浙江", "贵州", "新疆", "西藏", "江西", "湖南", "湖北", "黑龙江", "吉林", "辽宁", "江苏", "甘肃", "青海", "四川", "安徽",
               "宁夏", "海南", "香港", "澳门", "台湾"]

    def init(self,uri,na,eng,encoding):
        #uri:"E:\Temp\toCentOs\input\small_user.csv"
        #na:{'null', 'NULL', ''}
        #eng:'python'
        self.user_data = pd.read_csv(uri,na_values=na,engine=eng,encoding = encoding)

    # def rm_line(self,i):
    def describe(self):
        print(self.user_data)

    # 该方法用作df.apply的参数，针对每一行数据，返回一个随机省份
    def getRandomProvince(self):
        return self.provice[random.randint(0, 33)]

    def getSubString(self,str):
        return str[0:10]

    # 数据导出csv并保存
    def toCsv(self,uri,index,encoding,sep):
        # index指明是否将索引也存入 header为none表示不写入表头
        self.user_data.to_csv(uri,index = index,header = None,encoding = encoding,sep = sep)



if __name__ == '__main__':
    userDataOperator = UserDataOperator()  # 初始化 数据操作对象
    # 对象初始化
    userDataOperator.init(userDataOperator.dataSetPath, userDataOperator.naValues, userDataOperator.engine, userDataOperator.encoding)

    # 删除user_geohash字段，后面不需要该字段
    userDataOperator.user_data.drop(['user_geohash'], axis=1, inplace=True)

    # 添加名为id的新列，用作主键区分 | 或者直接不用生成该列，使用dataframe的index导出即可
    # userDataOperator.user_data['id'] = userDataOperator.user_data.index

    # 添加名为省份的新列，用随机值生成对应的省份填充
    userDataOperator.user_data['provice'] = userDataOperator.user_data.apply(
        lambda x: userDataOperator.getRandomProvince(), axis=1)

    # 对原来的time列作处理：由yyyy-MM-dd hh 转换为yyyy-MM-dd
    userDataOperator.user_data['time'] = userDataOperator.user_data.apply(
        lambda x: userDataOperator.getSubString(x.time), axis=1)

    # userDataOperator.describe()
    userDataOperator.toCsv(userDataOperator.dataTablePath, True, userDataOperator.encoding, userDataOperator.sep)




