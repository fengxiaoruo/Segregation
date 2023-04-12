import numpy as np
import pandas as pd
import time
from scipy.stats import entropy
import geopandas as gpd                                                                                                 #实现空间匹配
import matplotlib.pyplot as plt
import fiona as fna
from geopy.distance import geodesic as gd                                                                               #计算地理距离
from segregation.batch import batch_compute_multigroup



"""
导入数据，并将数据转化为geodf
"""

df = pd.read_csv('beijing_tianjin_seg_v3.csv', sep='\s+', encoding='utf-8')
df_border = df['border'].str.split(',', expand=True)

border_combine = "POLYGON((" + df_border[0]+" "+df_border[1]+ "," +df_border[2]+" "+df_border[3]+ "," +df_border[4]+" "+df_border[5]+ "," +df_border[6]+" "+df_border[7]+","+df_border[8]+" "+df_border[9]+ "))"
border_combine=border_combine.tolist()

import shapely.wkt
P=[]
for i in range(len(border_combine)):
    P.insert(i-1,shapely.wkt.loads(border_combine[i]))
p=gpd.GeoSeries(P)
df=df.drop(['border'], axis=1)
geo_df = gpd.GeoDataFrame(data=df,geometry=p)
geo_df.crs="epsg:3857"


df_code = pd.read_csv('county_list_merge.csv', sep='\s+', encoding='utf-8')                                             #读入区划代码
df_code.columns =['城市', '区县', 'proper_id']                                                                           #重命名
new_geodf = pd.merge(geo_df, df_code, how='inner', on=['城市', '区县'])                                                         #匹配
new_geodf['id'] = new_geodf.groupby(u'proper_id', as_index=False).ngroup()


"""
循环计算各市辖区的居住隔离指标
"""

multioutput_edu=pd.DataFrame()
multioutput_income=pd.DataFrame()
proper_df = pd.DataFrame()
proper_n = len(new_geodf['proper_id'].unique())


for idnum in range(proper_n):  # avoid 0/0

    proper_df = new_geodf[new_geodf['id'] == idnum]
    income_mul = batch_compute_multigroup(proper_df, groups=['2499及以下','2500~3999','4000~7999','8000~19999','20000及以上'],)
    edu_mul = batch_compute_multigroup(proper_df, groups=['高中及以下','大专','本科及以上'],)
    income_mul_tran = income_mul.T
    income_mul_tran.insert(loc=0, column="cityid",value= idnum )
    income_mul_tran.insert(loc=0, column="proper_id",value= proper_df['proper_id'].iloc[0])
    edu_mul_tran = edu_mul.T
    edu_mul_tran.insert(loc=0, column="cityid",value= idnum)
    edu_mul_tran.insert(loc=0, column="proper_id",value= proper_df['proper_id'].iloc[0])
    multioutput_income  = multioutput_income.append(income_mul_tran.copy())
    multioutput_edu = multioutput_edu.append(edu_mul_tran.copy())
    df_multioutput_income = pd.DataFrame(data=multioutput_income)
    df_multioutput_edu = pd.DataFrame(data=multioutput_edu)
    df_multioutput_income = pd.merge(df_multioutput_income, df_code,how='inner',on=['proper_id'] )
    df_multioutput_income = df_multioutput_income.sort_values(by=['proper_id'])
    df_multioutput_income.to_csv('segregation_index_city_proper_income_multi0208.csv', index=False)                                                 # 导出数据
    df_multioutput_edu = pd.merge(df_multioutput_edu, df_code,how='inner',on=['proper_id'] )
    df_multioutput_edu = df_multioutput_edu.sort_values(by=['proper_id'])
    df_multioutput_edu.to_csv('segregation_index_city_proper_edu_multi0208.csv', index=False)

