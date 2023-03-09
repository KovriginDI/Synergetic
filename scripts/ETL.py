from pyspark.sql import SparkSession 
import pyspark.sql.functions as F
import pandas as pd

PATH_FILE = ''
PATH_DATA = ''

spark = SparkSession.builder\
.master("local") \
.appName("App") \
.config("spark.default.parallelism", "10")\
.getOrCreate()

file1 = pd.read_excel(PATH_FILE + "01.2022 Метро химия+косметика ДЛЯ ТЕСТА.xlsx", header=[0,1])
file2 = pd.read_excel(PATH_FILE + "01.2023 Метро химия+косметика ДЛЯ ТЕСТА.xlsx", header=[0,1])

def df_column_labels(df):
    column_list = ['_'.join(col) for col in df.columns.values]
    df.columns = df.columns.droplevel()
    df.columns = column_list
    
    df = df.rename(columns={'Unnamed: 0_level_0_Unnamed: 0_level_1':'sales_point_id',
                        'Unnamed: 1_level_0_ТТ':'sales_point',
                        'Unnamed: 2_level_0_Регион':'region',
                        'Unnamed: 3_level_0_Город':'city',
                        'Unnamed: 4_level_0_Адрес':'address',
                        'Unnamed: 5_level_0_Код поставщика':'supplier_id',
                        'Unnamed: 6_level_0_Поставщик':'supplier_nm',
                        'Unnamed: 7_level_0_Категория':'category_nm',
                        'Unnamed: 8_level_0_Код sku':'sku_id',
                       'Unnamed: 9_level_0_Наименование sku':'sku_nm'})
    return df

def stuck_week(df):
    df = df.fillna(0)
    df = df[[col for col in df.columns if 'Total' not in col]]
    dim_list = [col for col in df.columns if 'CW' not in col]
    lst = [col for col in df.columns if 'CW' in col]
    week_list = [lst[i:i + 6] for i in range(0, len(lst), 6)]
    out_df = pd.DataFrame()
    for week_cols in week_list:
        df_fact = df[week_cols].astype(int)
        df_fact.columns = [c[10:] for c in list(df_fact.columns)]
        df_dim = df[dim_list]
        data = pd.concat([df_dim, df_fact], axis=1)
        data['year'] = int(week_cols[0][:4])
        data['week'] = int(week_cols[0][7:9])
        out_df = pd.concat([out_df, data])
    return out_df

df1 = stuck_week(df_column_labels(file1))
df2 = stuck_week(df_column_labels(file2))

df = pd.concat([df1, df2])

df.columns = [c.replace(" ", "_") for c in list(df.columns)]
df.columns = [c.replace("(", "") for c in list(df.columns)]
df.columns = [c.replace(")", "") for c in list(df.columns)]

df_spark = spark.createDataFrame(df)
df_spark.write.format('orc').save(PATH_DATA + 'raw/data')

data = spark.read.orc(PATH_DATA + "raw/data/")

category = data.select('category_nm').distinct().withColumn("category_id", F.monotonically_increasing_id())\
.select('category_id', 'category_nm')

data.join(category, on = 'category_nm', how='left').write.orc(PATH_DATA + "stg/data")

data = spark.read.orc(PATH_DATA + "stg/data/")

data.select('category_id', 'category_nm').distinct()\
.write.orc(PATH_DATA + "dds/data/category")

data.select('supplier_id', 'supplier_nm').distinct()\
.write.orc(PATH_DATA + "dds/data/supplier")

data.select('sku_id', 'category_id', 'sku_nm').distinct()\
.write.orc(PATH_DATA + "dds/data/sku")

data.select('sales_point_id', 'sales_point', 'city', 'region', 'address').distinct()\
.write.orc(PATH_DATA + "dds/data/sales_point")

data.selectExpr('year', 'week', 'sales_point_id', 'supplier_id', 'category_id', 'sku_id',
                    'Sales', 'SlsQty_Colli', 'SlsQty_Pc', 'SlsQty_Kg', 'Price_Colli_N_Min', 'Price_NNBP_Colli_N_Min')\
.write.orc(PATH_DATA + "dds/data/sales")
