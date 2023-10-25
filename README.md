Hi Kiran,

As discussed in last call, we are going through existing classification jobs and also identifying the optimized solution and tagging logic. Currently we went through the RTGS and classification framework for the same, other mode of transaction classification.

We will send you the detailed report of existing framework by next Wednesday and Friday we can showcase the demo.




--------------------------------------------------------------------------------
Self Transfer -

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.column import Column, _to_java_column, _to_seq
'''
def compare_names(name1, name2):
    try:
        name_set1 = set(name1)
        name_set2 = set(name2)
        if name_set1 >= name_set2 or name_set2 >= name_set1:
            return 1
        else:
            return 0
    except:
        return 0
'''

def check_subset_udf(sc, col1, col2):
    _test_udf = sc._jvm.org.ybl.apps.subset.ScalaPySparkUDFs.checkSubset()
    return Column(_test_udf.apply(_to_seq(sc, [col1,col2], _to_java_column)))

def getSelfTransferFlag(sc, df, r_name, b_name, self_transfer_col):
    '''
    df => 'Transaction data frame'
    r_name => 'column containing remitter_name'
    b_name => 'column containing benef_name'
    self_transfer_col => 'output column to contain self transfer flag 0/1'
    '''
    df2 = df.withColumn(r_name + '_list', F.split(F.lower(F.col(r_name)), '\s+'))
    df3 = df2.withColumn(b_name + '_list', F.split(F.lower(F.col(b_name)), '\s+'))

    df4 = df3.withColumn(self_transfer_col, check_subset_udf(sc, F.col(r_name + '_list'), F.col(b_name + '_list')))
    df4 = df4.withColumn(self_transfer_col, F.col(self_transfer_col).cast(T.StringType()) )
    return df4.drop(r_name + '_list', b_name + '_list')

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

POSFW.Py

import os, io, re, csv
import string
from collections import Counter
from itertools import permutations,chain
import time
#from textutils.viktext import KeywordProcessor
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
import pyspark
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window
import ConfigParser
import sys


class PosFW:
    def __init__(self, kp):
        self.kp = kp
        
    def get_mcc_detail(self, ca_name):
        words = self.kp.value.extract_keywords(ca_name)
        if(len(words)==1):
            return words[0]
        elif(len(words)>1):
            return sorted(words, key=len, reverse=True)[0]
        else:
            return ''
    
    def registerudf(self):
        return F.udf(self.get_mcc_detail, T.StringType())

    
def pycleaner(text, regex_list):
    for reg in regex_list:
        text = re.sub(reg, ' ', text)
    return text.strip()

def get_one_to_many_category_map2(file_name, filters, clean_regex_list):
    one_many_map = {}
    with open(file_name, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for i, row in enumerate(spamreader):
            if i == 0:
                row = [x.upper() for x in row]
                entity_index = row.index('ENTITY_ID')
                keyword_index = row.index('KEY')
                cat_code_index = row.index('CATEGORY_CODE')
                channel_key = row.index('CHANNEL')
            
            elif row[channel_key] in filters:
                k1 = pycleaner(row[keyword_index], clean_regex_list)
                k2 = pycleaner(row[entity_index], clean_regex_list)
                channel = row[channel_key]
                v = row[cat_code_index]
                if k2 not in one_many_map:
                    one_many_map[k2] = []
                one_many_map[k2].append(v)
    
    conflict_dict = {}
    for ele in one_many_map:
        if len(list(set(one_many_map[ele]))) > 1:
            conflict_dict[ele] = list(set(one_many_map[ele]))

    return conflict_dict  


def get_keywords_from_csv(file_name, filters, clean_regex_list):
    mapper_dict = {}
    regex_list = []
    conflict_dict = get_one_to_many_category_map2(file_name, filters, clean_regex_list)
    with open(file_name, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for i, row in enumerate(spamreader):
            if i == 0:
                row = [x.upper() for x in row]
                entity_index = row.index('ENTITY_ID')
                keyword_index = row.index('KEY')
                cat_code_index = row.index('CATEGORY_CODE')
                channel_key = row.index('CHANNEL')
            
            elif row[channel_key] in filters:
                v11 = None
                v1 = row[keyword_index]
                if '@' in v1:
                    v11 = v1.split('@')[0]
                
                v2 = pycleaner(row[entity_index].lower(), clean_regex_list)
                if v1.startswith('REGEX::'):
                    v1 = v1.split('::')[1]
                    regex_list.append([v1, row[cat_code_index] +'|'+ row[entity_index].lower()])
                else:
                    v1 = pycleaner(v1.lower(), clean_regex_list)
                    
                    k = row[cat_code_index] + '|' + row[entity_index].lower()
                    if k not in mapper_dict:
                        mapper_dict[k] = []
                    if v1 not in mapper_dict[k] and len(v1) > 1:
                        mapper_dict[k].append(v1)
                        v1_s = v1.replace(' ','')
                        if v1_s != v1:
                            mapper_dict[k].append(v1_s)
                        
                    if v11:
                        v11 = pycleaner(v11.lower(), clean_regex_list)
                        if v11 not in mapper_dict[k] and len(v11) > 1 and v11 not in conflict_dict:
                            mapper_dict[k].append(v11)
                            v11_s = v11.replace(' ', '')
                            if v11_s != v11:
                                mapper_dict[k].append(v11_s)
                    
                    if v2 not in mapper_dict[k] and len(v2) > 1 and v2 not in conflict_dict:
                        mapper_dict[k].append(v2)
                        v2_s = v2.replace(' ','')
                        if v2_s != v2:
                            mapper_dict[k].append(v2_s)
                        
    return mapper_dict




def get_keywords_from_csv_old(file_name, filters, regex_list_clean):
    mapper_dict = {}
    regex_list = []
    with open(file_name, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for i, row in enumerate(spamreader):
            if i == 0:
                row = [x.upper() for x in row]
                entity_index = row.index('ENTITY_ID')
                keyword_index = row.index('KEY')
                cat_code_index = row.index('CATEGORY_CODE')
                channel_key = row.index('CHANNEL')
            
            elif row[channel_key] in filters:
                v1 = row[keyword_index]
                v2 = pycleaner(row[entity_index].lower(), regex_list_clean)

                if v1.startswith('REGEX::'):
                    v1 = v1.split('::')[1]
                    regex_list.append([v1, row[cat_code_index] +'|'+ row[entity_index].lower()])
                else:
                    v1 = pycleaner(v1.lower(), regex_list_clean)
                    k = row[cat_code_index] + '|' + row[entity_index].lower()
                    if k not in mapper_dict:
                        mapper_dict[k] = []
                    if v1 not in mapper_dict[k] and len(v1) > 1:
                        mapper_dict[k].append(v1)
                    #if v2 not in mapper_dict[k] and len(v2) > 1:
                    #    mapper_dict[k].append(v2)
    return mapper_dict



def initialize(root_path, sc, data_list = ['POS'], regex_list= [r'[^A-Za-z]+', r'\bNULL\b', r'\s+'], fname = '/Transaction-Classification/MasterData/FINAL_MAPPER_DATA.csv'):
    """
    root_path -> directory where Transaction-Classification folder is present
    sc -> spark context
    data_list -> list of beneficiary name datasets from the mapper table, contains these types at max: ['EPI', 'POS', 'crowdsource', 'DD', 'Cheques', 'UPI', 'NACH'], default value: ['POS']
    regex_list -> list of regular expressions for cleanup, default vaule: [r'[^A-Za-z\&]+', r'\bNULL\b', r'\s+']
    data_csv -> path to mapper file in csv format, default_value: './Transaction-Classification/MasterData/FINAL_MAPPER_DATA.csv'
    """
    sc.addPyFile(root_path + '/Transaction-Classification/textutils/viktext.py')    
    from viktext import KeywordProcessor
    
    fname = root_path + fname
    result_dict = get_keywords_from_csv(fname, data_list, regex_list)
    kp=KeywordProcessor()
    kp.add_keywords_from_dict(result_dict)
    return kp
    
def textcleaner2(df, col_name, regex_list = [r'[^A-Za-z]+', r'\bNULL\b', r'\s+'], i = 0):
    if i == len(regex_list):
        return df.withColumn('__cat_merchant__', F.upper(F.trim(col_name)))    
    else:
        funct = F.regexp_replace(col_name, regex_list[i], ' ')
        return textcleaner2(df, col_name = funct, i = i+1)
    
def get_mcc_tab(sqlContext, cat_col):
    return sqlContext.read.csv("/user/sapd044172/mlearning/mcc_master.csv", header = True) \
           .select(F.trim(F.col('cod_merch_typ')).alias(cat_col), F.trim(F.col('category_code')).alias('__mcc_cat__') )
       
def get_pos_txn_cat(sc, sqlContext, dframe, name_col, cat_col, 
                    entity_col, category_code_FW, kp, 
                    regex_list = [r'[^A-Za-z]+', r'\bNULL\b', r'\s+']):
    '''
    sc -> Spark context
    sqlContext -> sqlContext
    dframe -> data frame contaning POS transactions
    name_col -> name of column containing Merchant name
    cat_col -> column name contaning MCC categories
    entity_col -> output column name for entity id which is currently same as normalized entity name
    category_code_FW -> output column name for transaction category code
    kp -> keyword processor object contaning keyword dictonary
    regex_list -> List of regular expressions for cleanup, default value: [r'[^A-Za-z]+', r'\bNULL\b', r'\s+']
    '''
    kp_broadcast = sc.broadcast(kp)
    
    pfw = PosFW(kp_broadcast)
    udf_for_cat = pfw.registerudf()
    
    clean_pos = textcleaner2(dframe, F.col(name_col))
    clean_pos = clean_pos.fillna('NA', subset=['__cat_merchant__'])

    pos_names_clean1 = clean_pos.select(F.col('__cat_merchant__')).dropDuplicates(['__cat_merchant__'])

    pos_names_clean=(pos_names_clean1.withColumn("cat_merchant_",udf_for_cat(F.col('__cat_merchant__'))))
    
    split_col=F.split(pos_names_clean['cat_merchant_'],'\|')
    pos_names_clean2=pos_names_clean.withColumn("__cat_code__",split_col.getItem(0)).withColumn(entity_col, split_col.getItem(1))
    pos_names_clean2 = pos_names_clean2.fillna('', subset=[entity_col])
    mcc_tab=get_mcc_tab(sqlContext, cat_col)
    pos_names_clean3=clean_pos.join(pos_names_clean2,'__cat_merchant__','left').join(mcc_tab, cat_col,'left')
    
    abc=(pos_names_clean3.withColumn(category_code_FW, F.when((F.col('__cat_code__') 
                                                               .isNotNull() & 
                                                               ~F.col('__cat_code__').isin('') & 
                                                               ~F.col('__cat_code__').isin('170000')
                                                              ),F.col('__cat_code__'))
                                 .otherwise(F.when((F.col('__mcc_cat__').isNotNull() 
                                                    & ~F.col('__mcc_cat__').isin('')
                                                   ),F.col('__mcc_cat__'))
                                .otherwise(F.lit("170000")))))
    temp_cols = ['__cat_code__', '__mcc_cat__', 'cat_merchant_', '__cat_merchant__']
    abc1=abc.drop('__cat_code__', '__mcc_cat__', 'cat_merchant_', '__cat_merchant__')
    return abc1
    

if __name__ == '__main__':
    sc = SparkContext()
    try:
        # Try to access HiveConf, it will raise exception if Hive is not added
        sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        sqlContext = HiveContext(sc)
    except py4j.protocol.Py4JError:
        sqlContext = SQLContext(sc)
    except TypeError:
        sqlContext = SQLContext(sc)
    
    sc.addPyFile('POSFramework0.py')
    from POSFramework0 import *
    kp = initialize(sc)
    
    pos_table_loc="db_stage.stg_fcr_fcrlive_1_xf_ol_st_postxn_mmdd"
    pos_tab=sqlContext.read.table(pos_table_loc)
    final_tab = get_pos_txn_cat(sc, sqlContext, pos_tab,'ca_name','cod_merch_typ','entity_id','category_code_FW', kp)
    final_tab.count()
---------------
NEFT.py

# coding: utf-8

# In[2]:

import pandas as pd
from nltk.util import skipgrams

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.ml import feature as MF
from dateutil import relativedelta
from pyspark.sql.functions import unix_timestamp, lit
import datetime



# In[ ]:


sc = SparkContext()
sc.setCheckpointDir('/tmp/spark-code-neft')

try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)


# In[3]:

t1 = 'db_smith.smth_pool_base_neft'
t2='db_stage.stg_fle_category_master'


# In[4]:

import ConfigParser
import sys
configFile = sys.argv[1]
#configFile = '/data/08/notebooks/tmp/Anisha/TransactionClassification/smth_pool_neft_20190222195838_python.ini'
config = ConfigParser.ConfigParser()
config.read(configFile)


# In[5]:

data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY',)
data_dt = data_dtt.strip('"').strip("'")
t1 = config.get('default','INP_DB_NM_1') + '.' +      config.get('default','INP_TBL_NM_1')


t2 = config.get('default', 'INP_DB_NM_2') + '.' +      config.get('default','INP_TBL_NM_2')
t2_batch = config.get('default', 'END_BATCH_ID_2')


output_tbl=config.get('default', 'OUT_DB_NM') + '.' +             config.get('default','OUT_TBL_NM')
#data_dtt='2019-08-11'


# In[6]:
neft_tbl_df=sqlContext.table(t1)
neft_data=(neft_tbl_df.filter((F.col('data_dt')>=( datetime.datetime.strptime(data_dt, "%Y-%m-%d")-relativedelta.relativedelta(days=7)))
                        #&(F.col('data_dt')<=str(data_dtt))
						).drop('benef_id','category_code'))


# In[7]:

category_master=sqlContext.table(t2)

def replaceNull(df, col_list,default_value=''):
    for col in col_list:
        df = df.withColumn(col,F.when(F.col(col).isNull(),default_value).otherwise(F.col(col)))
    return df

col_list1 = ['base_txn_text','benef_nickname','derived_txn_txt','rmtr_to_bene_note']
neft_data = replaceNull(neft_data,col_list1)


# In[8]:

neft_data1=(neft_data.withColumn('Remarks',(F.upper(F.concat((F.regexp_replace(F.col('base_txn_text'),'(\d+)','')),
                                                           #F.lit(' '),
                                                           #(F.regexp_replace(F.col('derived_txn_txt'),'(\d+)','')),
                                                           F.lit(' '),
                                                           (F.regexp_replace(F.col('rmtr_to_bene_note'),'(\d+)','')))))))


# In[9]:

neft_data2=neft_data1.withColumn('Remarks1',F.concat(F.col('Remarks'),F.lit(" "),F.col('benef_nickname'))).drop(F.col('benef_id')).drop(F.col('self_flag'))


# In[10]:

root_path = '/ybl/dwh/artifacts/sherlock/pythonJobs'

sc.addPyFile(root_path + '/Transaction-Classification/EntityFW.py')
from EntityFW import *
kpp, regex_list = initilize_keywords(root_path,sc,['NACH', 'crowdsource', 'DD', 'Cheques', 'COMMON','TRANSFERS','Ecollect'])
sc.addPyFile(root_path +'/Transaction-Classification/RemarksFW.py')
from RemarksFW import *
R_kp, R_result_dict = R_initialize(root_path,sc)
sc.addPyFile(root_path +'/Transaction-Classification/RemarkEntityWrapper.py')
from RemarkEntityWrapper import *

df_res = ApplyFWSequence(root_path,sc,neft_data2,'benef_name','Remarks1', 'category_code', 'benef_id',R_kp, R_result_dict,kpp,
                         regex_list,'rb','510000', remit_col='remitter_name', self_tnfr_col='self_flag')


# In[11]:

purpose_code=sc.parallelize([('PC01','410000'),
('PC02','410000'),
('PC03','110000'),
('PC04','110000'),
('PC05','350200'),
('PC06','190000'),
('PC07','390000'),
('PC08','290400'),
('PC09','290000'),
('PC10','230000'),
('PC11','150000'),
('PC12','160000'),
('PC13','120001'),
('PC31','410000')]).toDF(['code','category_code1'])


# In[12]:

df_res1=df_res.join(purpose_code,'code','left')


# In[20]:

df_res2=df_res1.withColumn('category_code',F.when((F.col('category_code').isin('510000'))
                                                  &(~F.col('category_code1').isNull()),F.col('category_code1'))
                                                                                       .otherwise(F.col('category_code')))


# In[21]:

df_res3=df_res2.withColumn('benef_name',F.when((F.col('benef_id').isNull()|F.col('benef_id').isin('')),F.col('benef_name')).otherwise(F.col('benef_id')))


# In[22]:

neft_final=df_res3.join(category_master,'category_code','left')


# In[24]:

neft_final1=neft_final.select(F.col("txn_ref_no"),
F.col("txn_date"),
F.col("txn_amt"),
F.col("mode"),
F.col("remitter_id"),
F.col("remitter_name"),
F.col("remitter_type"),
F.col("remitter_class"),
F.col("remitter_sub_class"),
F.col("remitter_ifsc"),
F.col("remitter_bank_name"),
F.col("remitter_account_no"),
F.col("remitter_cust_id"),
F.col("benef_id"),
F.col("benef_name"),
F.col("benef_type"),
F.col("benef_class"),
F.col("benef_sub_class"),
F.col("benef_ifsc"),
F.col("benef_bank_name"),
F.col("benef_account_no"),
F.col("benef_cust_id"),
F.col("base_txn_text"),
F.col("online_offline"),
F.col("category_level1"),
F.col("category_level2"),
F.col("category_level3"),
F.col("category_code"),
F.col("recurrance_flag"),
F.col("recurrance_pattern_id"),
F.col("verification_flag"),
F.col("self_flag"),
F.col("txn_ref_key"),
F.col("channel_code"),
F.col("codstatus"),
F.col("acctstatus"),
F.col("msgstatus"),
F.col("codcurr"),
F.col("datvalue"),
F.col("direction"),
F.col("msgtype"),
F.col("submsgtype"),
F.col("benef_nickname"),
F.col("iduserreference_2020"),
F.col("idrelatedref_2006"),
F.col("reasoncode_6346"),
F.col("rejectreason_6366"),
F.col("derived_txn_txt"),
F.col("rmtr_to_bene_note"),
F.col("channel"),
F.col("I_C"),
F.col('data_dt'))


# In[ ]:


output_cols = sqlContext.table(output_tbl).columns


# In[46]:

to_fill = neft_final1.columns

res = neft_final1
for x in output_cols:
    if x  not in to_fill:
        res = res.withColumn(x, F.lit(''))

res_to_write = res.select(output_cols)



# In[ ]:

res_to_write.write.insertInto(output_tbl, True)

---------------------------------------------------------------------------------------

UPI 

# coding: utf-8

# In[1]:

import pandas as pd
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.ml import feature as MF
from dateutil import relativedelta
import datetime
import ConfigParser
import sys


# In[2]:

#sc.applicationId


# In[ ]:

sc = SparkContext()
#sc.setCheckpointDir('/tmp/spark-code-neft')

try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)


# In[4]:

configFile = sys.argv[1]
#configFile = "/data/08/notebooks/tmp/Anisha/Pooling_table/pool_dc.ini"
config = ConfigParser.ConfigParser()
config.read(configFile)
data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY',)
data_dt = data_dtt.strip('"').strip("'")
#data_dt='2019-08-19'
#cust_mst_tbl_name = config.get('contactability','cust_mst_tbl_name')
upi_tbl = config.get('default','INP_DB_NM_1') + '.' +  config.get('default','INP_TBL_NM_1')
upi_tbl_df=sqlContext.table(upi_tbl)
upi_tbl1= (upi_tbl_df.filter((F.col('data_dt')>=( datetime.datetime.strptime(data_dt, "%Y-%m-%d")-relativedelta.relativedelta(days=7)))
                        #&(F.col('data_dt')<=str(data_dt))
						).withColumn('payee_add_2', F.split(F.col('payee_addr'),'[\@]')[0])
           .drop('benef_id','self_flag'))
category_mast_tbl = config.get('default', 'INP_DB_NM_2') + '.' +  config.get('default','INP_TBL_NM_2')
category_master = sqlContext.table(category_mast_tbl).drop('batch_id')

#upi_merch_tbl = config.get('default', 'INP_DB_NM_3') + '.' +  config.get('default','INP_TBL_NM_3')
upi_merch_tbl = 'tempdb.temp_upi_classified_marchant_19072019'
upi_merch = sqlContext.table(upi_merch_tbl).filter(~(F.col('category_code').isin('510000'))).dropDuplicates(['Handle'])

out_tbl = config.get('default', 'OUT_DB_NM') + '.' +  config.get('default','OUT_TBL_NM')
out_table = sqlContext.table(out_tbl)


# In[30]:

#upi_a=sqlContext.table('db_smith.smth_pool_base_upi').filter(F.col('data_dt')>=('2019-04-01')) \
                               # .withColumn('payee_add_2', F.split(F.col('payee_addr'),'[\@]')[0]).drop('benef_id')


# In[5]:

rootpath = '/ybl/dwh/artifacts/sherlock/pythonJobs'
#rootpath='.'
sc.addPyFile( rootpath+'/Transaction-Classification/EntityFW.py')
from EntityFW import *
sc.addPyFile( rootpath+'/Transaction-Classification/RemarksFW.py')
from RemarksFW import *

kpp, regex_list = initilize_keywords(rootpath, sc, ['UPI','POS','Ecollect'], regex_list=['\s+'])
kpp_b, regex_list_b = initilize_keywords(rootpath, sc, ['POS','COMMON'])
R_kp, R_result_dict = R_initialize(rootpath,sc)


# In[6]:

sc.addPyFile(rootpath+'/Transaction-Classification/RemarkEntityWrapper.py')
from RemarkEntityWrapper import *


# In[7]:

def_res=process_beneficiary(upi_tbl1, 'payee_add_2', 'category_code', 'benef_id',regex_list, kpp,'510000', cleaner_regex_list=['\s+'])


# In[8]:

df_tagged=def_res.filter(~F.col('category_code').isin('510000')).withColumn('self_flag',F.lit(0))


# In[9]:

df_untagged=def_res.filter(F.col('category_code').isin('510000')).drop('category_code','benef_id')


# In[10]:

df_res2 = ApplyFWSequence(rootpath,sc, df_untagged, 'benef_name',
                              'base_txn_text', 'category_code', 'benef_id',
                          R_kp, R_result_dict, kpp_b, regex_list_b, 'rb','510000','remitter_name','self_flag')


# In[42]:

tagged=df_res2.filter(~F.col('category_code').isin('510000'))


# In[36]:

both_untagged=df_res2.filter(F.col('category_code').isin('510000')).drop('category_code')


# In[37]:

#upi_merch= sqlContext.table('tempdb.temp_upi_classified_marchant_19072019')


# In[38]:

upi1=both_untagged.join(F.broadcast(upi_merch),F.col('payee_addr')==F.col('Handle'),'left')


# In[40]:

upi2=upi1.withColumn('category_code',F.when(F.col('category_code').isNull(),'510000').otherwise(F.col('category_code')))


# In[11]:

#all_upi.show(500)


# In[43]:

all_upi=tagged.select(df_res2.columns).unionAll(df_tagged.select(df_res2.columns)).unionAll(upi2.select(df_res2.columns))


# In[46]:

all_upi1=all_upi.join(category_master,'category_code','left')


# In[ ]:

all_upi1a=all_upi1.withColumn('benef_name',F.when((F.col('benef_id').isNull()|F.col('benef_id').isin('')),
                                             F.upper(F.col('benef_name')))
                                                  .otherwise(F.upper(F.col('benef_id'))))


# In[49]:

all_upi2=all_upi1a.select("txn_ref_no",
"upi_trn",
"txnid",
"txn_date",
"txn_amt",
"mode",
"remitter_id",
"remitter_name",
"remitter_type",
"remitter_class",
"remitter_sub_class",
"remitter_ifsc",
"remitter_bank_name",
"remitter_account_no",
"remitter_cust_id",
"benef_id",
"benef_name",
"benef_type",
"benef_class",
"benef_sub_class",
"benef_ifsc",
"benef_bank_name",
"benef_account_no",
"benef_cust_id",
"base_txn_text",
"online_offline",
"category_level1",
"category_level2",
"category_level3",
"category_code",
"recurrance_flag",
"recurrance_pattern_id",
"verification_flag",
"self_flag",
"txn_ref_key",
"channel_code",
"prdapp",
"currstatuscode",
"currstatusdesc",
"txntype",
"drcrflag",
"paytype",
"pracctype",
"payer_addr",
"payee_addr",
"prfname",
"praccno",
"I_C",
"prdmobile",
"prdloc",
"txninit",
"custrefno",
"subcode",
"mid",
"merchanttype",
"brand",
"legal",
"franchise",
"negmobflag",
"channel",
"code",
'data_dt')


# In[13]:

upi_classified=all_upi2


# In[14]:

output_cols = out_table.columns

to_fill = upi_classified.columns


# In[131]:

res = upi_classified
for x in output_cols:
    if x  not in to_fill:
        res = res.withColumn(x, F.lit(''))


# In[47]:

res_to_write = res.select(output_cols)


# In[ ]:

res_to_write.write.insertInto(out_tbl, True)

----------------------------

POS.Py

# coding: utf-8

# In[1]:

import pandas as pd
from nltk.util import skipgrams

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.ml import feature as MF
from dateutil import relativedelta
import datetime

import os, io, re, csv
import string
from collections import Counter
from itertools import permutations,chain
import time
#from textutils.viktext import KeywordProcessor
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
import pyspark
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window
import ConfigParser
import sys



# In[ ]:


sc = SparkContext()
sc.setCheckpointDir('/tmp/spark-code-neft')

try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)

# In[2]:

config = ConfigParser.ConfigParser()
configFile = sys.argv[1]
config.read(configFile)
data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY',)
data_dt = data_dtt.strip('"').strip("'")
#configFile = "/data/08/notebooks/tmp/Anisha/Pooling_table/pool_dc.ini"


# In[4]:

#'initilize with reading table names, data_dt and batch ids from configuration file'

#cust_mst_tbl_name = config.get('contactability','cust_mst_tbl_name')
pos_txn_tbl = config.get('default','INP_DB_NM_1') + '.' +                      config.get('default','INP_TBL_NM_1')

pos_table = (sqlContext.table(pos_txn_tbl)
             .filter((F.col('data_dt')>=( datetime.datetime.strptime(data_dt, "%Y-%m-%d")-relativedelta.relativedelta(days=7)))
                     &(F.col('data_dt')<=str(data_dt))).drop('benef_id'))

#sms_tbl = config.get('contactability','sms_tbl')
category_mast_tbl = config.get('default', 'INP_DB_NM_2') + '.' +                            config.get('default','INP_TBL_NM_2')
category_master = sqlContext.table(category_mast_tbl).drop('batch_id')

out_tbl = config.get('default', 'OUT_DB_NM') + '.' +                  config.get('default','OUT_TBL_NM')
out_table = sqlContext.table(out_tbl)


# In[ ]:

stopwords_list = ['pvt', 'ltd', 'www', 'com', 'and', 'limited', 'lt', 'of', 'to', 'co', 'bd', 'road', 'rd', 'private', 'mc',
          'es', 'company', 'st', 'sta', 'priv', 'pv', 'unit', 'dr', 'in', 'ce', 're', 'li', 'an', 'ss', 'ch', 'on',
          'tr', 'td', 'er', 'ms', 'by', 'ng', 'ts', 'ma', 'al', 'nd', 'pu', 'ba', 'ph', 'nx', 'no', 'nt', 'ho', 'v2',
          'um', 'sw', 'ks', 'me', 'ca', 'se', 'pr', 'fo', 'ar', 'en', 'et', 'jj', 'ls', 'tn', 'fu', 'op', 'hi', 'cs',
          'de', 'sa', 'ty', 'su', 'at', 'dt', 'll', 'sgh', 'ess', 'ing', 'for', 'grt', 'ses', 'pri', 'nkp', 'swe',
          'llp', 'lim', 'oth', 'les', 'sto', 'all', 'res', 'ted', 'stn', 'mau', 'stat', 'limi', 'tion', 'only', 'comp',
          'reta', 'conc', 'ourt', 'corp', 'bros', 'stati', 'brothers' 'limite', 'limit', 'new', 'navi',
          'service', 'services', 'india', 'the', 'zes', 'pl', 'msw', 'tps', 'inc', 'cpm', 'rwl', 'ads', 'rel', 'ser', 'rsp',
          'cf', 'wh', 'le', 'hyderabad', 'pune', 'http', 'da', 'obd', 'blr', 'bo', 'karnataka', 'af', 'expressway', 'goa',
          'kapashera', 'ebs', 'so', 'gujarat', 'it', 'am', 'sol', 'mal', 'out', 'ji', 'fi', 'up', 'gene', 'airoli', 'di',
          'ge', 'dahisar', 'mh', 'cc', 'chandigarh', 'comm', 'sho', 'mgl', 'fue', 'thaliwada', 'panchkula', 'ac', 'sector',
          'thane', 'rohini', 'cg', 'ka', 'hyd', 'ez', 'tsspdclpgi', 'dc', 'sil', 'panchku', 'ii', 'foo', 'ind', 'sp', 'kharghar',
          'nh', 'yo', 'pa', 'bengal', 'vee', 'rampura', 'bhopal', 'tra', 'dubai', 'ea', 'be', 'pe', 'th', 'au', 'us', 'eta',
          'https','ne', 'lucknow', 'fa', 'rr', 'si', 'ga', 'pc', 'gurgao']


# In[5]:

pos_table1 = (pos_table.withColumn('benef_name',
                                               F.upper(F.trim(F.regexp_replace(F.trim(F.regexp_replace(F.trim(F.regexp_replace(
                                (F.substring('benef_name',1,22)),'(\d+)','')),'[^A-Z? ?a-z?]',' ')),'  +',' ')))).
                     withColumn('city',F.trim(F.substring('benef_name',23,15))).
                     withColumn('country',F.trim(F.substring('benef_name',37,4)))
                    )



# In[7]:

root_path = '/ybl/dwh/artifacts/sherlock/pythonJobs'
sc.addPyFile(root_path + '/Transaction-Classification/PosFW.py')
from PosFW import *
kp = initialize(root_path, sc)

pos_cat = get_pos_txn_cat(sc, sqlContext, pos_table1,'benef_name','cod_merch_typ','benef_id','category_code', kp)


# In[12]:

pos_cat1=(pos_cat.withColumn('benef_name',F.when((F.col('benef_id').isNull()|F.col('benef_id').isin('')),
                                             F.upper(F.col('benef_name')))
                                                  .otherwise(F.upper(F.col('benef_id'))))
                .withColumn('category_code',F.when(F.col('category_code').isNull(),'170000').otherwise(F.col('category_code')))
     )


# In[13]:

pos_data_final=pos_cat1.join(category_master,'category_code','left')


# In[53]:

pos_final=pos_data_final.select(F.col("txn_ref_no"),
F.col("txn_date"),
F.col("txn_amt"),
F.col("mode"),
F.col("remitter_id"),
F.col("remitter_name"),
F.col("remitter_type"),
F.col("remitter_class"),
F.col("remitter_sub_class"),
F.col("remitter_ifsc"),
F.col("remitter_bank_name"),
F.col("remitter_account_no"),
F.col("remitter_cust_id"),
F.col("benef_id"),
F.col("benef_name"),
F.col("benef_type"),
F.col("benef_class"),
F.col("benef_sub_class"),
F.col("benef_ifsc"),
F.col("benef_bank_name"),
F.col("benef_account_no"),
F.col("benef_cust_id"),
F.col("base_txn_text"),
F.col("online_offline"),
F.col("category_level1"),
F.col("category_level2"),
F.col("category_level3"),
F.col("category_code"),
F.col("recurrance_flag"),
F.col("recurrance_pattern_id"),
F.col("verification_flag"),
F.col("self_flag"),
F.col("txn_ref_key"),
F.col("channel_code"),
F.col("cod_msg_typ"),
F.col("flg_drcr"),
F.col("acq_inst_id"),
F.col("fwd_inst_id"),
F.col("retrieval_ref_no"),
F.col("cod_reply"),
F.col("ca_term_id"),
F.col("ca_id_code"),
F.col("txn_nrrtv"),
F.col("cod_txn_ccy"),
F.col("dat_value_stl"),
F.col("dat_post_stl"),
F.col("cod_merch_typ"),
F.col("cod_txn_ccy"),
F.col("cod_prod"),
F.col("cod_txn_mnemonic"),
F.col("cod_txn_literal"),
F.col("ref_usr_no"),
F.col("cod_orig_proc"),
F.col("cod_merch_typ"),
F.col("ctr_batch_no"),
F.col("ref_sys_tr_aud_no"),
F.col("channel"),
F.col("I_C"),
F.col('data_dt'))

# In[54]:

output_cols = out_table.columns
to_fill = pos_data_final.columns

res = pos_data_final
for x in output_cols:
    if x  not in to_fill:
        res = res.withColumn(x, F.lit(''))

res_to_write = res.select(output_cols)


# In[14]:

#res_to_write.take(20)


# In[ ]:

res_to_write.write.insertInto(out_tbl, True)
