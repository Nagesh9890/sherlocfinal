# coding: utf-8

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


# In[12]:

configFile = sys.argv[1]
#configFile = "/data/08/notebooks/tmp/Anisha/Pooling_table/nach_settings.ini"
config = ConfigParser.ConfigParser()
config.read(configFile)
data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY',)
data_dt = data_dtt.strip('"').strip("'")
out_tbl = config.get('default', 'OUT_DB_NM') + '.' +  config.get('default','OUT_TBL_NM')
out_table = sqlContext.table(out_tbl)
usr_batch_id=config.get('default', 'END_BATCH_ID_11')
ccy_batch_id=config.get('default', 'END_BATCH_ID_10')
brn_batch_id=config.get('default', 'END_BATCH_ID_20')


category_master_tbl = config.get('default', 'INP_DB_NM_13') + '.' +  config.get('default','INP_TBL_NM_13')

eem_master = config.get('default', 'INP_DB_NM_21') + '.' +  config.get('default','INP_TBL_NM_21')
eem_mapper = config.get('default', 'INP_DB_NM_22') + '.' +  config.get('default','INP_TBL_NM_22')

# In[1]:

#data_dt='2019-09-12'


# In[8]:
pool="""select
        Mdm_Cust.source as source,
        trim(CH_NOBOOK.cod_acct_no) as cod_acct_no,
        cast(Mdm_Cust.cod_cust as string) as cust_id,
        Mdm_Cust.mdm_id as mdm_id,
        coalesce(NEFT_IN.txn_ref_no,NEFT_OUT.txn_ref_no,NEFT_REV_ACK.txn_ref_no,NEFT_RET.txn_ref_no,RTGS.txn_ref_no,
        rtgs_rev_ret.txn_ref_no,IMPS.txn_ref_no,imps_ref_null.txn_ref_no,EPI.txn_ref_key,EPI_irctc.txn_ref_key,
        DC.txn_ref_no,SI_DEBIT.txn_ref_no,SI_CREDIT.txn_ref_no,Nach.txn_ref_no,UPI.txn_ref_no,UPI_REV.txn_ref_no,UPI_P2M_POOL.txn_ref_no,iwchq.txn_ref_no,dd_issue.txn_ref_no,
        owchq.txn_ref_no,ATM.retrieval_ref_no,esb_ecollect.txn_ref_no,Intra_FT.txn_ref_no,CH_NOBOOK.ref_txn_no) as txn_ref_no,
        CH_NOBOOK.dat_post,
        CH_NOBOOK.dat_value,
        CH_NOBOOK.dat_txn as dat_time_txn,
        CH_NOBOOK.cod_txn_mnemonic,
        CH_NOBOOK.cod_drcr as d_c,
        CH_NOBOOK.amt_txn as amount,
        CH_NOBOOK.txt_txn_desc as nobook_txn_text,
        coalesce(
NEFT_IN.base_txn_text,NEFT_OUT.base_txn_text,NEFT_REV_ACK.base_txn_text,NEFT_RET.base_txn_text,RTGS.base_txn_text,rtgs_rev_ret.base_txn_text,
        IMPS.base_txn_text,imps_ref_null.base_txn_text,SI_DEBIT.base_txn_text,SI_CREDIT.base_txn_text,
        DC.base_txn_text,EPI.base_txn_text,EPI_irctc.base_txn_text,Nach.base_txn_text,UPI.base_txn_text,UPI_REV.base_txn_text,UPI_P2M_POOL.base_txn_text,
        dd_issue.base_txn_text,iwchq.base_txn_text,owchq.base_txn_text,esb_ecollect.base_txn_text,Intra_FT.base_txn_text) as base_txn_text,
        (
        case
        when Cat_Mast.mode ='NEFT'
        then TRIM(CH_NOBOOK.ref_txn_no)
        WHEN Cat_Mast.mode ='RTGS'
        then TRIM(CH_NOBOOK.ref_txn_no)
        WHEN (CH_NOBOOK.txt_txn_desc like 'IMPS/%%' or CH_NOBOOK.txt_txn_desc like '%%/RRN%%'
       OR CH_NOBOOK.txt_txn_desc like '%%/IMPS/RRN:%%' or CH_NOBOOK.txt_txn_desc LIKE '%%RRN : %%'
        )
        THEN (case when length(trim(regexp_extract(CH_NOBOOK.ref_usr_no,'[0-9]+',0)))>12
        then substr(REGEXP_EXTRACT(CH_NOBOOK.txt_txn_desc,'/RRN:(.*)',1),1,instr(
        REGEXP_EXTRACT(CH_NOBOOK.txt_txn_desc,'/RRN:(.*)',1),'/')-1)
        else trim(regexp_extract(CH_NOBOOK.ref_usr_no,'[0-9]+',0))
        end)
        WHEN DC.MODE = 'DEBIT CARD'
        THEN concat(TRIM(CH_NOBOOK.cod_acct_no),trim(CH_NOBOOK.ref_txn_no))
        WHEN (CH_NOBOOK.TXT_TXN_DESC LIKE 'REV:UPI/%%' OR CH_NOBOOK.TXT_TXN_DESC LIKE 'UPI/%%')
        THEN TRIM(CH_NOBOOK.ref_usr_no)
        WHEN EPI.MODE ='EPI'
        THEN concat(trim(CH_NOBOOK.cod_acct_no),trim(CH_NOBOOK.ref_txn_no))
        WHEN EPI_irctc.MODE ='EPI'
        THEN concat(trim(CH_NOBOOK.cod_acct_no),trim(CH_NOBOOK.ref_txn_no))
        WHEN Cat_Mast.MODE = 'NACH'
        THEN concat(trim(CH_NOBOOK.cod_acct_no),trim(CH_NOBOOK.ref_chq_no))
        WHEN Cat_Mast.MODE = 'ACCOUNT TRANSFER'
        THEN concat(trim(CH_NOBOOK.cod_acct_no),to_date(CH_NOBOOK.dat_txn),CH_NOBOOK.amt_txn)
        WHEN Cat_Mast.MODE = 'DD'
        THEN CONCAT(CH_NOBOOK.ref_chq_no,ch_nobook.ref_sys_tr_aud_no,CH_NOBOOK.cod_txn_mnemonic)
        WHEN iwchq.MODE ='CHEQUE'
        THEN concat(CH_NOBOOK.ref_chq_no,CH_NOBOOK.COD_ACCT_NO)
        WHEN owchq.MODE ='CHEQUE'
        THEN concat(CH_NOBOOK.ref_chq_no,CH_NOBOOK.COD_ACCT_NO)
        when trim(CH_NOBOOK.ref_usr_no) = trim(esb_ecollect.CREDIT_REQ_REF)
        then concat(CH_NOBOOK.ref_usr_no,CH_NOBOOK.cod_acct_no)
        when trim(ch_nobook.ref_txn_no) = trim(Intra_FT.txn_ref_no)
        then trim(ch_nobook.ref_txn_no)
        ELSE concat(CH_NOBOOK.cod_txn_mnemonic,trim(CH_NOBOOK.cod_acct_no),TRIM(CH_NOBOOK.ref_chq_no),trim(CH_NOBOOK.ref_txn_no))END) as noobook_key,
        coalesce(
        NEFT_IN.txn_ref_key,NEFT_OUT.txn_ref_key,NEFT_REV_ACK.txn_ref_key,NEFT_RET.txn_ref_key,RTGS.txn_ref_key,rtgs_rev_ret.txn_ref_key,IMPS.txn_ref_key,
        imps_ref_null.txn_ref_key,DC.txn_ref_key,
        EPI.txn_ref_key,EPI_irctc.txn_ref_key,SI_DEBIT.txn_ref_key,SI_CREDIT.txn_ref_key,Nach.txn_ref_key,UPI.txn_ref_key,UPI_REV.txn_ref_key,UPI_P2M_POOL.txn_ref_key,
        dd_issue.txn_ref_key,iwchq.txn_ref_key,owchq.txn_ref_key,esb_ecollect.txn_ref_no,Intra_FT.txn_ref_no) as txn_ref_key,
        Mdm_Cust.i_c,
        (
        case WHEN (CH_NOBOOK.cod_drcr = 'D' AND CH_NOBOOK.amt_txn > 0.00)
        THEN
        (CASE
        when NEFT_IN.mode = 'NEFT' THEN NEFT_IN.channel
        when NEFT_OUT.mode = 'NEFT' THEN NEFT_OUT.channel
        when NEFT_REV_ACK.mode = 'NEFT' THEN NEFT_REV_ACK.channel
        when NEFT_RET.mode = 'NEFT' THEN NEFT_RET.channel
        when RTGS.mode ='RTGS' THEN RTGS.channel
        when rtgs_rev_ret.mode ='RTGS' THEN rtgs_rev_ret.channel
        WHEN UPI.mode ='UPI' THEN upi.Channel
        when UPI_REV.mode='UPI' then UPI_REV.Channel
        WHEN IMPS.mode ='IMPS' then IMPS.channel
        WHEN imps_ref_null.mode ='IMPS' then imps_ref_null.channel
        when trim(ch_nobook.ref_txn_no) = trim(Intra_FT.txn_ref_no)
        then Intra_FT.channel
        WHEN SI_DEBIT.mode ='ACCOUNT TRANSFER' then SI_DEBIT.channel
        WHEN CH_NOBOOK.txt_txn_desc like '%%.TXT%%' THEN 'GEFU'
        WHEN Usr_Profile.cod_user_id ='POS_USER' THEN 'POS'
        WHEN Usr_Profile.cod_user_id ='ATM_USER' THEN 'ATM'
        WHEN Usr_Profile.cod_user_id in ('IB','IBUSER','NETUSER') THEN 'RNB'
        WHEN Usr_Profile.cod_user_id ='PYMT_USER' THEN 'PAYMENT'
        WHEN Usr_Profile.cod_user_id in ('SYSTEM','SYSTELLER') THEN 'SYSTEM'
        WHEN Usr_Profile.cod_user_id = 'BNAUSER' THEN 'BNA MACHINE USER'
        WHEN CH_NOBOOK.txt_txn_desc like '%%MOBTXN%%' THEN 'MB'
        WHEN Usr_Profile.cod_user_id ='MOBUSER'  THEN 'MB'
        WHEN Usr_Profile.cod_user_id ='EXCHUSER' THEN 'EXCHANGE'
        WHEN (Usr_Profile.cod_user_id rlike '[^0-9]' OR cod_user_id ='BRNEFTUSER') THEN 'BRANCH'
        WHEN Cat_Mast.channel ='' then 'OTHER'
        WHEN Cat_Mast.channel <>'' then Cat_Mast.channel
        ELSE 'OTHER'
        end)
        WHEN (CH_NOBOOK.cod_drcr = 'C' or (CH_NOBOOK.cod_drcr = 'D' and CH_NOBOOK.amt_txn < 0.00 ))
        THEN 'SYSTEM'
        end)as channel,
        (case
        when (trim(CH_NOBOOK.ref_usr_no) = trim(esb_ecollect.CREDIT_REQ_REF) and CH_NOBOOK.cod_drcr = 'C')
        then 'ESB_ECOLLECT'
        when (IMPS.channel_code = 'ESB_FT' or RTGS.channel_code ='ESB_FT' or NEFT_OUT.channel_code ='ESB_FT'
        or trim(ch_nobook.ref_txn_no) = trim(Intra_FT.txn_ref_no))
        then 'ESB_FT'
        when (IMPS.channel_code = 'ESB_INW_REMIT' or RTGS.channel_code ='ESB_INW_REMIT' or NEFT_OUT.channel_code ='ESB_INW_REMIT'
             or UPI.channel_code = 'ESB_INW_REMIT')
        then 'ESB_INW_REMIT'
        end) as channel_code,
        'NULL' as channel_class,
        (case
        WHEN ((IMPS.MODE = 'IMPS' or CH_NOBOOK.txt_txn_desc like 'IMPS/%%' or CH_NOBOOK.txt_txn_desc like '%%/RRN%%'
        or CH_NOBOOK.txt_txn_desc like '%%/IMPS/RRN:%%'
    or CH_NOBOOK.txt_txn_desc LIKE '%%RRN : %%') and (CH_NOBOOK.txt_txn_desc NOT LIKE '%%BNA CASH DEPOSIT%%'
    AND CH_NOBOOK.txt_txn_desc NOT LIKE '%%CASH DEPOSITYBL BNA RRN%%') )
        THEN 'IMPS'
        WHEN (UPI.MODE = 'UPI' or UPI_REV.mode='UPI' or UPI_P2M_POOL.mode = 'UPI' or CH_NOBOOK.TXT_TXN_DESC LIKE 'REV:UPI/%%' OR CH_NOBOOK.TXT_TXN_DESC LIKE 'UPI/%%')
        THEN 'UPI'
    when (trim(CH_NOBOOK.ref_usr_no) = trim(esb_ecollect.CREDIT_REQ_REF) and CH_NOBOOK.cod_drcr = 'C')
    then (case when esb_ecollect.mode ='FT' then 'ACCOUNT TRANSFER' else esb_ecollect.mode end)
    when trim(ch_nobook.ref_txn_no) = trim(Intra_FT.txn_ref_no)
    then Intra_FT.mode
        ELSE coalesce(NEFT_IN.MODE,NEFT_OUT.MODE,NEFT_REV_ACK.MODE,NEFT_RET.MODE,RTGS.MODE,rtgs_rev_ret.mode,EPI.MODE,EPI_irctc.MODE,
                    DC.MODE,Nach.MODE,SI_DEBIT.MODE,SI_CREDIT.MODE,dd_issue.MODE,iwchq.MODE,owchq.MODE,
                        (case when Cat_Mast.mode IS NULL then 'OTHER'
                        ELSE Cat_Mast.mode end), 'OTHER')
        END
        ) as mode,
       coalesce
(NEFT_IN.remitter_id,NEFT_OUT.remitter_id,RTGS.remitter_id,rtgs_rev_ret.remitter_id,IMPS.remitter_id,imps_ref_null.remitter_id,SI_DEBIT.remitter_id,SI_CREDIT.remitter_id,EPI.remitter_id,EPI_irctc.remitter_id,DC.remitter_id,Nach.remitter_id,
        UPI.remitter_id,UPI_REV.remitter_id,UPI_P2M_POOL.remitter_id,dd_issue.remitter_id,iwchq.remitter_id,owchq.remitter_id,esb_ecollect.remitter_id,Intra_FT.remitter_id) as remitter_id,
        coalesce(NEFT_IN.remitter_name,NEFT_OUT.remitter_name,NEFT_REV_ACK.remitter_name,NEFT_RET.remitter_name,
        RTGS.remitter_name,rtgs_rev_ret.remitter_name,
        IMPS.remitter_name,imps_ref_null.remitter_name,SI_DEBIT.remitter_name,SI_CREDIT.remitter_name,DC.remitter_name,EPI.remitter_name,EPI_irctc.remitter_name,Nach.remitter_name,
        UPI.remitter_name,UPI_REV.remitter_name,UPI_P2M_POOL.remitter_name,dd_issue.remitter_name,iwchq.remitter_name,owchq.remitter_name,esb_ecollect.remitter_name,Intra_FT.remitter_name,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'D')
        then Mdm_Cust.cod_acct_title end),
        (case
        when (CH_NOBOOK.cod_txn_mnemonic ='2157' and CH_NOBOOK.ref_usr_no is NULL)
        then Mdm_Cust.cod_acct_title end)
        ) as remitter_name,
        coalesce
(NEFT_IN.remitter_type,NEFT_OUT.remitter_type,RTGS.remitter_type,rtgs_rev_ret.remitter_type,IMPS.remitter_type,imps_ref_null.remitter_type,
    SI_DEBIT.remitter_type,SI_CREDIT.remitter_type,DC.remitter_type,EPI.remitter_type,EPI_irctc.remitter_type,
       Nach.remitter_type,UPI.remitter_type,UPI_REV.remitter_type,UPI_P2M_POOL.remitter_type,dd_issue.remitter_type,iwchq.remitter_type,owchq.remitter_type,esb_ecollect.remitter_type
        ) as remitter_type,
        coalesce(NEFT_IN.remitter_class,NEFT_OUT.remitter_class,RTGS.remitter_class,rtgs_rev_ret.remitter_class,IMPS.remitter_class,imps_ref_null.remitter_class,
        SI_DEBIT.remitter_class,SI_CREDIT.remitter_class,
    DC.remitter_class,EPI.remitter_class,EPI_irctc.remitter_class,Nach.remitter_class,UPI.remitter_class,UPI_REV.remitter_class,UPI_P2M_POOL.remitter_class,dd_issue.remitter_class,iwchq.remitter_class,owchq.remitter_class,
    esb_ecollect.remitter_class) as remitter_class,
       coalesce
(NEFT_IN.remitter_sub_class,NEFT_OUT.remitter_sub_class,RTGS.remitter_sub_class,rtgs_rev_ret.remitter_sub_class,IMPS.remitter_sub_class,imps_ref_null.remitter_sub_class,SI_DEBIT.remitter_sub_class,DC.remitter_sub_class,
EPI.remitter_sub_class,EPI_irctc.remitter_sub_class,Nach.remitter_sub_class,UPI.remitter_sub_class,UPI_REV.remitter_sub_class,UPI_P2M_POOL.remitter_sub_class,dd_issue.remitter_sub_class,iwchq.remitter_sub_class,
owchq.remitter_sub_class,esb_ecollect.remitter_sub_class) as remitter_sub_class,
        coalesce(NEFT_IN.remitter_ifsc,NEFT_OUT.remitter_ifsc,NEFT_REV_ACK.remitter_ifsc,NEFT_RET.remitter_ifsc,
        RTGS.remitter_ifsc,rtgs_rev_ret.remitter_ifsc,IMPS.remitter_ifsc,imps_ref_null.remitter_ifsc,SI_DEBIT.remitter_ifsc,SI_CREDIT.remitter_ifsc,DC.remitter_ifsc,EPI.remitter_ifsc,EPI_irctc.remitter_ifsc,
        Nach.remitter_ifsc,UPI.remitter_ifsc,UPI_REV.remitter_ifsc,UPI_P2M_POOL.remitter_ifsc,dd_issue.remitter_ifsc,iwchq.remitter_ifsc,
        owchq.remitter_ifsc,esb_ecollect.remitter_ifsc,Intra_FT.remitter_ifsc,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'D')
        then Mdm_Cust.ifsc_code end),
        (case
        when (CH_NOBOOK.cod_txn_mnemonic ='2157' and CH_NOBOOK.ref_usr_no is NULL)
        then Mdm_Cust.ifsc_code end)
        ) as remitter_ifsc,
        coalesce
(NEFT_IN.remitter_bank_name,NEFT_OUT.remitter_bank_name,NEFT_REV_ACK.remitter_bank_name,NEFT_RET.remitter_bank_name,
        RTGS.remitter_bank_name,rtgs_rev_ret.remitter_bank_name,IMPS.remitter_bank_name,imps_ref_null.remitter_bank_name,
        SI_DEBIT.remitter_bank_name,SI_CREDIT.remitter_bank_name
        ,DC.remitter_bank_name,EPI.remitter_bank_name,EPI_irctc.remitter_bank_name,Nach.remitter_bank_name,UPI.remitter_bank_name,
       UPI_REV.remitter_bank_name,UPI_P2M_POOL.remitter_bank_name,dd_issue.remitter_bank_name,iwchq.remitter_bank_name,owchq.remitter_bank_name,esb_ecollect.remitter_bank_name,Intra_FT.remitter_bank_name,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'D')
        then Mdm_Cust.Bank_Name end),
        (case
        when (CH_NOBOOK.cod_txn_mnemonic ='2157' and CH_NOBOOK.ref_usr_no is NULL)
        then Mdm_Cust.Bank_Name end)) as remitter_bank_name,
        coalesce
(NEFT_IN.remitter_account_no,NEFT_OUT.remitter_account_no,NEFT_REV_ACK.remitter_account_no,NEFT_RET.remitter_account_no,
        RTGS.remitter_account_no,rtgs_rev_ret.remitter_account_no,IMPS.remitter_account_no,imps_ref_null.remitter_account_no,SI_DEBIT.remitter_account_no,SI_CREDIT.remitter_account_no
        ,EPI.remitter_account_no,EPI_irctc.remitter_account_no,DC.remitter_account_no,Nach.remitter_account_no,
        UPI.remitter_account_no,UPI_REV.remitter_account_no,UPI_P2M_POOL.remitter_account_no,dd_issue.remitter_account_no,iwchq.remitter_account_no,owchq.remitter_account_no,esb_ecollect.remitter_account_no,Intra_FT.remitter_account_no,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'D')
        then Mdm_Cust.cod_acct_no end),
        (case
        when (CH_NOBOOK.cod_txn_mnemonic ='2157' and CH_NOBOOK.ref_usr_no is NULL)
        then Mdm_Cust.cod_acct_no end)
        ) as remitter_account_no,
        coalesce(NEFT_OUT.remitter_cust_id,NEFT_REV_ACK.remitter_cust_id,RTGS.remitter_cust_id,rtgs_rev_ret.remitter_cust_id,
        IMPS.remitter_cust_id,imps_ref_null.remitter_cust_id,EPI.remitter_cust_id,EPI_irctc.remitter_cust_id,DC.remitter_cust_id,
        SI_DEBIT.remitter_cust_id,SI_CREDIT.remitter_cust_id,
        Nach.remitter_cust_id,UPI.remitter_cust_id,UPI_REV.remitter_cust_id,UPI_P2M_POOL.remitter_cust_id,dd_issue.remitter_cust_id,
        iwchq.remitter_cust_id,owchq.remitter_cust_id,esb_ecollect.remitter_cust_id,Intra_FT.remitter_cust_id,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'D')
        then Mdm_Cust.cod_cust end),
        (case
        when (CH_NOBOOK.cod_txn_mnemonic ='2157' and CH_NOBOOK.ref_usr_no is NULL)
        then Mdm_Cust.cod_cust end)
        ) as remitter_cust_id,
        coalesce(NEFT_IN.benef_id,NEFT_RET.benef_id,RTGS.benef_id,rtgs_rev_ret.benef_id
        ,IMPS.benef_id,imps_ref_null.benef_id,SI_DEBIT.benef_id,EPI.benef_id,EPI_irctc.benef_id,DC.benef_id,Nach.benef_id,UPI.benef_id,UPI_REV.benef_id,
        UPI_P2M_POOL.benef_id,dd_issue.benef_id,iwchq.benef_id,owchq.benef_id,esb_ecollect.benef_id,Intra_FT.benef_id
        )as benef_id,
        coalesce(NEFT_IN.benef_name,NEFT_OUT.benef_name,NEFT_REV_ACK.benef_name,NEFT_RET.benef_name,
        RTGS.benef_name,rtgs_rev_ret.benef_name,IMPS.benef_name,imps_ref_null.benef_name
        ,SI_DEBIT.benef_name,SI_CREDIT.benef_name,EPI.benef_name,DC.benef_name,Nach.benef_name,UPI.benef_name,UPI_REV.benef_name,UPI_P2M_POOL.benef_name,
        dd_issue.benef_name,iwchq.benef_name,owchq.benef_name,esb_ecollect.benef_name,Intra_FT.bene_name,
        (case
        when Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'C'
        then Mdm_Cust.cod_acct_title end)
        )as benef_name,
        coalesce(NEFT_IN.benef_type,NEFT_OUT.benef_type,RTGS.benef_type,rtgs_rev_ret.benef_type
        ,IMPS.benef_type,imps_ref_null.benef_type,SI_DEBIT.benef_type,SI_CREDIT.benef_type,DC.benef_type,EPI.benef_type,EPI_irctc.benef_type,Nach.benef_type,UPI.benef_type,UPI_REV.benef_type,
        UPI_P2M_POOL.benef_type,dd_issue.benef_type,iwchq.benef_type,owchq.benef_type,esb_ecollect.benef_type) as benef_type,
        coalesce(NEFT_IN.benef_class,NEFT_OUT.benef_class,RTGS.benef_class,rtgs_rev_ret.benef_class,IMPS.benef_class,imps_ref_null.benef_class,
        SI_DEBIT.benef_class,SI_CREDIT.benef_class,DC.benef_class,EPI.benef_class,EPI_irctc.benef_class,Nach.benef_class,UPI.benef_class,UPI_REV.benef_class,
        UPI_P2M_POOL.benef_class,dd_issue.benef_class,iwchq.benef_class,owchq.benef_class,esb_ecollect.benef_class) as benef_class,
        coalesce(NEFT_IN.benef_sub_class,NEFT_OUT.benef_sub_class,RTGS.benef_sub_class,rtgs_rev_ret.benef_sub_class,IMPS.benef_sub_class,imps_ref_null.benef_sub_class,
        SI_DEBIT.benef_sub_class,SI_CREDIT.benef_sub_class,DC.benef_sub_class,EPI.benef_sub_class,EPI_irctc.benef_sub_class,Nach.benef_sub_class,UPI.benef_sub_class,UPI_REV.benef_sub_class,
        UPI_P2M_POOL.benef_sub_class,dd_issue.benef_sub_class,iwchq.benef_sub_class,owchq.benef_sub_class,esb_ecollect.benef_sub_class) as benef_sub_class,
        coalesce(NEFT_IN.benef_ifsc,NEFT_OUT.benef_ifsc,NEFT_REV_ACK.benef_ifsc,NEFT_RET.benef_ifsc,
        RTGS.benef_ifsc,rtgs_rev_ret.benef_ifsc,IMPS.benef_ifsc,imps_ref_null.benef_ifsc
        ,SI_DEBIT.benef_ifsc,SI_CREDIT.benef_ifsc,EPI.benef_ifsc,EPI_irctc.benef_ifsc,DC.benef_ifsc,Nach.benef_ifsc,UPI.benef_ifsc,UPI_REV.benef_ifsc,
        UPI_P2M_POOL.benef_ifsc,dd_issue.benef_ifsc,iwchq.benef_ifsc,owchq.benef_ifsc,esb_ecollect.benef_ifsc,Intra_FT.benef_ifsc,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'C')
        then Mdm_Cust.ifsc_code end)
        ) as benef_ifsc,
        coalesce(NEFT_IN.benef_bank_name,NEFT_OUT.benef_bank_name,NEFT_REV_ACK.benef_bank_name,NEFT_RET.benef_bank_name
        ,RTGS.benef_bank_name,rtgs_rev_ret.benef_bank_name,IMPS.benef_bank_name,imps_ref_null.benef_bank_name,SI_DEBIT.benef_bank_name,SI_CREDIT.benef_bank_name
        ,EPI.benef_bank_name,EPI_irctc.benef_bank_name,DC.benef_bank_name,Nach.benef_bank_name,UPI.benef_bank_name,UPI_REV.benef_bank_name,
        UPI_P2M_POOL.benef_bank_name,dd_issue.benef_bank_name,iwchq.benef_bank_name,owchq.benef_bank_name,esb_ecollect.benef_bank_name,Intra_FT.bene_bank_name,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'C')
        then Mdm_Cust.Bank_Name end)
        ) as benef_bank_name,
        coalesce(NEFT_IN.benef_account_no,NEFT_OUT.benef_account_no,NEFT_REV_ACK.benef_account_no,NEFT_RET.benef_account_no
        ,RTGS.benef_account_no,rtgs_rev_ret.benef_account_no,IMPS.benef_account_no,imps_ref_null.benef_account_no
        ,SI_DEBIT.benef_account_no,SI_CREDIT.benef_account_no,EPI.benef_account_no,EPI_irctc.benef_account_no,DC.benef_account_no,
        Nach.benef_account_no,UPI.benef_account_no,UPI_REV.benef_account_no,UPI_P2M_POOL.benef_account_no,dd_issue.benef_account_no,iwchq.benef_account_no,owchq.benef_account_no,esb_ecollect.benef_account_no,Intra_FT.bene_account_no,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'C')
        then Mdm_Cust.cod_acct_no end)
        ) as benef_account_no,
        coalesce(NEFT_IN.benef_cust_id,NEFT_RET.benef_cust_id,RTGS.benef_cust_id,rtgs_rev_ret.benef_cust_id,IMPS.benef_cust_id,imps_ref_null.benef_cust_id
        ,SI_DEBIT.benef_cust_id,SI_CREDIT.benef_cust_id,EPI.benef_cust_id,EPI_irctc.benef_cust_id,DC.benef_cust_id,Nach.benef_cust_id,
        UPI.benef_cust_id,UPI_REV.benef_cust_id,UPI_P2M_POOL.benef_cust_id,dd_issue.benef_cust_id,iwchq.benef_cust_id,owchq.benef_cust_id,esb_ecollect.benef_cust_id,Intra_FT.bene_cust_id,
        (case
        when (Cat_Mast.class_type = 'Direct' and CH_NOBOOK.cod_drcr = 'C')
        then Mdm_Cust.cod_cust end)
        ) as benef_cust_id,
        coalesce(NEFT_IN.online_offline,NEFT_OUT.online_offline,RTGS.online_offline,rtgs_rev_ret.online_offline,IMPS.online_offline,imps_ref_null.online_offline
        ,SI_DEBIT.online_offline,SI_CREDIT.online_offline,EPI.online_offline,EPI_irctc.online_offline,DC.online_offline,Nach.online_offline,
        UPI.online_offline,UPI_REV.online_offline,UPI_P2M_POOL.online_offline,dd_issue.online_offline,iwchq.online_offline,owchq.online_offline,esb_ecollect.online_offline) as online_offline,
        Curr_Mast.NAM_CCY_SHORT as currency,
        coalesce(NEFT_IN.category_level1,NEFT_OUT.category_level1,NEFT_REV_ACK.category_level1,
        NEFT_RET.category_level1,RTGS.category_level1,rtgs_rev_ret.category_level1,IMPS.category_level1,imps_ref_null.category_level1,
        SI_DEBIT.category_level1,SI_CREDIT.category_level1,DC.category_level1,
       EPI.category_level1,EPI_irctc.category_level1,Nach.category_level1,UPI.category_level1,UPI_REV.category_level1,UPI_P2M_POOL.category_level1,dd_issue.category_level1,iwchq.category_level1,
        Cat_Mast.category_level1) as category_level1,
        coalesce
(NEFT_IN.category_level2,NEFT_OUT.category_level2,NEFT_REV_ACK.category_level2,NEFT_RET.category_level2,RTGS.category_level2,rtgs_rev_ret.category_level2,
IMPS.category_level2,imps_ref_null.category_level2,SI_DEBIT.category_level2,SI_CREDIT.category_level2,DC.category_level2,EPI_irctc.category_level2,EPI.category_level2,Nach.category_level2,UPI.category_level2,
UPI_REV.category_level2,UPI_P2M_POOL.category_level2,dd_issue.category_level2,iwchq.category_level2,Cat_Mast.category_level2
        ) as category_level2,
        coalesce
(NEFT_IN.category_level3,NEFT_OUT.category_level3,NEFT_REV_ACK.category_level3,NEFT_RET.category_level3,
RTGS.category_level3,rtgs_rev_ret.category_level3,IMPS.category_level3,imps_ref_null.category_level3,SI_DEBIT.category_level3,
SI_CREDIT.category_level3,DC.category_level3,EPI.category_level3,EPI_irctc.category_level3,Nach.category_level3,
        UPI.category_level3,UPI_REV.category_level3,UPI_P2M_POOL.category_level3,dd_issue.category_level3,iwchq.category_level3,Cat_Mast.category_level3
        ) as category_level3,
        coalesce(NEFT_IN.category_code,NEFT_OUT.category_code,NEFT_REV_ACK.category_code,NEFT_RET.category_code
        ,RTGS.category_code,rtgs_rev_ret.category_code,IMPS.category_code,imps_ref_null.category_code,
       SI_DEBIT.category_code,SI_CREDIT.category_code,DC.category_code,
        EPI.category_code,EPI_irctc.category_code,Nach.category_code,UPI.category_code,UPI_REV.category_code,UPI_P2M_POOL.category_code,dd_issue.category_code,iwchq.category_code,Cat_Mast.category_code
        ) as category_code,
        coalesce(NEFT_IN.recurrance_flag,NEFT_OUT.recurrance_flag,NEFT_REV_ACK.recurrance_flag,NEFT_RET.recurrance_flag
        ,RTGS.recurrance_flag,rtgs_rev_ret.recurrance_flag,IMPS.recurrance_flag,imps_ref_null.recurrance_flag,
        SI_DEBIT.recurrance_flag,SI_CREDIT.recurrance_flag,DC.recurrance_flag,EPI.recurrance_flag,EPI_irctc.recurrance_flag,
        Nach.recurrance_flag,UPI.recurrance_flag,UPI_REV.recurrance_flag,UPI_P2M_POOL.recurrance_flag,dd_issue.recurrance_flag,iwchq.recurrance_flag,esb_ecollect.recurrance_flag) as recurrance_flag,
        coalesce
(NEFT_IN.recurrance_pattern_id,NEFT_OUT.recurrance_pattern_id,RTGS.recurrance_pattern_id,rtgs_rev_ret.recurrance_pattern_id,
IMPS.recurrance_pattern_id,imps_ref_null.recurrance_pattern_id,
SI_DEBIT.recurrance_pattern_id,SI_CREDIT.recurrance_pattern_id,DC.recurrance_pattern_id,EPI.recurrance_pattern_id,EPI_irctc.recurrance_pattern_id,Nach.recurrance_pattern_id,UPI.recurrance_pattern_id,
UPI_REV.recurrance_pattern_id,UPI_P2M_POOL.recurrance_pattern_id,dd_issue.recurrance_pattern_id,iwchq.recurrance_pattern_id,owchq.recurrance_pattern_id,esb_ecollect.recurrance_pattern_id) as recurrance_pattern_id,
        coalesce
(NEFT_IN.verification_flag,NEFT_OUT.verification_flag,RTGS.verification_flag,rtgs_rev_ret.verification_flag,IMPS.verification_flag,imps_ref_null.verification_flag,
SI_DEBIT.verification_flag,SI_CREDIT.verification_flag,
DC.verification_flag,EPI.verification_flag,EPI_irctc.verification_flag,Nach.verification_flag,UPI.verification_flag,UPI_REV.verification_flag,UPI_P2M_POOL.verification_flag
,dd_issue.verification_flag,iwchq.verification_flag,owchq.verification_flag,esb_ecollect.verification_flag) as verification_flag,
        coalesce
(NEFT_IN.self_flag,NEFT_OUT.self_flag,RTGS.self_flag,rtgs_rev_ret.self_flag,IMPS.self_flag,imps_ref_null.self_flag,
        SI_DEBIT.self_flag,SI_CREDIT.self_flag,DC.self_flag,EPI.self_flag,EPI_irctc.self_flag,
        Nach.self_flag,UPI.self_flag,UPI_REV.self_flag,UPI_P2M_POOL.self_flag,dd_issue.self_flag,iwchq.self_flag,owchq.self_flag,esb_ecollect.self_flag) as self_flag,
        CH_NOBOOK.COD_USERNO,CH_NOBOOK.ref_sys_tr_aud_no,
        CH_NOBOOK.ctr_batch_no,
        CH_NOBOOK.cod_proc,
        CH_NOBOOK.ref_card_no,
        CH_NOBOOK.ref_usr_no,
        'NULL' as transaction_category,
        'NULL' as mcc_category,
        'NULL' as mcc_desc,
        DC.cod_merch_typ as mt_category_code,
        coalesce(IMPS.benef_nickname,NEFT_IN.benef_nickname,NEFT_OUT.benef_nickname,
        NEFT_REV_ACK.benef_nickname,NEFT_RET.benef_nickname,RTGS.benef_nickname) as benef_nickname,
        CH_NOBOOK.unique_ref_no,
        'NULL' as transaction_sub_cat,
        'NULL' as transaction_net_cat,
        cast(CH_NOBOOK.amt_txn_tcy as string) as amt_txn_tcy,
        cast(CH_NOBOOK.ref_sub_seq_no as string) as ref_sub_seq_no,
        CH_NOBOOK.cod_cc_brn_txn,
        CH_NOBOOK.cod_txn_literal,
        ATM.ca_term_id,
        '%s' as DATA_DT,
        (case
        WHEN ((IMPS.MODE = 'IMPS' or CH_NOBOOK.txt_txn_desc like 'IMPS/%%' or CH_NOBOOK.txt_txn_desc like '%%/RRN%%'
        or CH_NOBOOK.txt_txn_desc like '%%/IMPS/RRN:%%'
    or CH_NOBOOK.txt_txn_desc LIKE '%%RRN : %%') and (CH_NOBOOK.txt_txn_desc NOT LIKE '%%BNA CASH DEPOSIT%%'
    AND CH_NOBOOK.txt_txn_desc NOT LIKE '%%CASH DEPOSITYBL BNA RRN%%') )
        THEN 'IMPS'
        WHEN (UPI.MODE = 'UPI' or UPI_REV.mode='UPI' or UPI_P2M_POOL.mode = 'UPI' or CH_NOBOOK.TXT_TXN_DESC LIKE 'REV:UPI/%%' OR CH_NOBOOK.TXT_TXN_DESC LIKE 'UPI/%%')
        THEN 'UPI'
    when (trim(CH_NOBOOK.ref_usr_no) = trim(esb_ecollect.CREDIT_REQ_REF) and CH_NOBOOK.cod_drcr = 'C')
    then (case when esb_ecollect.mode ='FT' then 'ACCOUNT TRANSFER' else esb_ecollect.mode end)
    when trim(ch_nobook.ref_txn_no) = trim(Intra_FT.txn_ref_no)
    then Intra_FT.mode
        ELSE coalesce(NEFT_IN.MODE,NEFT_OUT.MODE,NEFT_REV_ACK.MODE,NEFT_RET.MODE,RTGS.MODE,rtgs_rev_ret.mode,EPI.MODE,EPI_irctc.MODE,
                    DC.MODE,Nach.MODE,SI_DEBIT.MODE,SI_CREDIT.MODE,dd_issue.MODE,iwchq.MODE,owchq.MODE,
                        (case when Cat_Mast.mode IS NULL then 'OTHER'
                        ELSE Cat_Mast.mode end), 'OTHER')
        END
        ) as partition_mode


       FROM
        (select * from db_gold.GLD_LIAB_TRANSACTION a
        where a.year='%s'
    and a.month ='%s'
    and a.`date` ='%s'
    )CH_NOBOOK


        LEFT OUTER JOIN
        (SELECT /*+MAPJOIN(a) */ a.COD_CCY,a.NAM_CCY_SHORT FROM db_stage.STG_FCR_FCRLIVE_1_BA_CCY_CODE a
       where a.batch_id ='%s')Curr_Mast
       ON
        cast(CH_NOBOOK.cod_txn_ccy  as string)= Curr_Mast.COD_CCY


       LEFT OUTER JOIN
        (select /*+MAPJOIN(a) */ a.* FROM db_stage.STG_FCR_FCRLIVE_1_SM_USER_PROFILE a
         where a.batch_id= '%s') Usr_Profile
       ON
        cast(Usr_Profile.COD_USERNO as int) = CH_NOBOOK.COD_USERNO


       LEFT OUTER JOIN
        (
       select /*+MAPJOIN(tmp1) */ * from
       (select
       b.category_code,b.category_level1,b.category_level2,b.category_level3,
       a.cod_txn_mnemonic,a.class_type,a.mode,a.channel
       from
       db_stage.stg_fle_txn_mnemonics_master a
       left outer join
       db_stage.stg_fle_category_master b
       on TRIM(a.category_code) = TRIM(b.category_code)
       ) tmp1)Cat_Mast
       ON
        cast(CH_NOBOOK.COD_TXN_MNEMONIC as string) = trim(Cat_Mast.COD_TXN_MNEMONIC)


LEFT OUTER JOIN
    (
    select
    a.cod_cust,a.cod_acct_no,a.cod_acct_title,a.cod_cc_brn,
    a.i_c,a.source,a.mdm_id,b.cod_fin_inst_id as ifsc_code,'YES BANK LTD' as Bank_Name
from
(SELECT
cod_cust,cod_acct_no,cod_acct_title,cod_cc_brn,i_c,source,mdm_id
FROM db_gold.GLD_MDM_CUST_POOL a
where a.data_dt ='%s')a
        left outer join
(select /*+MAPJOIN(a) */ a.cod_cc_brn,a.cod_fin_inst_id
        from db_stage.stg_fcr_fcrlive_1_ba_cc_brn_mast a
        where a.batch_id ='%s'
)b
on a.cod_cc_brn = b.cod_cc_brn
)Mdm_Cust
       ON
        trim(CH_NOBOOK.cod_acct_no)=TRIM(Mdm_Cust.cod_acct_no)

LEFT OUTER JOIN
        (SELECT a.* FROM db_smith.SMTH_POOL_SI a
        where a.data_dt in (select max(data_dt) from db_smith.SMTH_POOL_SI)
        and cod_si_type <> '10'
        AND flg_mnt_status = 'A'
             )SI_DEBIT
       ON
        (case when CH_NOBOOK.cod_txn_mnemonic = 9990
                then TRIM(CH_NOBOOK.cod_acct_no) ELSE CH_NOBOOK.DAT_TXN END) = TRIM(SI_DEBIT.remitter_account_no)
        AND CH_NOBOOK.AMT_TXN = CAST(SI_DEBIT.txn_amt AS DECIMAL(18,2))
        and substr(TRIM(CH_NOBOOK.txt_txn_desc),1,15) = TRIM(SI_DEBIT.benef_account_no)
        AND CH_NOBOOK.cod_txn_mnemonic = 9990
        and (case when CH_NOBOOK.txt_txn_desc like '%% RD %%'
            then upper(substr(trim(CH_NOBOOK.txt_txn_desc),-15))
            when CH_NOBOOK.txt_txn_desc like '%%NET TXN:%%'
            then upper(trim(substr(CH_NOBOOK.txt_txn_desc,26,7)))
            else upper(substr(trim(CH_NOBOOK.txt_txn_desc),-10))
            end) = (case when SI_DEBIT.base_txn_text like '%% RD %%'
            then upper(substr(trim(SI_DEBIT.base_txn_text),-15))
            when SI_DEBIT.base_txn_text like '%%NET TXN:%%'
            then upper(trim(substr(SI_DEBIT.base_txn_text,1,7)))
           else upper(substr(trim(SI_DEBIT.base_txn_text),-10))
            end)


LEFT OUTER join
        (SELECT a.* FROM db_smith.SMTH_POOL_SI a
        where a.data_dt in (select max(data_dt) from db_smith.SMTH_POOL_SI)
        and cod_si_type <> '10'
        AND flg_mnt_status = 'A'
        )SI_CREDIT
ON
        (CASE WHEN CH_NOBOOK.cod_txn_mnemonic = 9991 THEN TRIM(CH_NOBOOK.cod_acct_no)
                ELSE CH_NOBOOK.DAT_TXN  END ) = TRIM(SI_CREDIT.benef_account_no)
        AND CH_NOBOOK.AMT_TXN = CAST(SI_CREDIT.txn_amt AS DECIMAL(18,2))
        and substr(TRIM(CH_NOBOOK.txt_txn_desc),1,15) = TRIM(SI_CREDIT.remitter_account_no)
        AND CH_NOBOOK.cod_txn_mnemonic = 9991
        and (case when CH_NOBOOK.txt_txn_desc like '%% RD %%'
            then upper(substr(trim(CH_NOBOOK.txt_txn_desc),-15))
            when CH_NOBOOK.txt_txn_desc like '%%NET TXN:%%'
            then upper(trim(substr(CH_NOBOOK.txt_txn_desc,26,7)))
            else upper(substr(trim(CH_NOBOOK.txt_txn_desc),-10))
            end) = (case when SI_CREDIT.base_txn_text like '%% RD %%'
            then upper(substr(trim(SI_CREDIT.base_txn_text),-15))
            when SI_CREDIT.base_txn_text like '%%NET TXN:%%'
            then upper(trim(substr(SI_CREDIT.base_txn_text,1,7)))
            else upper(substr(trim(SI_CREDIT.base_txn_text),-10))
            end)

left outer join
      (SELECT * FROM db_smith.SMTH_POOL_DDISSUE where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2)) dd_issue
      on
       TRIM(CH_NOBOOK.cod_acct_no)=TRIM(dd_issue.remitter_account_no)
        and CH_NOBOOK.ref_sys_tr_aud_no = cast(dd_issue.ref_sys_tr_aud_no as int)
        and CH_NOBOOK.cod_txn_mnemonic = cast(dd_issue.cod_txn_mnemonic as int)
        and to_date(CH_NOBOOK.dat_post) = to_date(dd_issue.data_dt)
        and CH_NOBOOK.amt_txn =  cast(dd_issue.txn_amt as double)
        and CH_NOBOOK.cod_userno = cast(dd_issue.cod_userno as int)

left outer join
        (SELECT * FROM db_smith.SMTH_POOL_NACH WHERE (data_dt) >= date_sub(to_date(CURRENT_TIMESTAMP),7)
        and (mandate_reject_date is null and mandate_reject_code is null)
        ) Nach
         on
         (case
        when (CH_NOBOOK.txt_txn_desc not LIKE 'ECS%%' and CH_NOBOOK.txt_txn_desc not like 'MAXLIFE%%')
        then
        (case
        when (TRIM(CH_NOBOOK.ref_chq_no) is null or TRIM(CH_NOBOOK.ref_chq_no) = '' or trim(CH_NOBOOK.ref_chq_no)='000000000000')
       then CH_NOBOOK.dat_txn else TRIM(CH_NOBOOK.ref_chq_no) end)
       else CH_NOBOOK.dat_txn end) = trim(Nach.TXN_REF_NO)
       and CH_NOBOOK.amt_txn =  cast(nach.txn_amt as decimal(18,2))
       AND CH_NOBOOK.cod_txn_mnemonic IN(9835,9836,9844,9845,4913)

LEFT OUTER JOIN
        (select * from db_smith.SMTH_POOL_EPI where
        data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2))EPI_irctc
       on
        (case when TRIM(CH_NOBOOK.ref_txn_no) is null or TRIM(CH_NOBOOK.ref_txn_no) = ''
       then CH_NOBOOK.dat_txn else TRIM(CH_NOBOOK.ref_txn_no) end) = trim(EPI_irctc.txn_ref_key)
           AND TO_DATE(CH_NOBOOK.DAT_TXN) = TO_DATE(EPI_irctc.DATA_DT)
    and trim(regexp_extract(CH_NOBOOK.txt_txn_desc,'[0-9]+',0)) = trim(regexp_extract(EPI_irctc.mercrefno,'[0-9]+',0))
    and CH_NOBOOK.cod_txn_mnemonic in (6922,6923)

LEFT OUTER JOIN
        (select * from db_smith.SMTH_POOL_EPI where
    data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2)
    )EPI
    on
    (case when (TRIM(CH_NOBOOK.ref_txn_no) is null or TRIM(CH_NOBOOK.ref_txn_no) = '')
       then CH_NOBOOK.dat_txn else TRIM(CH_NOBOOK.ref_txn_no) end) = trim(EPI.txn_ref_key)
    and substr(cast(CH_NOBOOK.dat_txn as string),1,13) = substr(EPI.txn_date,1,13)
    and ch_nobook.amt_txn = cast(EPI.txn_amt as decimal(18,2))
    AND CH_NOBOOK.cod_txn_mnemonic in (6919,6921)

LEFT OUTER JOIN
        (select a.* from db_smith.SMTH_POOL_RTGS a
        where a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
        )RTGS
        ON
        trim(CH_NOBOOK.ref_usr_no) = trim(rtgs.utr)
		and CH_NOBOOK.amt_txn = cast(rtgs.txn_amt as decimal(18,2))

        left outer join
    (SELECT * FROM db_smith.smth_pool_rtgs
    where concat(codstatus,acctstatus,msgstatus) in ('21524','304','1739','206','1969','41516','17224',
    '494','304','15154','4916','18164','15153','209','4915','4920','497')
    and data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7))rtgs_rev_ret
    on
    coalesce(case
    when ((CH_NOBOOK.txt_txn_desc like 'RTGS Rev%%' or CH_NOBOOK.txt_txn_desc like 'Reversal%%') and CH_NOBOOK.cod_drcr = 'C')
    then trim(substr(CH_NOBOOK.txt_txn_desc,-22))
    when (CH_NOBOOK.txt_txn_desc like 'RTGS-Return-%%' and CH_NOBOOK.cod_drcr = 'C')
    then trim(CH_NOBOOK.ref_usr_no)
    end,concat(CH_NOBOOK.dat_post,trim(CH_NOBOOK.cod_acct_no)))=coalesce(case
        when (concat(rtgs_rev_ret.codstatus,rtgs_rev_ret.acctstatus,rtgs_rev_ret.msgstatus)in('494','4920') and rtgs_rev_ret.direction ='OUT')
        THEN trim(rtgs_rev_ret.utr)
        when (concat(rtgs_rev_ret.codstatus,rtgs_rev_ret.acctstatus,rtgs_rev_ret.msgstatus) 
        not in ('494','4920') and rtgs_rev_ret.direction ='IN')
        then trim(rtgs_rev_ret.utr)
    end,concat(rtgs_rev_ret.data_dt,rtgs_rev_ret.direction,rtgs_rev_ret.utr))

left outer join
       (select * from db_smith.SMTH_POOL_IWCHQ where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2)) iwchq
      on
       (case when TRIM(CH_NOBOOK.ref_chq_no) is null or TRIM(CH_NOBOOK.ref_chq_no) = '' or trim(CH_NOBOOK.ref_chq_no)='000000000000'
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_chq_no) end) = iwchq.txn_ref_no
        and trim(CH_NOBOOK.cod_acct_no) = trim(iwchq.remitter_account_no)
        and CH_NOBOOK.amt_txn = cast(iwchq.txn_amt as double)
        and iwchq.cod_site_reject is null
        and CH_NOBOOK.ctr_batch_no = cast(iwchq.ctr_batch_no as int)


left outer join
      (SELECT * FROM db_smith.smth_pool_base_owchq where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2))owchq
      on
       trim(CH_NOBOOK.cod_acct_no)=trim(owchq.benef_account_no)
        and (case when TRIM(CH_NOBOOK.ref_chq_no) is null or TRIM(CH_NOBOOK.ref_chq_no) = '' or trim(CH_NOBOOK.ref_chq_no)='000000000000'
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_chq_no) end) = (owchq.txn_ref_no)
        and CH_NOBOOK.cod_txn_mnemonic = cast(owchq.cod_txn_mnemonic as int)
        and CH_NOBOOK.ctr_batch_no = cast(owchq.ctr_batch_no as int)
        and CH_NOBOOK.amt_txn = cast(owchq.txn_amt as double)
        and CH_NOBOOK.ref_sub_seq_no = cast(owchq.ref_sub_seq_no as int)
        and ch_nobook.ref_sys_tr_aud_no = cast(owchq.ref_sys_tr_aud_no as int)

LEFT OUTER JOIN
    (select * from db_smith.smth_pool_base_infra_trf
    where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2)
    )Intra_FT

    on (case when TRIM(CH_NOBOOK.ref_txn_no) is null or TRIM(CH_NOBOOK.ref_txn_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_txn_no) end) = trim(Intra_FT.txn_ref_no)


LEFT OUTER join
    (select * from db_smith.smth_pool_esb_ecollect
    where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7))esb_ecollect
    on (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_usr_no) end) = trim(esb_ecollect.CREDIT_REQ_REF)
        AND trim(CH_NOBOOK.cod_acct_no) = trim(esb_ecollect.benef_account_no)
        and CH_NOBOOK.cod_drcr ='C'


left outer join
        (select * from db_smith.SMTH_POOL_DC where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),2))DC
       on
        trim(CH_NOBOOK.cod_acct_no) =  trim(DC.remitter_account_no)
        and
        (case when TRIM(CH_NOBOOK.ref_txn_no) is null or TRIM(CH_NOBOOK.ref_txn_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_txn_no) end) = trim(DC.txn_ref_no)


LEFT OUTER JOIN
        (select a.cod_acct_no,a.ref_txn_no,a.retrieval_ref_no,a.cod_txn_mnemonic,a.card_no,a.amt_txn_lcy,ca_term_id,dat_post_stl
        from db_stage.stg_fcr_fcrlive_1_xf_ol_st_cotxn_mmdd a
        where batch_id >= cast(unix_timestamp(date_add(to_date(current_timestamp()),-2),'yyyy-MM-dd')*1000 as string)
        and to_date(a.dat_post_stl)>= date_sub(to_date(CURRENT_TIMESTAMP),2)
        )ATM
        on
        trim(CH_NOBOOK.cod_acct_no) =  trim(ATM.cod_acct_no)
        and trim(CH_NOBOOK.ref_txn_no) = trim(ATM.ref_txn_no)
        AND CH_NOBOOK.cod_txn_mnemonic = CAST(ATM.cod_txn_mnemonic AS INT)
        AND CH_NOBOOK.AMT_TXN_TCY = CAST(ATM.amt_txn_lcy AS DECIMAL(18,2))
        and concat(CH_NOBOOK.year,'-',CH_NOBOOK.month,'-',CH_NOBOOK.`date`) = to_date(ATM.dat_post_stl)

LEFT OUTER JOIN
         (select * from db_smith.SMTH_POOL_NEFT a
        WHERE a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
        and a.DIRECTION = 'IN')NEFT_IN
        ON
        (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_usr_no) end) = trim(NEFT_IN.iduserreference_2020)
        and CH_NOBOOK.AMT_TXN = cast(NEFT_IN.txn_amt as decimal(18,2))
        and trim(
        substr(CH_NOBOOK.txt_txn_desc,(INSTR(CH_NOBOOK.txt_txn_desc,'-')+1),
        (INSTR(CH_NOBOOK.txt_txn_desc,'-')+12 - INSTR(CH_NOBOOK.txt_txn_desc,'-')-1)
        ))= trim(NEFT_IN.remitter_ifsc)
        and CH_NOBOOK.cod_drcr = 'C'


left outer join
        (select a.* from db_smith.SMTH_POOL_NEFT a
        WHERE a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
                and DIRECTION = 'OUT'
                and concat(a.acctstatus,a.codstatus) = '94')NEFT_REV_ACK
        ON
        (case when CH_NOBOOK.cod_drcr = 'C'
                then (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_usr_no) end)  else CH_NOBOOK.dat_txn end) = trim(NEFT_REV_ACK.iduserreference_2020)
        and CH_NOBOOK.AMT_TXN = cast(NEFT_REV_ACK.txn_amt as decimal(18,2))

left outer join
        (select a.* from db_smith.SMTH_POOL_NEFT a
                WHERE a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
                and DIRECTION = 'IN'
                and TRIM(idrelatedref_2006) IS NOT NULL)NEFT_RET
                ON
                (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_usr_no) end ) = trim(NEFT_RET.iduserreference_2020)
                and trim(substr
                (CH_NOBOOK.txt_txn_desc,instr(CH_NOBOOK.txt_txn_desc,'-')+8,
                (INSTR(CH_NOBOOK.txt_txn_desc,'-')+17 - INSTR(CH_NOBOOK.txt_txn_desc,'-')-1))) = TRIM(NEFT_RET.idrelatedref_2006)
                and CH_NOBOOK.AMT_TXN = cast(NEFT_RET.txn_amt as decimal(18,2))
                AND CH_NOBOOK.cod_drcr = 'C'

LEFT OUTER JOIN
        (select a.* from db_smith.SMTH_POOL_NEFT a
        WHERE a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
        and DIRECTION = 'OUT')NEFT_OUT
        ON
        (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn else
       TRIM(CH_NOBOOK.ref_usr_no) end) = trim(NEFT_OUT.iduserreference_2020)
        and CH_NOBOOK.AMT_TXN = cast(NEFT_OUT.txn_amt as decimal(18,2))
        AND CH_NOBOOK.cod_drcr = 'D'

left outer join
        (select * from  db_smith.SMTH_POOL_IMPS
        where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
        and trim(push_pull) not in('NREPA')
        )IMPS
       ON
          (case when (CH_NOBOOK.txt_txn_desc like 'IMPS/%%' or CH_NOBOOK.txt_txn_desc like '%%/RRN%%' or CH_NOBOOK.txt_txn_desc like '%%/IMPS/RRN:%%'
            or CH_NOBOOK.txt_txn_desc LIKE '%%RRN : %%')
          then (case when trim(CH_NOBOOK.ref_txn_no) is null then ch_nobook.dat_txn else trim(CH_NOBOOK.ref_txn_no)
          end) else CH_NOBOOK.dat_txn  end)= (case when trim(IMPS.bank_ref_no) is null
                                            then concat(IMPS.data_dt,IMPS.txn_amt) else trim(IMPS.bank_ref_no) end)


LEFT OUTER JOIN
        (select * from  db_smith.SMTH_POOL_IMPS where
        data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
    and ((NPCI_RESP_CODE IN ('CE','NA','EE','M1','08','30','92','12','13','20','52','51','M2',
        'M0','M3','M4','M5','94','96','MR','M6','75','MI','22') OR NPCI_RESP_CODE IS NULL)
        AND BANK_RESPONSE_CODE NOT IN('EXT_RSP9010','EXT_RSP9035','2435')
        ))imps_ref_null
     on
    (CASE
    WHEN (
(trim(CH_NOBOOK.ref_chq_no) LIKE '0%%' or trim(CH_NOBOOK.ref_chq_no) is null)
AND (CH_NOBOOK.txt_txn_desc LIKE '%%/IMPS/RRN%%' OR CH_NOBOOK.txt_txn_desc LIKE 'IMPS%%' OR CH_NOBOOK.txt_txn_desc LIKE '%%/REV/RRN%%')
)
then
(substr(REGEXP_EXTRACT(CH_NOBOOK.txt_txn_desc,'/RRN:(.*)',1),1,instr(
REGEXP_EXTRACT(CH_NOBOOK.txt_txn_desc,'/RRN:(.*)',1),'/')-1))
else (case when (trim(CH_NOBOOK.ref_chq_no) = '' or trim(CH_NOBOOK.ref_chq_no) is null or trim(CH_NOBOOK.ref_chq_no)='000000000000')
then ch_nobook.dat_txn else trim(CH_NOBOOK.ref_chq_no)  end) end)=trim(imps_ref_null.txn_ref_no)
AND TRIM(CASE WHEN CH_NOBOOK.amt_txn < 0 THEN SUBSTR(cast(CH_NOBOOK.amt_txn_tcy AS STRING),2)
    ELSE CAST(CH_NOBOOK.amt_txn_tcy AS STRING) END) = CAST(CAST(imps_ref_null.txn_amt AS DECIMAL(18,2)) AS STRING)


LEFT OUTER JOIN
    (select a.* from db_smith.SMTH_POOL_UPI a
    where   data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
    and a.currstatuscode ='3035')UPI_REV
on
    (case when trim(substr(CH_NOBOOK.txt_txn_desc,9,12)) ='' then CH_NOBOOK.dat_txn
    else trim(substr(CH_NOBOOK.txt_txn_desc,9,12))
    end) = trim(UPI_REV.custrefno)
    and (case when upper(substr(CH_NOBOOK.txt_txn_desc,27,8)) is null or upper(substr(CH_NOBOOK.txt_txn_desc,27,8))=''
        then CH_NOBOOK.dat_txn else upper(substr(CH_NOBOOK.txt_txn_desc,27,8)) end) = substr(UPPER(UPI_REV.payer_addr),1,8)
    and CH_NOBOOK.amt_txn < 0
    and CH_NOBOOK.txt_txn_desc like 'REV:UPI/%%'


left outer JOIN
(select a.* from db_smith.SMTH_POOL_UPI a
where   a.data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
and a.paytype ='P2M'
AND a.drcrflag = 'C'
)UPI_P2M_POOL
ON
(CASE
WHEN (CH_NOBOOK.cod_drcr = 'C' and CH_NOBOOK.ref_usr_no is null)
THEN trim(substr(CH_NOBOOK.txt_txn_desc,5,12))
else CH_NOBOOK.dat_txn
end) = trim(UPI_P2M_POOL.custrefno)
and CH_NOBOOK.amt_txn = (cast(cast(UPI_P2M_POOL.txn_amt as double)/100 as decimal(18,2)))
and upper(substr(CH_NOBOOK.txt_txn_desc,instr(CH_NOBOOK.txt_txn_desc,':')+1,9)) = substr(UPPER(UPI_P2M_POOL.payer_addr),1,9)
and CH_NOBOOK.txt_txn_desc like 'UPI/%%'
and UPI_P2M_POOL.currstatuscode IN ('3030','1010','2240','3010','1030','3039','2210','1016','2228','3050','2220',
                          '3036','8000','3035','8002','8004','3038')


LEFT OUTER join
(select * from db_smith.SMTH_POOL_UPI
     where data_dt >= date_sub(to_date(CURRENT_TIMESTAMP),7)
)UPI
     ON
       (CASE    WHEN CH_NOBOOK.cod_drcr = 'C'
        THEN (case when length(CH_NOBOOK.ref_usr_no)=12
          then substr(CH_NOBOOK.txt_txn_desc,5,12)
          else (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn
    else trim(CH_NOBOOK.ref_usr_no) end) end
          )
    when (CH_NOBOOK.cod_drcr = 'D' and CH_NOBOOK.amt_txn > 0)
    then (case when TRIM(CH_NOBOOK.ref_usr_no) is null or TRIM(CH_NOBOOK.ref_usr_no) = ''
       then CH_NOBOOK.dat_txn
    else trim(CH_NOBOOK.ref_usr_no) end)
    end)= (CASE WHEN UPI.drcrflag = 'C'
            then (case when UPI.base_txn_text like 'Loading YesPay Wallet%%'
                       then trim(UPI.custrefno) else CONCAT(TRIM(UPI.TXNID),'INW')end)
        WHEN UPI.drcrflag = 'D'
        THEN CONCAT(TRIM(UPI.TXNID),'OUT') end)
    and UPI.currstatuscode IN ('3030','1010','2240','3010','1030','3039','2210','1016','2228','3050','2220',
                          '3036','8000','3035','8002','8004','3038','2230','1015')
    AND substr(CH_NOBOOK.txt_txn_desc,5,12) = trim(UPI.custrefno)

"""% (data_dt,data_dt[:4],data_dt[5:7],data_dt[8:10],ccy_batch_id,usr_batch_id,data_dt,brn_batch_id)


df=sqlContext.sql(pool).cache()

category_master=sqlContext.table(category_master_tbl)



df1=sqlContext.table(out_tbl)

df2=(df.withColumn('category_code',
    F.when(((F.col('nobook_txn_text').rlike('^IMPS\/NA\/XXX....\/REV\/RRN'))&(F.col('amount')<0))
           |(F.col('nobook_txn_text').like('%NEFT-RETURN-%'))
           |(F.col('nobook_txn_text').like('%RTGS-RETURN-%'))
           |(F.col('nobook_txn_text').rlike('^REV:UPI/'))
           ,'530000').otherwise(F.col('category_code'))))

df3=(df2.withColumn('category_level1',F.when((F.col('category_code').isin('530000')),'TRANSACTION REVERSAL').otherwise(F.col('category_level1')))
        .withColumn('category_level2',F.when((F.col('category_code').isin('530000')),'TRANSACTION REVERSAL').otherwise(F.col('category_level2')))
     )

df_classified=df3.filter(~((F.col('category_code').isNull())|(F.col('category_code').isin(''))))


df_unclassified=(df3.filter((F.col('category_code').isNull())|(F.col('category_code').isin('')))
                      .drop('category_code',
                                                                'category_level1',
                                                                'category_level2',
                                                                'category_level3',
                                                               )
                 )




root_path = '/ybl/dwh/artifacts/sherlock/pythonJobs'
#root_path='/data/08/notebooks/tmp/Anisha/Prod_code/Deployed_codes_fw/'
sc.addPyFile(root_path + '/Transaction-Classification/RemarksFW.py')
from RemarksFW import *
R_kp, R_result_dict = R_initialize(root_path, sc)


df_res=R_get_txn_class_remark(root_path, sc, df_unclassified, 'nobook_txn_text', 'category_code', R_kp, R_result_dict,'510000')



df_res1=df_res.join(F.broadcast(category_master),'category_code','left')


df_pool=df_classified.unionAll(df_res1.select(df_classified.columns))

df_pool1=(df_pool.withColumn('category_code',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), '130100').
                            otherwise(F.col('category_code'))
                            )
                .withColumn('category_level1',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), 'PERSONAL TRANSFERS').
                            otherwise(F.col('category_level1'))
                            )
                .withColumn('category_level2',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), 'TRANSFER SELF').
                            otherwise(F.col('category_level2'))
                            )
    )


##EEM Integration script changes
### EEM Integration
def clean_account(acct_col):
    return F.regexp_replace(F.trim(F.col(acct_col)),"^0+","")
def get_ifsc_4(ifsc_col):
    return F.upper(F.substring(F.trim(F.col(ifsc_col)), 1, 4))

eem_master_df = (sqlContext.table(eem_master)
                 .filter(F.col("is_validated")==1)
                 .select(F.col("key").alias("entity_id"),
                         F.col("category_code").alias("correct_category"))
                )
eem_mapper_df = sqlContext.table(eem_mapper).filter((F.col("entity_id").isNotNull())&(F.col("cust_category")!='I'))
eem_mapper_df1 = eem_mapper_df.join(F.broadcast(eem_master_df),'entity_id')

df_pool11 = (df_pool1.withColumn("classification_flag",
                                 F.when(F.col("category_code").isin(["510000","170000"]),F.lit(0)).otherwise(F.lit(1)))
             .withColumn("benef_account_no_clean",F.trim(clean_account("benef_account_no")))
             .withColumn("benef_ifsc_clean",F.upper(F.trim(get_ifsc_4("benef_ifsc"))))
             .withColumn("benef_name_clean",F.lower(F.trim(F.col("benef_name"))))
             .withColumn("key",F.concat_ws('_',*[F.col("benef_ifsc_clean"),F.col("benef_account_no_clean")]))
            )

df_pool111 = df_pool11.filter(F.col("classification_flag")==0)
eem_mapper_df2 = (eem_mapper_df1.withColumn("benef_ifsc_clean",F.upper(F.trim(get_ifsc_4("ifsc"))))
                             .withColumn("benef_account_no_clean",F.trim(clean_account("account_number")))
                             .withColumn("benef_name_clean",F.lower(F.trim(F.col("suggested_name"))))
                             .withColumn("category_code_eem",F.col("correct_category"))
                 )

#EEM Integration Method 1
eem_mapper_df21 = (eem_mapper_df2.filter(F.col('key').isNotNull()))
df_pool1111 = (df_pool111.join(F.broadcast(eem_mapper_df21.select("key",
                                                       "business_name",
                                                       "category_code_eem")
                                        .distinct())
                            ,on=["key"],how="left")
            ).drop('key')

df_pool_method1 = df_pool1111.filter(F.col("category_code_eem").isNotNull())
df_pool_method2 = (df_pool_method1
                   .withColumn("benef_id",
                               F.when(F.col("business_name").isNotNull(),F.col("business_name"))
                               .otherwise(F.col("benef_id"))
                              )
                  )

df_pool_method1_res = df_pool1111.filter(F.col("category_code_eem").isNull()).drop(*['business_name','category_code_eem'])

#EEM Integration Method 2
eem_mapper_df22 = (eem_mapper_df2.filter(F.col('benef_name_clean').isNotNull()))
w = Window.partitionBy(F.col("benef_name_clean")).orderBy(F.col('entity_id').desc())
eem_mapper_df22 = eem_mapper_df22.withColumn('r',F.row_number().over(w)).filter(F.col('r')==1).drop("r")
df_pool_method1_res1 = (df_pool_method1_res.join(F.broadcast(eem_mapper_df22.select("benef_name_clean",
                                                       "business_name",
                                                       "category_code_eem")
                                        .distinct())
                            ,on=["benef_name_clean"],how="left")
            ).drop('benef_name_clean')

cols = df_pool1.columns+['business_name','category_code_eem']
df_pool_eem_final = df_pool_method1_res1.select(cols).union(df_pool_method1.select(cols))
df_pool_eem_final1 = (df_pool_eem_final
                   .withColumn("benef_id",
                               F.when(F.col("business_name").isNotNull(),F.col("business_name"))
                               .otherwise(F.col("benef_id"))
                              )
                  )
df_pool_eem_final2 = (df_pool_eem_final1
                      .withColumn('category_code',
                                  F.when(F.col('category_code_eem').isNull(),F.col('category_code')
                                        ).otherwise(F.col('category_code_eem'))
                                 )
                     ).drop(*['category_code_eem','category_level1','category_level2','category_level3'])
# integration with Category Master

df_pool_eem_final3=df_pool_eem_final2.join(F.broadcast(category_master),'category_code','left')


df_pool_eem_final4=df_pool_eem_final3.select(df_pool1.columns)

df_pool_eem_final5=(df_pool_eem_final4.withColumn('category_code',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), '130100').
                            otherwise(F.col('category_code'))
                            )
                .withColumn('category_level1',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), 'PERSONAL TRANSFERS').
                            otherwise(F.col('category_level1'))
                            )
                .withColumn('category_level2',F.when( (F.col('self_flag').isin('1'))
                                                        &(F.col('category_code').isin('510000')), 'TRANSFER SELF').
                            otherwise(F.col('category_level2'))
                            )
    )

df_pool_seg1 = df_pool11.filter(F.col("classification_flag")==1).select(df_pool1.columns)
df_pool_seg2 = df_pool_eem_final5.select(df_pool1.columns)
df_pool_with_eem = df_pool_seg1.union(df_pool_seg2)




df_pool_with_eem.select(df1.columns).repartition('data_dt','partition_mode').write.insertInto(out_tbl,True)













































---------------------------------------------------------------------------------------------------------------------
# coding: utf-8

# In[16]:

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
import sys

# ---- initializations ----

# create spark context
sc = SparkContext()

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

# for compatibility
sqlCtx = sqlContext


# In[29]:

import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime, timedelta
from dateutil import relativedelta
from ConfigParser import ConfigParser
from dateutil import parser
from dateutil import relativedelta


# In[31]:

#Ini file dynamic variables reading
configFile = sys.argv[1]
#configFile = '/data/08/notebooks/tmp/Devesh/RemarkUtil/smth_latest_month_incremental_pool.ini'
config = ConfigParser()
config.read(configFile)


INP_DB_NM_1 = config.get('default', 'INP_DB_NM_1')
INP_TBL_NM_1 = config.get('default','INP_TBL_NM_1')

OUT_DB_NM = config.get('default', 'OUT_DB_NM')
OUT_TBL_NM = config.get('default','OUT_TBL_NM')

folder_name=OUT_TBL_NM.lower()

# In[32]:

current_date = config.get('default','MASTER_DATA_DATE_KEY').replace('"', "").replace("'","")
transaction_pool_table_name = INP_DB_NM_1+'.'+INP_TBL_NM_1
incremental_table_name = OUT_DB_NM+'.'+OUT_TBL_NM


# In[33]:

q_create_incremental_table ="""create EXTERNAL table if not exists %s
(
cod_acct_no string,
cust_id string,
source string,
category_code string,
d_c string,
channel string,
amount_m0 double,
txn_count_m0 int,
last_txn_date string,
last_txn_amount double,
remitter_ifsc string,
benef_ifsc string,
mode string,
data_dt string
)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://nameservice1/ybl/smith/genie/hive/%s'
"""


q_insert_incremental = """select trim(cod_acct_no) as cod_acct_no,
            cust_id,
            source,
            category_code,
            d_c,
            channel,
            sum((cast(amount as float))) as amount_m0,
            count(channel) txn_count_m0,
            to_date(max(dat_time_txn)) as last_txn_date,
            max((cast(amount as float))) as last_txn_amount,
            substr(remitter_ifsc,1,4) as remitter_ifsc,
            substr(benef_ifsc,1,4) as benef_ifsc,
            mode,
            current_date as data_dt
      from transaction_pool_table_name
      where to_date(data_dt)>=(date_add(current_date,1 - day(current_date) )) and
       to_date(dat_value) between (date_add(current_date,
             1 - day(current_date) )) and last_day(current_date)
     group by cod_acct_no, cust_id, source, category_code,d_c,channel,substr(remitter_ifsc,1,4),substr(benef_ifsc,1,4),mode
     """

def lastOfMonth(dtDateTime):
    dYear = dtDateTime.strftime("%Y")
    dMonth = str(int(dtDateTime.strftime("%m"))%12+1)
    dDay = "01"
    nextMonth = datetime.strptime(dYear+'-'+dMonth+'-'+dDay,'%Y-%m-%d').date()
    delta = timedelta(days=1)    #create a delta of 1 second
    return nextMonth - delta


def createQuery(query,incremental_table_name,transaction_pool_table_name):
    return (query.replace('transaction_pool_table_name',transaction_pool_table_name)
            .replace('incremental_table_name',incremental_table_name)
           )

def createAndWriteTable(createQuery,insertQuery,tableName,folder_name,current_date,overwrite_mode=True):
    sqlContext.sql(createQuery%(tableName,folder_name))
    insertQuery = insertQuery.replace('current_date', "to_date('%s')"%current_date)
    cols = sqlContext.table(tableName).columns
    df = sqlContext.sql(insertQuery).select(cols)
    df.write.insertInto(tableName, overwrite_mode)


def insertTable(insertQuery,tableName,current_date,overwrite_mode=True):
    insertQuery = insertQuery.replace('current_date', "to_date('%s')"%current_date)
    df = sqlContext.sql(insertQuery)
    cols = sqlContext.table(tableName).columns
    df = df.select(cols)
    df.write.insertInto(tableName, overwrite_mode)


def runOneTimeTxn(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date):
    try :
        df = sqlContext.table(incremental_table_name)
        data_dt = sqlContext.table(incremental_table_name).select('data_dt').orderBy(F.col('data_dt').desc()).first()
        if data_dt ==None:
            sqlContext.sql('truncate table %s'%incremental_table_name)
            createAndWriteTable(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date,True)
    except :
        createAndWriteTable(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date,True)

def runDailytxn(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date):

    data_dt = sqlContext.table(incremental_table_name).select('data_dt').orderBy(F.col('data_dt').desc()).first().data_dt

    date1 = datetime.strptime(data_dt,'%Y-%m-%d').date()
    date2 = datetime.strptime(current_date,'%Y-%m-%d').date()

    if date1<date2:
        date1 = datetime.strptime(datetime.strftime(date1,'%Y-%m-01'),'%Y-%m-%d').date()
        date2 = datetime.strptime(datetime.strftime(date2,'%Y-%m-01'),'%Y-%m-%d').date()

        if (relativedelta.relativedelta(date2,date1).months<=1):
            insertTable(q_insert_incremental,incremental_table_name,current_date,True)

        if (relativedelta.relativedelta(date2,date1).months>1):
            sqlContext.sql("truncate table %s"%incremental_table_name)
            runOneTimeTxn(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date)


def runScheduler(q_create_incremental_table,q_insert_incremental,incremental_table_name,transaction_pool_table_name,folder_name,current_date):
    if current_date==None:
        current_date = (datetime.today()- timedelta(days=1)).strftime('%Y-%m-%d')

    q_insert_incremental = createQuery(q_insert_incremental,incremental_table_name,transaction_pool_table_name)
    runOneTimeTxn(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date)
    runDailytxn(q_create_incremental_table,q_insert_incremental,incremental_table_name,folder_name,current_date)

runScheduler(q_create_incremental_table,q_insert_incremental,incremental_table_name,transaction_pool_table_name,folder_name,current_date)
------------------------------------------------------------------------------------------------
POS--------------
# coding: utf-8

# In[72]:

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


# In[73]:

configFile = sys.argv[1]
#configFile = "/data/08/notebooks/tmp/Anisha/Pooling_table/cc_sample.ini"
config = ConfigParser.ConfigParser()
config.read(configFile)
data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY',)
data_dt = data_dtt.strip('"').strip("'")
#data_dt='2019-06-02'
#cust_mst_tbl_name = config.get('contactability','cust_mst_tbl_name')
cc_tbl = config.get('default','INP_DB_NM_1') + '.' +  config.get('default','INP_TBL_NM_1')   
cc_tbl_df=sqlContext.table(cc_tbl)
cc_tbl1= cc_tbl_df.filter(F.col('data_dt')==data_dt).drop('category_code','category_level1','category_level2','category_level3','benef_id')

category_mast_tbl = config.get('default', 'INP_DB_NM_2') + '.' +  config.get('default','INP_TBL_NM_2') 
category_master = sqlContext.table(category_mast_tbl).drop('batch_id')

#cc_code_mast_tbl = config.get('default', 'INP_DB_NM_3') + '.' +  config.get('default','INP_TBL_NM_3') 
cc_code_master = sqlContext.table('db_stage.stg_cc_category_mapping')

output_tbl = config.get('default', 'OUT_DB_NM') + '.' +  config.get('default','OUT_TBL_NM_1')


# In[17]:

#cc.select(F.col('transaction_sub_cat'),F.col('transaction_net_cat')).distinct().count()


# In[18]:

#cc_code_master=sqlContext.table('db_stage.stg_cc_category_mapping')


# In[19]:

#category_master=sqlContext.table('db_stage.stg_fle_category_master')


# In[74]:

#sc.applicationId


# In[75]:

rootpath='/ybl/dwh/artifacts/sherlock/pythonJobs'
sc.addPyFile(rootpath + '/Transaction-Classification/PosFW.py')
from PosFW import *
kp = initialize(rootpath,sc)


# In[76]:

#cc_table=sqlContext.table('db_smith.smth_transaction_cc_pool').drop('category_code','category_level1','category_level2','category_level3')


# In[77]:

cc_table1=cc_tbl1.filter(F.col('transaction_category').isin('SPENDS'))


# In[78]:

cc_table2=get_pos_txn_cat(sc, sqlContext, cc_table1,'nobook_txn_text','mt_category_code','benef_id','category_code', kp)

cc_table2a=cc_table2.withColumn('benef_name',F.trim(
        F.regexp_replace(
            F.regexp_replace(F.col('nobook_txn_text'),'[^A-Z0-9_?a-z?]',' '),'  +',' ')
                                ))
# In[25]:

#cc_table1.count()


# In[26]:

#cc_table2.groupBy('category_code_FW').count().orderBy(F.col('count').desc()).take(200)


# In[79]:

cc_table3=cc_tbl1.filter(~(F.col('transaction_category').isin('SPENDS'))|(F.col('transaction_category').isNull()))


# In[80]:

cc_table4=cc_table3.join(cc_code_master,['transaction_net_cat','transaction_sub_cat'],'left').withColumn('benef_id',F.lit(''))


# In[81]:

cc_all=cc_table2a.select(cc_table2a.columns).unionAll(cc_table4.select(cc_table2a.columns))


# In[82]:

cc_all1=(cc_all.withColumn('category_code',F.when(F.col('base_txn_text').like('%PAYMENT%RECEIVED%'),'250000')
                          .otherwise(F.when(F.col('base_txn_text').like('%REWARDS%REDEMPTION%'),'521900')
                                     .otherwise(F.col('category_code')))))


# In[83]:

cc_all_final=cc_all1.join(category_master,'category_code','left')


# In[84]:

pool=sqlContext.table('db_smith.smth_transaction_pool')


# In[70]:

final_df=cc_all_final.select(pool.columns)


# In[ ]:

final_df.write.insertInto(output_tbl, True)


































----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Remarks.py

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
#sc.addPyFile("textutils/viktext.py")
#from viktext import KeywordProcessor


class RemarksFW():
    def __init__(self, kp_b, result_dict_b, default_cat):
        self.kp_b = kp_b
        self.result_dict_b = result_dict_b
        self.default_cat = default_cat
        
    def get_branch(self, start, end, startswith, category_set, level):
        rem = ''
        c2 = Counter([x[start:end] for x in category_set if x.startswith(startswith) and x[start:end] != '00'])
        level2 = [x[0] for x in c2.most_common(5)]
        if len(level2) == 1:
            l2 = level2[0]
        elif len(level2) == 0:
            l2 = '00'
        else:
            l2 = '00'
            rem += 'Conflict at level ' + level + ' '
        return l2, rem

    def resolve_deeper(self, start, end, category_set):
    #     print start, end, category_set[0][start:end], category_set[1][start:end]
        if category_set[0][start:end] != '00' and category_set[1][start:end] == '00':
            return category_set[0]
        elif category_set[0][start:end] == '00' and category_set[1][start:end] != '00':
            return category_set[1]


    def get_deepest_category_among_two(self, category_set):
        res = self.resolve_deeper(4,6, category_set)
        if res is None:
            res = self.resolve_deeper(2,4, category_set)
        return res

    
    def conflict_resolver(self, category_set):
        category_set = list(category_set)

        if len(category_set) == 0:
            return self.default_cat, "No match Found"
        else:
            common_two = Counter(category_set).most_common(2)
            if len(common_two) == 2 and common_two[0][1] == common_two[1][1] and common_two[0][0][:2] != common_two[1][0][:2]:
                category_set = [x[0] for x in common_two]
                category = self.get_deepest_category_among_two(category_set)
                if category:
                    return category, "Resolved to deeper between two different primary categories"
                else:
                    return self.default_cat, 'Conflict couldnot be resolved'
            else:    
                remark = ''
                c = Counter([x[:2] for x in category_set])
                l1 = c.most_common(1)[0][0]
                category_set = list(set(category_set))
                l2, rem = self.get_branch(2, 4, l1, category_set, '2')
                remark += rem
                l3, rem = self.get_branch(4, 6, l1+l2, category_set, '3')
                remark += rem
                return l1+l2+l3, remark.strip()
    
    def registerudf(self):
        return F.udf(self.main_remarks_category, T.StringType())

    def main_remarks_category_old(self, remark):
        try:
            if remark:
                words=self.kp_b.value.extract_keywords(remark)
                words_set = set(words)
                res = []
                for ele in words_set:
                    for set1 in self.result_dict_b.value[ele]:
                        if ((words_set >= set(set1))):
                            res.append(self.result_dict_b.value[ele][set1])
                r = self.conflict_resolver(res)[0]
                return r
            else:
                return self.default_cat
        except:
            return self.default_cat
    
    def main_remarks_category(self, remark):        
        if remark:
            words=self.kp_b.value.extract_keywords(remark)
            words_set = set(words)
            res = []
            for ele in words_set:
                for set1 in self.result_dict_b.value[ele]:
                    if ((words_set >= set(set1))):
                        res.append(self.result_dict_b.value[ele][set1])
            #print 'Matched Categories-', res
            if len(res) == 0:
                return self.default_cat
            res_res = Counter(res).most_common()
            if len(res_res) == 1:
                #print 'after conflict resolution-', res_res[0][0]
                return res_res[0][0]
            
            elif len(res_res) == 2 and res_res[0][0].startswith('13') and not res_res[1][0].startswith('13'):
                return res_res[1][0]

            elif len(res_res) == 2 and res_res[1][0].startswith('13') and not res_res[0][0].startswith('13'):
                return res_res[0][0]
                
            elif Counter(res).most_common()[0][1] > Counter(res).most_common()[1][1]*2:
                #print 'after conflict resolution-', res_res[0][0]
                return res_res[0][0]
            else:
                r = self.conflict_resolver(res)[0]
                #print 'after conflict resolution-', r
                return r
        else:
            return self.default_cat
    
        
    def main_remarks_category_tester(self, remark):        
        if remark:
            words=self.kp_b.value.extract_keywords(remark)
            words_set = set(words)
            res = []
            for ele in words_set:
                for set1 in self.result_dict_b.value[ele]:
                    if ((words_set >= set(set1))):
                        res.append(self.result_dict_b.value[ele][set1])
            print 'Matched Categories-', res
            if len(res) == 0:
                print self.default_cat
                return self.default_cat
            res_res = Counter(res).most_common()
            if len(res_res) == 1:
                print 'after conflict resolution-', res_res[0][0]
                return res_res[0][0]
            elif Counter(res).most_common()[0][1] > Counter(res).most_common()[1][1]*2:
                print 'after conflict resolution-', res_res[0][0]
                return res_res[0][0]
            else:
                r = self.conflict_resolver(res)[0]
                print 'after conflict resolution-', r
                return r
        else:
            return self.default_cat

            
def R_combine(a_list):
    res = []
    for i, ele in enumerate(a_list):
        k = ''.join(a_list[:i]) + ' ' + ''.join(a_list[i:])
        res.append(k.strip()) 
    return res

def R_combine2(a_list):
    res = []
    for i, ele in enumerate(a_list):
        k = ''.join(a_list[:i]) + ' ' + ''.join(a_list[i:])
        k2 = ''.join(a_list[:i])  + ' '+ ''.join(a_list[i:]) +'s'
        res.append(k.strip())
        res.append(k2.strip()) 
    return res 


    
def R_get_keywords_from_csv(filename):
    result_dict = {}
    cat_key_mapp={}
    from itertools import permutations,chain
    with open(filename, 'rb') as filereader:
        rd = csv.reader(filereader)
        for line in rd:
            code = line[3]
            keywords = line[4].lower().replace('[','') \
                              .replace(']','').replace("'","").replace('0', '').replace('\n', '').strip().split(',')
            cat_key_mapp[code]=(line[1]+" "+line[2]).strip()
            for ele in keywords:
                if len(ele.strip()) > 1:
                    if len(ele.split()) > 2:
                        k2 = [R_combine(x) for x in list(permutations(ele.split()))]
                        merged = list(chain(*k2))
                        merged.append(ele)
                    else:
                        k2 = [R_combine2(x) for x in list(permutations(ele.split()))]
                        merged = set(list(chain(*k2)))
                    for elem in merged:    
                        k1 = elem.split(' ')
                        for k11 in k1:
                            if k11 != '':
                                k11.strip()
                                if k11 not in result_dict:
                                    result_dict[k11] = {}
                                result_dict[k11][tuple(elem.split())] = code
    return result_dict

def R_initialize(root_path, sc, fname = '/Transaction-Classification/MasterData/Txn_Classification_28March.csv'):
    """
    root_path -> directory where Transaction-Classification folder is present
    sc -> spark context
    fname -> path to csv containing remarks master, default: './Transaction-Classification/MasterData/Txn_Classification_28March.csv'
    """
    sc.addPyFile(root_path + '/Transaction-Classification/textutils/viktext.py')
    from viktext import KeywordProcessor
    result_dict = R_get_keywords_from_csv(root_path + fname)
    kp = KeywordProcessor()
    kp.add_keywords_from_list([k for k in result_dict])
    return kp, result_dict
    

def R_textcleaner2(df, col_name, regex_list = [r'[^A-Za-z]+', r'\bNULL\b', r'\s+'], i = 0):
    if i == len(regex_list):
        return df.withColumn('Remarks_clean', F.upper(F.trim(col_name)))    
    else:
        funct = F.regexp_replace(col_name, regex_list[i], ' ')
        return R_textcleaner2(df, col_name = funct, i = i+1)
    
def R_extractRegex2(default, benif_col, regex_list,  i = 0):
    if i == len(regex_list):
        return default
    else:
        return F.when(benif_col.rlike(regex_list[i][0]),regex_list[i][1]) \
                .otherwise(R_extractRegex2(default, benif_col, regex_list, i = i+1))
    
def R_get_txn_class_remark(root_path, sc, df, remark_col, category_col, kp, result_dict, default_cat = '510000', R_name=None, B_name=None, self_transfer_flag_col = '__temp_self__', broadcasting=False):
    """
    root_path -> directory where Transaction-Classification folder is present
    sc -> Spark context
    df -> Transaction Dataframe
    remark_col -> column name which would contain textual information about transaction
    category_col -> output column name which will be populated with category code (ensure this column doen't pre-exist in df)
    kp -> Keyword processor object you get through initilization
    result_dict -> dictonary you get through initilization
    default_cat -> Default category code
    R_name -> column containing Remitter name
    B_name -> column containing Beneficiary name
    self_transfer_flag_col -> output column which would contain self transfer flag (0/1)
        
    **IMP: don't forget to add jar incase you are calculating self flag -> ./jars/subset_udf.jar**

    """
    sc.addPyFile(root_path + '/Transaction-Classification/selfTransferFW.py')
    from selfTransferFW import *
    #kp = kp1
    #result_dict = result_dict1
    kp_b = sc.broadcast(kp)
    result_dict_b = sc.broadcast(result_dict)
    R_fw = RemarksFW(kp_b, result_dict_b, default_cat)
    main_remarks_categoryUDF = R_fw.registerudf()
    
    ff = R_textcleaner2(df, F.col(remark_col))
    ff = ff.fillna('NA', subset=['Remarks_clean'])
    df_remarks_clean = ff.select(F.col('Remarks_clean')).dropDuplicates(['Remarks_clean'])
    
    res = df_remarks_clean.withColumn(category_col, main_remarks_categoryUDF(F.col('Remarks_clean')))
    res = res.filter( ( (F.col(category_col) != default_cat) | 
                        (~F.col(category_col).isNull() ) 
                      ) )
    
    
    if broadcasting == True:
        #experiment
        res = res.cache()
        print (res.count())
        df_joined = ff.join(F.broadcast(res), 'Remarks_clean', 'left')
    else:
        df_joined = ff.join(res, 'Remarks_clean', 'left')
    #df_joined2 =  df_joined.drop(F.col('Remarks_clean'))
    R_regex_list = [[r'((?i).*[A-Za-z]{3}\s*(sal|salary)\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec))', '180100']]
    
    if R_name != None and B_name != None and self_transfer_flag_col != None:
        df_joined2 = getSelfTransferFlag(sc, df_joined, R_name, B_name, self_transfer_flag_col)
    
        df_final = df_joined2.withColumn(category_col, F.when(((F.col(category_col) == default_cat) &
                                                          (F.col(self_transfer_flag_col) == 1)), '130100') \
                                                    .otherwise(F.col(category_col)))
        df_final = df_final.withColumn(category_col , F.when( (F.col(category_col).isNull()),
                                                             R_extractRegex2(F.col(category_col), F.col('Remarks_clean'), R_regex_list))
                                       .otherwise(F.col(category_col)) )
        
        #df_final = df_final.withColumn(category_col, R_extractRegex2(F.col(category_col), F.col('Remarks_clean'), R_regex_list))
                                       
        
        
        df_final = df_final.withColumn(category_col, F.when(F.col(category_col).isNull(),
                                                              default_cat).otherwise(F.col(category_col))
                                               )
        return df_final.drop(F.col('Remarks_clean'))
    else:
        df_joined = df_joined.withColumn(category_col , F.when( (F.col(category_col).isNull()), R_extractRegex2(F.col(category_col), F.col('Remarks_clean'), R_regex_list)).otherwise(F.col(category_col)) )
        #df_joined = df_joined.withColumn(category_col ,R_extractRegex2(F.col(category_col), F.col('Remarks_clean'), R_regex_list))
        
        df_joined = df_joined.withColumn(category_col, F.when(F.col(category_col).isNull(),
                                                              default_cat).otherwise(F.col(category_col))
                                       )
                                       
        return df_joined.drop(F.col('Remarks_clean'))
'''
def get_main(sc):
    csv_file = 'MasterData/Txn_Classification_28March.csv'
    kp1, result_dict1 = initialize(sc, csv_file)
    kp_b = sc.broadcast(kp1)
    result_dict_b = sc.broadcast(result_dict1)
    R_fw = RemarksFW(kp_b, result_dict_b)
    main_remarks_categoryUDF = R_fw.registerudf()
    return main_remarks_categoryUDF
'''

if __name__ == '__main__':
    'Following is an example of using This framework-'
    sc = SparkContext()
    try:
        # Try to access HiveConf, it will raise exception if Hive is not added
        sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        sqlContext = HiveContext(sc)
    except py4j.protocol.Py4JError:
        sqlContext = SQLContext(sc)
    except TypeError:
        sqlContext = SQLContext(sc)
    
    kp, result_dict = R_initialize(sc)

    table = 'db_smith.smth_pool_neft'
    df = sqlContext.read.table(table)

    df_res = R_get_txn_class_remark(sc, df, 'base_txn_text', 'category_code_FW', kp, result_dict)
    #df_res = df_res
    #df_res.count()
    df_res.show()



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
