import numpy as np
import pandas as pd
import datetime
import sys
import cx_Oracle

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

sc = SparkContext.getOrCreate()
sc.stop()

import pyspark.sql.functions as F
from pyspark.sql.functions import countDistinct as CD

now = datetime.datetime.now()

txntable = 'udm_cds_transactions0124'
accttable = 'udm_cds_account0109'

conf = (SparkConf()
         .setMaster("yarn-client")
         .setAppName("Xin_TXN_sup_step1")
         .set("spark.num.executors", "5")
         .set("spark.executor.cores","5")
         .set("spark.driver.cores","2")
         .set("spark.driver.memory", "5g")
         .set("spark.executor.memory", "5g")
         .set("spark.dynamicAllocation.enabled","true"))
sc = SparkContext(conf = conf)
hive_context = HiveContext(sc)
hive_context.sql("use risk")
sc.setLogLevel("ERROR")

#connect SQL db to load alert data
username = 'SEC_BKT'
pwd ='secbkt93#41'
servname = 'vsam'
hoststr = 't53oraoda921t1a'
portnum = '1521'
dsn_tns = cx_Oracle.makedsn(hoststr, portnum, service_name=servname) #if needed, place an 'r' before any parameter in order to address any special character such as '\'.
conn = cx_Oracle.connect(user=username, password=pwd, dsn=dsn_tns) 
query = 'SELECT * FROM sec_bkt.alert_mar'
supervised_pd = pd.read_sql(query, con=conn)

#correct variables type by name
datevars = [colu for colu in supervised_pd.columns if 'DATE' in colu]
keyvars = [colu for colu in supervised_pd.columns if 'KEY' in colu]
supervised_pd[datevars]= supervised_pd[datevars].apply(pd.to_datetime, errors='coerce')
supervised_pd[keyvars]= supervised_pd[keyvars].astype(str)
supervised_pd.columns = map(str.lower,supervised_pd.columns)

#remove alerts generated after case
# supervised_pd = supervised_pd.loc[~ (supervised_pd.alert_create_date > supervised_pd.case_create_date)] ###removed this filter when coverage assessment for SAR

#case link fix 1, remove the link those linked but not interesting ones
supervised_pd.loc[supervised_pd.alert_rank.isnull(),'alert_rank'] = '-1'
supervised_pd['case_id_fix1'] = supervised_pd['case_id'] 
supervised_pd.loc[lambda row: (row['case_id'].notnull()) & (row['alert_rank'].astype('str').isin(['0','1','2'])) ,'case_id_fix1'] = None

## fix data using donna's input
donna_case_fix = pd.read_csv("/home/wangx17/sup_workbooks/donna_case_fix.csv") #fix 432 alert and 306 account
donna_no_case_fix = pd.read_csv("/home/wangx17/sup_workbooks/donna_no_case_fix.csv")

donna_case_fix.columns = map(str.lower,donna_case_fix.columns)
donna_no_case_fix.columns = map(str.lower,donna_no_case_fix.columns)

donna_case_treat = donna_case_fix.loc[lambda df: (df.case_id.str.startswith('SAI') |  df.case_id.str.startswith('MAN') | df.case_id.str.startswith('C')| df.case_id.str.startswith('SAM')) | df.form_info.isin(["SAR","NONSAR","Non SAR"]) ].copy() #need to check why have sam started cases!
donna_case_treat['case_id'] = donna_case_treat.case_id.combine_first(donna_case_treat.form_info)  #some of donnas fix still say SAR/NONSAR though case number were not provided, here trust her input since manually done
donna_notcase_treat = donna_no_case_fix.loc[lambda df: (df.donna_highlighted ==1)].copy() #"There were some that had no case relationship, but were ranked for a case. Those have been identified in red as well."
donna_notcase_treat['case_id_fix2'] = 'NULL'
donna_case_treat['case_id_fix2'] = donna_case_treat.case_id
donna_fix = donna_case_treat[['alert_id','case_id_fix2']].append(donna_notcase_treat[['alert_id','case_id_fix2']])

#merge back
supervised_pd_fix = supervised_pd.merge(donna_fix,on= "alert_id",how = "left")

#combine:
supervised_pd_fix['case_id_raw'] = supervised_pd_fix['case_id']
supervised_pd_fix['case_id'] = supervised_pd_fix.case_id_fix2.combine_first(supervised_pd_fix.case_id_fix1) #fist is first latter is second
supervised_pd_fix.loc[supervised_pd_fix.case_id =='NULL','case_id'] = None

# supervised_pd_fix['account_key'] = supervised_pd_fix['final_account_key']
supervised_pd_fix['is_case_raw'] = 0
supervised_pd_fix.loc[supervised_pd_fix.case_id_raw.notnull(),'is_case_raw'] = 1
supervised_pd_fix['is_case'] = 0
supervised_pd_fix.loc[supervised_pd_fix.case_id.notnull(),'is_case'] = 1
supervised_pd_fix['is_sar_raw'] = 0
supervised_pd_fix.loc[supervised_pd_fix.last_sar_id.notnull(),'is_sar_raw'] = 1
supervised_pd_fix['is_sar'] = supervised_pd_fix['is_sar_raw'] * supervised_pd_fix['is_case']

supervised_pd, supervised_pd_beforefix = supervised_pd_fix, supervised_pd

#use alert data to filter txn data to reduce computation/memory
supervised_pd_filter = supervised_pd[['final_account_key','alert_month_sk']].drop_duplicates()
supervised_pd_filter.columns = ['account_key','alert_month_sk']
supervised_filter = hive_context.createDataFrame(supervised_pd_filter)

#generate monthly txn summary
# sam_txn = hive_context.table(txntable).where("month_sk >=216").withColumn('abs_value',F.abs(F.col('acct_curr_amount')))
# sam_acct = hive_context.table(accttable).where("is_error_account is null").dropDuplicates()
# sam_txn_acctsmry = sam_txn.where("acct_curr_amount<>0").groupBy(["account_sk","month_sk"])\
#                     .agg(F.sum('abs_value').alias('total_value'),CD('transaction_key').alias('total_volume')).alias('t')\
#                     .join(sam_acct.alias('a'),F.col('t.account_sk')==F.col('a.entity_sk'),'left')\
#                     .selectExpr('a.account_key','a.account_product_sk','a.account_type_sk',"t.*").alias('t2')\
#                     .join(supervised_filter.alias('s'),[F.col('t2.account_key')==F.col('s.account_key'),F.col("t2.month_sk") +1 == F.col("s.alert_month_sk")], "inner")\
#                     .selectExpr("t2.*","s.alert_month_sk").distinct()
# sam_txn_acctsmry_pd = sam_txn_acctsmry.toPandas()

#merge alert data and txn summary
supervised_pd_common = supervised_pd.rename(index=str,columns={"account_key": "fort_account_key","final_account_key":"account_key","alert_month_sk":"alert_month_sk"})
# supervised_pd_common.loc[supervised_pd_common.alert_rank.isnull(),'alert_rank'] = '-1'


#EXT alert only keep the top valued account
#Customer SB only keep top 2 valued account based on its logic that second largest (top2) accounts over threshold:
# supervised_pd_EXT = supervised_pd_common.loc[supervised_pd_common.alert_id.str[:3]=='EXT']
# supervised_pd_EXT = supervised_pd_EXT.sort_values(['alert_id','alert_create_date','total_value','account_key'],ascending = [True,True,False,True])
# maxvalue_idx  = (supervised_pd_EXT.groupby(['alert_id','alert_create_date'])['total_value'].transform(lambda x: x.max())== supervised_pd_EXT['total_value'])
# supervised_pd_EXT_fixed = supervised_pd_EXT[maxvalue_idx]

# supervised_pd_CUSTSB = supervised_pd_common.loc[supervised_pd_common.detail_alert_type=='Customer Security Blanket'].copy()
# supervised_pd_CUSTSB['top2value'] = supervised_pd_CUSTSB.groupby(['alert_id','alert_create_date'])['total_value'].transform(lambda x: x.nlargest(2).min())
# supervised_pd_CUSTSB_fixed = supervised_pd_CUSTSB.loc[supervised_pd_CUSTSB.total_value >= supervised_pd_CUSTSB.top2value]

# supervised_pd_other = supervised_pd_common[~(supervised_pd_common.alert_id.isin(supervised_pd_CUSTSB.alert_id) | supervised_pd_common.alert_id.isin(supervised_pd_EXT.alert_id) )]
# supervised_pd_fixed = supervised_pd_other.append([supervised_pd_CUSTSB_fixed,supervised_pd_EXT_fixed])

supervised_pd_fixed = supervised_pd_common

datevars = [colu for colu in supervised_pd_fixed.columns if 'date' in colu]
keyvars = [colu for colu in supervised_pd_fixed.columns if 'key' in colu]
supervised_pd_fixed[datevars]= supervised_pd_fixed[datevars].apply(pd.to_datetime, errors='coerce')
supervised_pd_fixed[keyvars]= supervised_pd_fixed[keyvars].astype(str)
supervised_pd_fixed['is_SB'] = supervised_pd_fixed.detail_alert_type.isin(['Account Security Blanket','Customer Security Blanket']).astype(int)
supervised_pd_fixed['alert_rank'] = supervised_pd_fixed['alert_rank'].astype(int)

#zhenqi duplicate txn fix
SB_duplicate_clean = pd.read_csv("/home/wangx17/sup_workbooks/clean_case.csv")
SB_duplicate_clean['is_SB'] = 1
SB_duplicate_clean['is_case'] = 1
supervised_pd_fixed = supervised_pd_fixed.merge(SB_duplicate_clean[['ACCOUNT_ID','alert_month_sk','question_sbcase','is_SB','is_case']],\
                                  left_on = ['account_key','alert_month_sk','is_SB','is_case'],\
                                  right_on = ['ACCOUNT_ID','alert_month_sk','is_SB','is_case'], how = "left")
supervised_pd_fixed.loc[supervised_pd_fixed.question_sbcase.isnull(),'question_sbcase'] = 0
supervised_pd_fixed['is_case_clean'] = supervised_pd_fixed.is_case * (1 - supervised_pd_fixed.question_sbcase)

with open("updated_data_name.txt","w") as file:
    file.write("ATC_mar_fixed"+now.strftime("%Y%m%d")+".csv")
supervised_pd_fixed.to_csv("ATC_mar_fixed"+now.strftime("%Y%m%d")+".csv",index = False)

#build the data used for unsupervised evaluation
sup_pd_alert = supervised_pd_fixed
sup_pd_alert.sort_values(['account_key','alert_month_sk','alert_rank','is_case_clean','is_case','is_case_raw','is_SB','is_sar']\
                        ,ascending=[True,True,False,False,False,False,False,False],inplace = True)
sup_eval_pd = sup_pd_alert.groupby(['account_sk','account_key','alert_month_sk']).first().reset_index()\
             .sort_values(['account_sk','account_key','alert_month_sk'])[['account_sk','account_key','alert_month_sk','alert_rank','is_case_clean','is_case','is_case_raw','is_SB','is_sar']]
sup_eval_pd.to_csv("alert_evaluate"+now.strftime("%Y%m%d")+".csv",index = False)

