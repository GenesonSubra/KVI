# Databricks notebook source
# MAGIC %md
# MAGIC ### THIS WORKBOOK CALCULATES METRICS FOR KVI SCORING

# COMMAND ----------

# Tracking run time
# only for tracking elapsed time - put this in first cell by itself
import time
start_all = time.time()

# COMMAND ----------

# set Banner_ID using widget
dbutils.widgets.text("Banner_ID","")
Banner_ID = dbutils.widgets.get("Banner_ID")

# set RunDate variable using a widget
dbutils.widgets.text("RunDate","","")
RunDate = dbutils.widgets.get("RunDate")

# set MountPath using widget
dbutils.widgets.text("MountPath","")
MountPath = dbutils.widgets.get("MountPath")

# COMMAND ----------

# DBTITLE 1,Set widgets for RunDate to look back a year
# all required imports
from datetime import timedelta, datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# this function will calculate the a full fiscal year back from the RunDate
def Look_Back_A_Year(RunDate):
  # set date parameters for sales pull
  weeks_back = 52
  
  # define the windows for how many weeks to look  back
  start_window = Window.partitionBy().orderBy('fisc_wk_end_dt').rowsBetween(weeks_back*-1, Window.currentRow)
  
  # calculate start_date
  sales_start_date = fisc_calendar.select('fisc_wk_end_dt').distinct() \
    .withColumn('look_back', F.min('fisc_wk_end_dt').over(start_window)) \
    .filter(F.col('fisc_wk_end_dt').isin(RunDate)) \
    .alias('fisc_cal')\
    .collect()[0][1] \
    - timedelta(days=7)
  
  # convert sales_start_date from date.time to str
  sales_start_date = sales_start_date.strftime("%Y-%m-%d")
  
  return sales_start_date

# COMMAND ----------

# DBTITLE 1,Load required tables and files
spark.read.parquet(MountPath + 'Product/Product/100[0-4]*/').createOrReplaceTempView("temp_product")
spark.read.parquet(MountPath + 'Customer/lylty_cust/lylty_cust/100[0-4]*/').createOrReplaceTempView("temp_custdata")
spark.read.parquet(MountPath + 'Customer/lylty_card/lylty_card/100[0-4]*/').createOrReplaceTempView("temp_carddata")
spark.read.parquet(MountPath + 'CALENDAR/Fiscal_Week/').createOrReplaceTempView("temp_caldata")
spark.read.parquet(MountPath + 'Location/Location_CDM/').createOrReplaceTempView("temp_location")
spark.read.parquet(MountPath + 'POS/BatchReportingHive/item').createOrReplaceTempView("temp_itemdata")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/History_Modal/').createOrReplaceTempView("price_sensitivity_modal")

# CLOVER
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/MonFreqSegmentation/MonFreq_history/').createOrReplaceTempView("clover_raw")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/MonFreqSegmentation/Segment_mapping/').createOrReplaceTempView("clover_mapping")
spark.read.parquet(MountPath + 'Product/AdvocatedCategories/Product_parquet/Ahold/').createOrReplaceTempView("regroup_clover_ah")
spark.read.parquet(MountPath + 'Product/AdvocatedCategories/Product_parquet/Delhaize/').createOrReplaceTempView("regroup_clover_da")

# Price Sensititivy
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/History_52weeks/').createOrReplaceTempView("price_sensitivity_raw")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/Centroids/').createOrReplaceTempView("price_sensitivity_centroids")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/CustomerDeciles/').createOrReplaceTempView("price_sensitivity_deciles")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/History_Modal/').createOrReplaceTempView("price_sensitivity_modal")
spark.read.parquet(MountPath + 'Dipla/Segmentation/Data/PricingSegmentation/ProductMapping/').createOrReplaceTempView("price_sensitivity_regroups")

# Clover Segments
spark.sql("""select cust_level_value as cust_ident
             ,h.*
             ,loyalty_segment3 as clover
             ,loyalty_segment4 as clover4
             ,case when loyalty_segment3 = 'P' then '1 Primary'
             when loyalty_segment3 = 'S' then '2 Secondary'
             when loyalty_segment3 = 'T' then '3 Tertiary'
             end as clover_desc
             ,case when loyalty_segment4 = 'P' then '1 Primary'
             when loyalty_segment4 = 'SB' then '2 Secondary Low Breadth'
             when loyalty_segment4 = 'SF' then '3 Secondary Low Frequency'
             when loyalty_segment3 = 'T' then '4 Tertiary'       
             end as clover4_desc

             from   clover_raw h
             inner join clover_mapping m
             on  h.banner_id = m.banner_id 
             and h.cust_seg_value1_mon = m.cust_seg_value1_mon
             and h.cust_seg_value1_freq = m.cust_seg_value1_freq
             and h.cust_seg_value1_ac  = m.cust_seg_value1_ac
             """).createOrReplaceTempView('clover')  

# Price Sensitivity Segments
spark.sql("""select *
             ,case 
             when cpa_segment=1 then '1. Very Price Driven'
             when cpa_segment=2 then '2. Price Focused'
             when cpa_segment=3 then '3. Balanced'
             when cpa_segment=4 then '4. Quality Focused'
             else '9. Unknown' 
             end as CPA_SEGMENT_DSC
             from price_sensitivity_raw
             """)

# Load fiscal week calendar for aggregating data
fisc_calendar = spark.read.parquet(MountPath + 'CALENDAR/Fiscal_Week/', sep='|', header=True) \
    .select(F.col('day_dt'), F.col('wk_end_dt').alias('fisc_wk_end_dt')) \
    .alias('fisc_cal')


# COMMAND ----------

sales_start_date = Look_Back_A_Year(RunDate)
print(sales_start_date)

# COMMAND ----------

# DBTITLE 1,pulling Price Master from temp_product to review the content
temp_prod_df = spark.sql('SELECT * FROM temp_product')

# COMMAND ----------

# DBTITLE 1,SET VARIABLES
# NEED TO PICK ONE BRAND AT A TIME
# NOTE THIS VERSION OF THE CODE IS SET UP FOR AHOLD BRANDS ONLY AND WILL NEED TO BE ADAPTED FOR DELHAIZE BRANDS

# FOR RUN PERIOD, ONE COMPLETE YEAR SHOULD BE SELECTED BY DEFAULT AND SHOULD RUN ACROSS COMPLETE FISCAL QUARTERS - ADAPT CELL BELOW TO CHECK THESE

#brandnum = Banner_ID
#enddate=f"'{RunDate}'"
#startdate=f"'{sales_start_date}'"
runname="SS_Q3_2021"

# COMMAND ----------

# DBTITLE 1,Combine new LIR lookup with historic lookup
spark.sql("""select a.upc,first(a.lir_description) as lir_description
             from otsa_line_groups_ahold a left 
             join otsa_kvi_lir_upc_ss_20211108a b
             on a.upc=b.upc_cd
             where b.upc_cd is null and a.upc is not null
             group by 1
             """).createOrReplaceTempView('upcs_in_old_not_in_new')

spark.sql("""select a.upc,a.lir_description
             from upcs_in_old_not_in_new a inner 
             join (select distinct lir_description as lir_description 
             from otsa_kvi_lir_upc_ss_20211108a) b
             on a.lir_description=b.lir_description
             """).createOrReplaceTempView('ups_not_in_current_lir')

#verify the below code! - ok
spark.sql("""select * from otsa_kvi_lir_upc_ss_20211108a 
             where upc_cd is not null union all
             select * from ups_not_in_current_lir where upc is not null
             """).createOrReplaceTempView('otsa_kvi_lir_upc')


# COMMAND ----------

# DBTITLE 1,Create transaction data view by filtering item table
# 4 min to run
spark.sql(f"""select cal.WK_END_DT
          , cal.FISC_QTR_OF_YR_NUM
          , item.trans_id
          , item.banner_id
          , item.store_nbr
          , item.cust_crd_nbr
          , item.lgcy_sub_dept
          , item.item_id
          , item.qty_sld
          , item.wgt_sld
          , item.unit_of_meas
          , item.unit_net_prc
          , item.ext_net_prc
          , item.unit_rtl_prc
          , item.ext_rtl_prc
          , item.businessdate
          , case when pp.store_nbr is not null then 1 else 0 end as PEAPOD_FLAG
          from TEMP_CALDATA cal
          inner join temp_itemdata item
          on cal.day_dt=item.businessdate
          and void_cd IN (0)
          and (banner_id=1001 or store_nbr in (select loc_cd as store_nbr 
          from temp_location where banner_id=1001 and loc_hier_lvl3_desc like '%PEAPOD%'))
          and (lgcy_sub_dept <=21 or lgcy_sub_dept between 24 and 29)        
          and businessdate between "{sales_start_date}" and "{RunDate}"
          left join (select loc_cd as store_nbr from temp_location where banner_id=1001 
          and loc_hier_lvl3_desc like '%PEAPOD%') pp
          on item.store_nbr=pp.store_nbr
          where day_dt between "{sales_start_date}" and "{RunDate}"
          """).createOrReplaceTempView('txns')

# COMMAND ----------

# DBTITLE 1,Build out UPC to LIR Lookup with Sales and Additional Variables
# takes 10 mins to run
# Build out UPC to LIR Lookup with Sales and Additional Variables

spark.sql("""select UPC_CD as UPC_NBR
             ,LIR_Description
             from otsa_kvi_lir_upc
             """).createOrReplaceTempView('lir_lu')

spark.sql(f"""select a.item_id as upc_nbr
             ,b.upc_dsc
             ,replace(b.PRDT_LVL_2_DSC,'POS DEPT ','') as DEPARTMENT_DSC
             ,b.PRDT_LVL_5_DSC as PORTFOLIO_DSC
             ,b.PRDT_LVL_6_DSC as CATEGORY_DSC
             ,B.BRND_DSC AS BRAND_DSC
             , CASE WHEN  PVT_LBL_DSC <> 'NULL' AND PVT_LBL_DSC <> 'UNDEFINED' THEN 1 ELSE 0 END AS PL_IND
             ,case when c.LIR_Description is null then 'UPC '||UPC_DSC else LIR_Description end as LIR_Description
             ,sum(ext_rtl_prc) as sales 
             ,sum(qty_sld) as item_52wk_units
                  
          from txns a
              inner join temp_product b
                  on a.item_id=b.upc_nbr
                  and b.banner_id={Banner_ID}
              left join lir_lu c
                on a.item_id=c.upc_nbr
               
                  group by 1,2,3,4,5,6,7,8
""").createOrReplaceTempView('otsa_kvi_lir_upc_processed')

# COMMAND ----------

# DBTITLE 1,Create LIR Master Lookup 
# runs for 32 min
# Create LIR Master Lookup 

# Get dominant department  for each LIR
spark.sql("""select DEPARTMENT_DSC, LIR_Description 
             from (select *, RANK() over (partition by LIR_Description order by units DESC, RAND()) as UNITS_RANK
             from (select DEPARTMENT_DSC
             ,LIR_Description
             ,SUM(item_52wk_units) as units 
             from otsa_kvi_lir_upc_processed
             group by 1,2))
             where UNITS_RANK=1
             """).createOrReplaceTempView('otsa_kvi_DEPARTMENT')

# Get dominant portfolio  for each LIR
spark.sql("""select PORTFOLIO_DSC, LIR_Description  
             from (select *, RANK() over (partition by LIR_Description order by units DESC, RAND()) as UNITS_RANK
             from (select PORTFOLIO_DSC
             ,LIR_Description
             ,SUM(item_52wk_units) as units 
             from otsa_kvi_lir_upc_processed
             group by 1,2))
             where UNITS_RANK=1
             """).createOrReplaceTempView('otsa_kvi_PORTFOLIO')

# Get dominant category  for each LIR
spark.sql("""select CATEGORY_DSC, LIR_Description  
             from (select *, RANK() over (partition by LIR_Description order by units DESC, RAND()) as UNITS_RANK
             from (select CATEGORY_DSC
             ,LIR_Description
             ,SUM(item_52wk_units) as units 
             from otsa_kvi_lir_upc_processed
             group by 1,2))
             where UNITS_RANK=1
             """).createOrReplaceTempView('otsa_kvi_CATEGORY')

# Get dominant category  for each BRAND
spark.sql("""select BRAND_DSC, LIR_Description  
             from (select *, RANK() over (partition by LIR_Description order by units DESC, RAND()) as UNITS_RANK
             from (select BRAND_DSC
             ,LIR_Description
             ,SUM(item_52wk_units) as units 
             from otsa_kvi_lir_upc_processed
             group by 1,2))
             where UNITS_RANK=1
             """).createOrReplaceTempView('otsa_kvi_BRAND')

# Flag if LIR includes Private label
spark.sql("""select  LIR_Description
             ,MAX(PL_IND) as PL_IND 
             from otsa_kvi_lir_upc_processed
             group by 1
             """).createOrReplaceTempView('otsa_kvi_PL_IND')

# Create LIR master with brand, product hierarchy and private label flags
spark.sql("""select * from (select a.LIR_Description    
             ,c.DEPARTMENT_DSC
             ,d.CATEGORY_DSC
             ,e.PORTFOLIO_DSC
             ,f.BRAND_DSC
             ,g.PL_IND
             from (select distinct LIR_Description from otsa_kvi_lir_upc_processed) a  
             left join otsa_kvi_DEPARTMENT c    ON a.LIR_Description=c.LIR_Description
             left join otsa_kvi_CATEGORY   d    ON a.LIR_Description=d.LIR_Description
             left join otsa_kvi_PORTFOLIO  e    ON a.LIR_Description=e.LIR_Description
             left join otsa_kvi_BRAND      f    ON a.LIR_Description=f.LIR_Description                        
             left join otsa_kvi_PL_IND     g    ON a.LIR_Description=g.LIR_Description) 
             where PORTFOLIO_DSC not in ('Tobacco')
             and LIR_Description not like '%REDUCED%'
             and LIR_Description not like '%DEPOSIT%'
             and LIR_Description not like '%DISC_AT_TOTAL%'
             and LIR_Description not like '%BAG_FEE%'
             and LIR_Description not like '%CHECKOUT_BAG_CHARGE%'
             and LIR_Description not like 'No LIR'
             """).createOrReplaceTempView('otsa_kvi_lir_master')

# COMMAND ----------

# DBTITLE 1,Create customer tables 
# runs for 6.7 min
# Create customer tables
# Customer Identifier

spark.sql(f"""select distinct cust.hh_id as cust_ident
             ,crd.lylty_crd_nbr
             ,cust_bnnr_id as banner_id
             from temp_carddata as crd
             inner join temp_custdata as cust
             on crd.lylty_crd_bnnr_id= cust.cust_bnnr_id 
             and cust.cust_bnnr_id = {Banner_ID}
             and crd.lylty_cust_id = cust.cust_id
       
             where crd.lylty_crd_bnnr_id = {Banner_ID}
             and crd.lylty_crd_typ_id IN (1,2)             
""").createOrReplaceTempView('tmp_custs_ss')     

# Select CLOVER segments for baner and time period

spark.sql(f""" select cust_ident
         ,businessdate
         ,banner_id
         ,CLOVER
         from  clover
         where banner_id={Banner_ID} and businessdate between "{sales_start_date}" and "{RunDate}"
""").createOrReplaceTempView('tmp_clover_ss')  

# COMMAND ----------

# DBTITLE 1,Add on linegroup, household information and product information to txns data
# Add on linegroup, household information and product information to txns data
# Add Item ID

spark.sql(f"""select a.*
         ,LIR_Description
         from txns a
         left join otsa_kvi_lir_upc_processed b on a.item_id=b.UPC_NBR
  """).createOrReplaceTempView('txns0') 

# Add household ID
spark.sql("""select txns.*
         ,custs.cust_ident
         from txns0 as txns
         left outer join tmp_custs_ss as custs
         on txns.cust_crd_nbr = custs.lylty_crd_nbr
  """).createOrReplaceTempView('txns1') 

# Join on Subcategory from product lookup
spark.sql(f"""select a.*, prdt_lvl_7_id
         from txns1 a
         inner join temp_product b 
         on a.item_id=b.upc_nbr 
         where b.banner_id={Banner_ID}
         and cust_ident is not null and prdt_lvl_7_id is not null
   """).createOrReplaceTempView('txns2') 

# Add CLOVER Segment
spark.sql("""select txns.*
         ,clover.clover
         from txns2 as txns
         left outer join tmp_clover_ss as clover
         on clover.cust_ident=txns.cust_ident
         and clover.businessdate = txns.wk_end_dt
  """).createOrReplaceTempView('txns3') 

# Add Price Sensitivity Segment
spark.sql(f"""select txns.*
         ,cpa.cpa_segment_modal
         from txns3 as txns
         left outer join (select * from price_sensitivity_modal where fisc_week_ending="{RunDate}") as cpa
         on cpa.cust_ident=txns.cust_ident
         and cpa.banner_id={Banner_ID}
  """).createOrReplaceTempView('txns4')


# COMMAND ----------

# DBTITLE 1,Run Line Group Level Metrics
# Run Line Group Level Metrics
# Create Metrics at Line Group level
# Total year

spark.sql("""select LIR_Description
         ,sum(ext_rtl_prc) as TOTAL_SALES_GROSS
         ,sum(ext_net_prc) as TOTAL_SALES_NET  
         ,sum(qty_sld) as TOTAL_QTY    
         ,sum(wgt_sld) as TOTAL_WEIGHT        
         ,sum(case when unit_of_meas ='EA' then qty_sld else 0 end) as TOTAL_UNIT_QTY  
         ,count(distinct store_nbr) as TOTAL_STORES
         ,count(distinct ITEM_ID) as UPC_COUNT
         ,sum(case when PEAPOD_FLAG=1 then qty_sld else 0 end) as PEAPOD_QTY
         ,sum(case when CUST_IDENT is not null then ext_rtl_prc else 0 end) as CARDED_SALES_GROSS
         ,sum(case when CUST_IDENT is not null then ext_net_prc else 0 end) as CARDED_SALES_NET  
         ,sum(case when CUST_IDENT is not null then qty_sld else 0 end) as CARDED_QTY    
         ,sum(case when CUST_IDENT is not null then wgt_sld else 0 end) as CARDED_WEIGHT        
         ,sum(case when CUST_IDENT is not null and unit_of_meas ='EA' then qty_sld else 0 end) as CARDED_UNIT_QTY 
         ,count(distinct CUST_IDENT) as HOUSEHOLDS
         ,count(distinct TRANS_ID) as TRANSACTIONS  
         ,max(businessdate) as LAST_SOLD_DATE
         ,sum(case when ext_net_prc<ext_rtl_prc then qty_sld else 0 end) as PROMOTED_UNITS
         ,sum(case when CLOVER='P' then qty_sld else 0 end) as CLOVER_QTY_PRIMARY
         ,sum(case when CLOVER='P' then ext_net_prc else 0 end) as CLOVER_SALES_PRIMARY
         ,sum(case when CLOVER='S' then qty_sld else 0 end) as CLOVER_QTY_SECONDARY
         ,sum(case when CLOVER='T' then qty_sld else 0 end) as CLOVER_QTY_TERTIARY 
         ,sum(case when cpa_segment_modal=1 then qty_sld else 0 end) as PS_QTY_VERYPRICEDRIVEN
         ,sum(case when cpa_segment_modal=2 then qty_sld else 0 end) as PS_QTY_PRICEFOCUSED
         ,sum(case when cpa_segment_modal=3 then qty_sld else 0 end) as PS_QTY_BALANCED 
         ,sum(case when cpa_segment_modal=4 then qty_sld else 0 end) as PS_QTY_QUALITYFOCUSED
         ,sum(case when cpa_segment_modal in (1,2) then ext_net_prc else 0 end) as PS_SALES_VERYPRICEDRIVEN_PRICEFOCUSED
         from txns4 a
         group by 1 """).createOrReplaceTempView('line_groups_metrics_in1')


# CALCULATE QUARTERLY PENETRATION METRICS
# Q1
spark.sql("""select  LIR_Description
         ,count(distinct TRANS_ID) as TRANSACTIONS_q1
         from txns4 a
         where FISC_QTR_OF_YR_NUM=1
         group by 1
""").createOrReplaceTempView('line_groups_metrics_in1_q1')

# Q2
spark.sql("""select  LIR_Description
         ,count(distinct TRANS_ID) as TRANSACTIONS_q2  
         from txns4 a
         where FISC_QTR_OF_YR_NUM=2
         group by 1
""").createOrReplaceTempView('line_groups_metrics_in1_q2')

# Q3
spark.sql("""select  LIR_Description
         ,count(distinct TRANS_ID) as TRANSACTIONS_q3  
         from txns4 a
         where FISC_QTR_OF_YR_NUM=3
         group by 1
""").createOrReplaceTempView('line_groups_metrics_in1_q3')

# Q4
spark.sql("""select  LIR_Description
        ,count(distinct TRANS_ID) as TRANSACTIONS_q4
        from txns4 a
        where FISC_QTR_OF_YR_NUM=4
        group by 1
""").createOrReplaceTempView('line_groups_metrics_in1_q4')


# COMMAND ----------

# DBTITLE 1,Output Metrics
#Create output metrics tables (total and quarterly)

spark.sql("""select met.*
         ,CLOVER_QTY_PRIMARY/(CLOVER_QTY_PRIMARY+CLOVER_QTY_SECONDARY+CLOVER_QTY_TERTIARY) AS CLOVER_QTY_PRIMARY_PC
         ,PS_QTY_VERYPRICEDRIVEN/(PS_QTY_VERYPRICEDRIVEN+PS_QTY_PRICEFOCUSED+PS_QTY_BALANCED+PS_QTY_QUALITYFOCUSED) AS PS_QTY_VERYPRICEDRIVEN_PC      
         ,PS_QTY_PRICEFOCUSED/(PS_QTY_VERYPRICEDRIVEN+PS_QTY_PRICEFOCUSED+PS_QTY_BALANCED+PS_QTY_QUALITYFOCUSED) AS PS_QTY_PRICEFOCUSED_PC    
         ,((PS_QTY_VERYPRICEDRIVEN*3)+(PS_QTY_PRICEFOCUSED*2)+(PS_QTY_BALANCED))/(PS_QTY_VERYPRICEDRIVEN+PS_QTY_PRICEFOCUSED+PS_QTY_BALANCED+PS_QTY_QUALITYFOCUSED) as PRICE_SENSITIVE_SKEW
         ,PEAPOD_QTY/TOTAL_QTY as PEAPOD_PC 
         ,(TOTAL_SALES_GROSS-TOTAL_SALES_NET)/TOTAL_SALES_GROSS as MARKDOWN_PC
         ,PROMOTED_UNITS/TOTAL_QTY as PROMOTED_PC
         ,coalesce(TRANSACTIONS_q1,0) as TRANSACTIONS_q1
         ,coalesce(TRANSACTIONS_q2,0) as TRANSACTIONS_q2
         ,coalesce(TRANSACTIONS_q3,0) as TRANSACTIONS_q3
         ,coalesce(TRANSACTIONS_q4,0) as TRANSACTIONS_q4
         from  line_groups_metrics_in1 as met
         left join line_groups_metrics_in1_q1 q1 on met.LIR_Description=q1.LIR_Description
         left join line_groups_metrics_in1_q2 q2 on met.LIR_Description=q2.LIR_Description
         left join line_groups_metrics_in1_q3 q3 on met.LIR_Description=q3.LIR_Description
         left join line_groups_metrics_in1_q4 q4 on met.LIR_Description=q4.LIR_Description
""").createOrReplaceTempView('otsa_kvi_output1_')


# COMMAND ----------

# DBTITLE 1,Calculate Product Loyalty 
# Get total units purchased in each subcategory for each LIR
spark.sql("""select cust_ident
         ,prdt_lvl_7_id
         ,LIR_Description
         ,sum(qty_sld) as LIR_SUBCAT_QTY    
         ,count(distinct trans_id) as LIR_SUBCAT_PURCH  
         from txns2
         where cust_ident is not null and prdt_lvl_7_id is not null
         group by 1,2,3
  """).createOrReplaceTempView('pl1a') 

# Get total units purchased in each subcategory 
spark.sql("""select cust_ident
         ,prdt_lvl_7_id
         ,sum(qty_sld) as SUBCAT_QTY    
         ,count(distinct trans_id) as SUBCAT_PURCH 
         from txns2
         where cust_ident is not null and prdt_lvl_7_id is not null
         group by 1,2
  """).createOrReplaceTempView('pl1b')  

# Join subcategory and LIR by subcategory views together
spark.sql("""select a.cust_ident
         ,a.LIR_Description
         ,a.prdt_lvl_7_id
         ,a.LIR_SUBCAT_QTY
         ,b.SUBCAT_QTY
         ,a.LIR_SUBCAT_PURCH
         ,b.SUBCAT_PURCH 
         from pl1a as a
         inner join pl1b as b
         on a.cust_ident=b.cust_ident and a.prdt_lvl_7_id=b.prdt_lvl_7_id
  """).createOrReplaceTempView('pl1') 

# Sum up to each LIR for each customer.  Note that the subcategory sales base is the total sales of all the subcategories that the LIR covers
spark.sql("""select  cust_ident
         ,LIR_Description
         ,sum(LIR_SUBCAT_QTY) as LIR_SUBCAT_QTY
         ,sum(SUBCAT_QTY) as SUBCAT_QTY
         ,sum(LIR_SUBCAT_PURCH) as LIR_SUBCAT_PURCH
         ,sum(SUBCAT_PURCH) as SUBCAT_PURCH       
         from pl1
         group by 1,2
  """).createOrReplaceTempView('pl2') 

# Calculate the loyalty score for each customer
spark.sql("""select cust_ident
         ,LIR_Description
         ,LIR_SUBCAT_QTY    
         ,SUBCAT_QTY
         ,LIR_SUBCAT_PURCH  
         ,SUBCAT_PURCH       
         ,LIR_SUBCAT_QTY/SUBCAT_QTY as SUBCAT_SHARE
         ,LIR_SUBCAT_QTY*(LIR_SUBCAT_QTY/SUBCAT_QTY) as SHARE_SCORE
         ,LIR_SUBCAT_PURCH/SUBCAT_PURCH as SUBCAT_SHARE_PURCH
         ,LIR_SUBCAT_PURCH*(LIR_SUBCAT_PURCH/SUBCAT_PURCH) as SHARE_SCORE_PURCH       
         from pl2
  """).createOrReplaceTempView('pl2a')  

# Calculate the average loyalty score for each LIR
spark.sql("""select LIR_Description
         ,sum(LIR_SUBCAT_QTY) as LIR_SUBCAT_QTY
         ,sum(subcat_qty) as SUBCAT_QTY
         ,AVG(SHARE_SCORE) as LOYALTY_SCORE
         ,sum(LIR_SUBCAT_PURCH) as LIR_SUBCAT_PURCH
         ,sum(subcat_PURCH) as SUBCAT_PURCH
         ,AVG(SHARE_SCORE_PURCH) as LOYALTY_SCORE_PURCH       
         from pl2a
         group by 1
  """).createOrReplaceTempView('pl3') 

# Create output product loyalty table
spark.sql("""select * from pl3
 """).createOrReplaceTempView('otsa_kvi_loyalty_')  


# COMMAND ----------

# DBTITLE 1,Calculate total transactions and households that are used in the Final Excel Output creation below
# Calculate total transactions and households that are used in the Final Excel Output creation below

spark.sql(f"""
      select   count(distinct trans_id) as total_transactions
              ,count(distinct cust_crd_nbr) as total_customers
              ,count(distinct case when FISC_QTR_OF_YR_NUM=1 then trans_id else null end) as q1_transactions
              ,count(distinct case when FISC_QTR_OF_YR_NUM=2 then trans_id else null end) as q2_transactions
              ,count(distinct case when FISC_QTR_OF_YR_NUM=3 then trans_id else null end) as q3_transactions
              ,count(distinct case when FISC_QTR_OF_YR_NUM=4 then trans_id else null end) as q4_transactions
        from txns 
        
""").createOrReplaceTempView('otsa_kvi_totals_')  

# COMMAND ----------

# DBTITLE 1,Converting the variables
# part 1/3 - converting the variables

stat_df = spark.sql(f"""select percentile(PRICE_SENSITIVE_SKEW,0.5) as PRICE_SENSITIVE_SKEW_MEDIAN 
          ,percentile(CLOVER_QTY_PRIMARY_PC,0.5) as CLOVER_QTY_PRIMARY_PC_MEDIAN 
          ,(select percentile(LOYALTY_SCORE_PURCH,0.5)   from  otsa_kvi_loyalty_ where LIR_SUBCAT_QTY>1000) as LOYALTY_SCORE_PURCH_MEDIAN
    from  otsa_kvi_output1_
""")

median_ps = stat_df.collect()[0][0]
median_ppc = stat_df.collect()[0][1]
median_ls = stat_df.collect()[0][2]

# COMMAND ----------

# part 2/3 - using the variables created from cell above

spark.sql(f""" select a.LIR_Description
         ,b.DEPARTMENT_DSC as DEPARTMENT
         ,b.CATEGORY_DSC as CATEGORY
         ,b.PORTFOLIO_DSC as PORTFOLIO
         ,b.BRAND_DSC as BRAND
         ,b.PL_IND as PRIVATE_LABEL
         ,case when a.PROMOTED_PC>0.5 then 1 else 0 end as PROMOTED_FLAG
         ,a.TOTAL_STORES
         ,a.PEAPOD_PC
         ,a.UPC_COUNT as NUMBER_OF_UPCS
         ,(a.TRANSACTIONS/(select max(total_transactions) 
         from otsa_kvi_totals_))*pow((a.PRICE_SENSITIVE_SKEW/{median_ps}),2)*(a.CLOVER_QTY_PRIMARY_PC/{median_ppc})*( pow(t.LOYALTY_SCORE_PURCH/ {median_ls},1/4))          
         as KVI_SCORE
         ,(a.TRANSACTIONS_Q1/(select max(q1_transactions) 
         from otsa_kvi_totals_))*pow((a.PRICE_SENSITIVE_SKEW/{median_ps}),2)*(a.CLOVER_QTY_PRIMARY_PC/{median_ppc})*( pow(t.LOYALTY_SCORE_PURCH/ {median_ls},1/4))          
         as Q1_KVI_SCORE
         ,(a.TRANSACTIONS_Q2/(select max(q2_transactions) 
         from otsa_kvi_totals_))*pow((a.PRICE_SENSITIVE_SKEW/{median_ps}),2)*(a.CLOVER_QTY_PRIMARY_PC/{median_ppc})*( pow(t.LOYALTY_SCORE_PURCH/ {median_ls},1/4)) 
         as Q2_KVI_SCORE
         ,(a.TRANSACTIONS_Q3/(select max(q3_transactions) 
         from otsa_kvi_totals_))*pow((a.PRICE_SENSITIVE_SKEW/{median_ps}),2)*(a.CLOVER_QTY_PRIMARY_PC/{median_ppc})*( pow(t.LOYALTY_SCORE_PURCH/ {median_ls},1/4))          
         as Q3_KVI_SCORE
         ,(a.TRANSACTIONS_Q4/(select max(q4_transactions) 
         from otsa_kvi_totals_))*pow((a.PRICE_SENSITIVE_SKEW/{median_ps}),2)*(a.CLOVER_QTY_PRIMARY_PC/{median_ppc})*( pow(t.LOYALTY_SCORE_PURCH/ {median_ls},1/4)) 
         as Q4_KVI_SCORE

        ,a.HOUSEHOLDS/(select max(total_customers) from otsa_kvi_totals_) AS HOUSEHOLD_PENETRATION
        ,a.TOTAL_SALES_GROSS 
        ,a.TOTAL_SALES_NET 
        ,a.TOTAL_SALES_NET/(CASE WHEN a.TOTAL_UNIT_QTY > 0.5*a.TOTAL_QTY THEN a.TOTAL_QTY ELSE a.TOTAL_WEIGHT END) as AVERAGE_PRICE
        ,a.TOTAL_QTY 
        ,a.TOTAL_WEIGHT
        ,a.TOTAL_UNIT_QTY

        ,a.TRANSACTIONS/(select max(total_transactions) from otsa_kvi_totals_) as TRANSACTION_PENETRATION
        ,a.TRANSACTIONS_Q1/(select max(q1_transactions) from otsa_kvi_totals_) as TRANSACTION_PENETRATION_Q1
        ,a.TRANSACTIONS_Q2/(select max(q2_transactions) from otsa_kvi_totals_) as TRANSACTION_PENETRATION_Q2
        ,a.TRANSACTIONS_Q3/(select max(q3_transactions) from otsa_kvi_totals_) as TRANSACTION_PENETRATION_Q3
        ,a.TRANSACTIONS_Q4/(select max(q4_transactions) from otsa_kvi_totals_) as TRANSACTION_PENETRATION_Q4
  
-- Note, these wieghts can be adjusted to change the influence of the metrics on the overall KVI score.  They should also be updated to match in the formulas above.
        ,pow(a.PRICE_SENSITIVE_SKEW/{median_ps},2) as PRICE_SENSITIVE_WEIGHT
        ,a.CLOVER_QTY_PRIMARY_PC/{median_ppc} as PRIMARY_CLOVER_WEIGHT
        ,pow(t.LOYALTY_SCORE_PURCH / {median_ls},1/4) as PRODUCT_LOYALTY_WEIGHT


        ,a.PRICE_SENSITIVE_SKEW 
        ,a.CLOVER_QTY_PRIMARY_PC
        ,t.LOYALTY_SCORE_PURCH as PRODUCT_LOYALTY

        ,a.CLOVER_QTY_PRIMARY
        ,a.CLOVER_SALES_PRIMARY
        ,a.CLOVER_QTY_SECONDARY
        ,a.CLOVER_QTY_TERTIARY
        ,a.PS_QTY_VERYPRICEDRIVEN
        ,a.PS_QTY_PRICEFOCUSED
        ,a.PS_QTY_BALANCED
        ,a.PS_QTY_QUALITYFOCUSED
        ,a.PS_QTY_VERYPRICEDRIVEN_PC
        ,a.PS_QTY_PRICEFOCUSED_PC
        ,a.PS_SALES_VERYPRICEDRIVEN_PRICEFOCUSED
        ,a.MARKDOWN_PC
        ,a.PROMOTED_PC


        from otsa_kvi_output1_ as a

        left outer join otsa_kvi_loyalty_ t 
        on a.LIR_Description=t.LIR_Description
      
        inner join otsa_kvi_lir_master as b 
        on a.LIR_Description=b.LIR_Description 

        where (a.TRANSACTIONS>0 and a.TOTAL_QTY>0 and PRICE_SENSITIVE_SKEW>0 and CLOVER_QTY_PRIMARY_PC>0 and LOYALTY_SCORE_PURCH>0)
        order BY TOTAL_QTY desc 
        """).createOrReplaceTempView('out1')

# COMMAND ----------

# DBTITLE 1,Output Table
# Output Table

# Add KVI Ranking and create output file

spark.sql(f"""select  rank() over(order by KVI_SCORE desc) as KVI_RANK
         ,LIR_Description
         ,DEPARTMENT
         ,PORTFOLIO
         ,CATEGORY
         ,BRAND
         ,KVI_SCORE
         ,TRANSACTION_PENETRATION
         ,PRICE_SENSITIVE_WEIGHT
         ,PRIMARY_CLOVER_WEIGHT
         ,PRODUCT_LOYALTY_WEIGHT
         ,Q1_KVI_SCORE
         ,Q2_KVI_SCORE
         ,Q3_KVI_SCORE
         ,Q4_KVI_SCORE
         ,TRANSACTION_PENETRATION_Q1
         ,TRANSACTION_PENETRATION_Q2
         ,TRANSACTION_PENETRATION_Q3
         ,TRANSACTION_PENETRATION_Q4
         ,PRIVATE_LABEL
         ,TOTAL_STORES
         ,NUMBER_OF_UPCS
         ,PROMOTED_FLAG
         ,MARKDOWN_PC
         ,PROMOTED_PC
         ,AVERAGE_PRICE
         ,PRICE_SENSITIVE_SKEW
         ,PS_QTY_VERYPRICEDRIVEN
         ,PS_QTY_PRICEFOCUSED
         ,PS_QTY_BALANCED
         ,PS_QTY_QUALITYFOCUSED
         ,PS_QTY_VERYPRICEDRIVEN_PC
         ,PS_QTY_PRICEFOCUSED_PC
         ,PS_SALES_VERYPRICEDRIVEN_PRICEFOCUSED
         ,CLOVER_QTY_PRIMARY_PC
         ,CLOVER_SALES_PRIMARY
         ,CLOVER_QTY_PRIMARY
         ,CLOVER_QTY_SECONDARY
         ,CLOVER_QTY_TERTIARY
         ,PRODUCT_LOYALTY
         ,HOUSEHOLD_PENETRATION
         ,rank() over(order by (TOTAL_SALES_NET) DESC) as SALES_RANK
         ,TOTAL_SALES_NET
         ,TOTAL_SALES_GROSS
         ,TOTAL_QTY
         ,TOTAL_WEIGHT
         ,TOTAL_UNIT_QTY
         ,PEAPOD_PC
         ,'{sales_start_date}' as START_DATE
         ,'{RunDate}' as END_DATE
         ,from_unixtime(unix_timestamp()) as timestamp

         from out1 where 
         TOTAL_QTY>0 and product_loyalty>0 
""").createOrReplaceTempView('otsa_kvi_scores_output_')

# COMMAND ----------

# runs for 35 min
# writting the TempView to databricks table
# First, create a dataframe
df = spark.sql("select * from otsa_kvi_scores_output_")
# Second, save that dataframe as a dataricks table
df.write.mode('overwrite').saveAsTable('gs_test_kvi_output')

# COMMAND ----------

# only for tracking elapsed time - put this in last cell by itself
end_all = time.time()
elapsed_all = round((end_all - start_all)/60,2)
print(f'Process ended in {elapsed_all} minutes')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gs_test_kvi_output 

# COMMAND ----------



# COMMAND ----------

# DONE
