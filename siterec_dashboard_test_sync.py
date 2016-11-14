#!/usr/bin/env python
# -*- coding: utf-8 -*-


import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import datetime
import time

from rec_driver import *
from pymysql import PyMysql

time_now = int(time.time())
time_value = time.localtime(time_now)
time_year_str = time.strftime('%Y', time_value)
time_day_str = time.strftime('%Y-%m-%d', time_value)
time_day_str_all = time_day_str + " 00:00:00"
time_s = time.mktime(time.strptime(time_day_str_all, '%Y-%m-%d %H:%M:%S'))
time_day = int(time_s)

time_yesterday = time_day - 86400
time_value_yesterday = time.localtime(time_yesterday)
time_yesterday_str = time.strftime('%Y-%m-%d', time_value_yesterday)
time_yesterday_str_all = time_yesterday_str + " 00:00:00"

time_days_before = time_day - (15*86400)
time_value_days_before = time.localtime(time_days_before)
time_days_before_str = time.strftime('%Y-%m-%d', time_value_days_before)
time_days_before_str_all = time_days_before_str + " 00:00:00"

# print time_now
print 'start'
print time_day_str_all
# print time_day
# print time_yesterday
print time_yesterday_str_all
print time_days_before_str_all




# sql_str=''
# sql_str+="INSERT into siterec_dashboard_test.action_detail_static SELECT * from siterec_dashboard.action_detail_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.app_function_static SELECT * from siterec_dashboard.app_function_static where data_time = '"+time_day_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.audit_static SELECT * from siterec_dashboard.audit_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.cate_action_static SELECT * from siterec_dashboard.cate_action_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.cate_general_stat SELECT * from siterec_dashboard.cate_general_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.classify_static SELECT * from siterec_dashboard.classify_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.comment_static SELECT * from siterec_dashboard.comment_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.item_action_static SELECT * from siterec_dashboard.item_action_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.item_cate_static SELECT * from siterec_dashboard.item_cate_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.item_general_stat SELECT * from siterec_dashboard.item_general_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.item_tag_action_static SELECT * from siterec_dashboard.item_tag_action_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.item_tag_static SELECT * from siterec_dashboard.item_tag_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.page_dest_info SELECT * from siterec_dashboard.page_dest_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.page_hot_click_info SELECT * from siterec_dashboard.page_hot_click_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.page_source_field_info SELECT * from siterec_dashboard.page_source_field_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.page_source_info SELECT * from siterec_dashboard.page_source_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.page_static_info SELECT * from siterec_dashboard.page_static_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.platform_static_info SELECT * from siterec_dashboard.platform_static_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.potential_item_static SELECT * from siterec_dashboard.potential_item_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_ck_cate_stat SELECT * from siterec_dashboard.rec_ck_cate_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_ck_tag_stat SELECT * from siterec_dashboard.rec_ck_tag_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_effect_cate_stat SELECT * from siterec_dashboard.rec_effect_cate_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_effect_static SELECT * from siterec_dashboard.rec_effect_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_effect_tag_stat SELECT * from siterec_dashboard.rec_effect_tag_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_flow_stat SELECT * from siterec_dashboard.rec_flow_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_overall_static SELECT * from siterec_dashboard.rec_overall_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_source_info SELECT * from siterec_dashboard.rec_source_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.rec_total_static SELECT * from siterec_dashboard.rec_total_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.referer_action_static SELECT * from siterec_dashboard.referer_action_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.referer_daily_static SELECT * from siterec_dashboard.referer_daily_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_effect_static SELECT * from siterec_dashboard.search_effect_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_effect_static_platform SELECT * from siterec_dashboard.search_effect_static_platform where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_empty_query SELECT * from siterec_dashboard.search_empty_query where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_evaluate_static SELECT * from siterec_dashboard.search_evaluate_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_item_query_click SELECT * from siterec_dashboard.search_item_query_click where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_leastclick_query SELECT * from siterec_dashboard.search_leastclick_query where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_overall_static SELECT * from siterec_dashboard.search_overall_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_query_item_click SELECT * from siterec_dashboard.search_query_item_click where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_recent_cold SELECT * from siterec_dashboard.search_recent_cold where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_recent_hot SELECT * from siterec_dashboard.search_recent_hot where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_relate_overall_static SELECT * from siterec_dashboard.search_relate_overall_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_source_info SELECT * from siterec_dashboard.search_source_info where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_suggest_overall_static SELECT * from siterec_dashboard.search_suggest_overall_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_filter SELECT * from siterec_dashboard.search_top_filter where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_flip SELECT * from siterec_dashboard.search_top_flip where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_item SELECT * from siterec_dashboard.search_top_item where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_noresult SELECT * from siterec_dashboard.search_top_noresult where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_query SELECT * from siterec_dashboard.search_top_query where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.search_top_sort SELECT * from siterec_dashboard.search_top_sort where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.spider_daily_static SELECT * from siterec_dashboard.spider_daily_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.tag_general_stat SELECT * from siterec_dashboard.tag_general_stat where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
# sql_str+="INSERT into siterec_dashboard_test.user_action_static SELECT * from siterec_dashboard.user_action_static where data_time = '"+time_yesterday_str_all+"' ;"+"\r\n"
#
# sql_str+="delete from siterec_dashboard_test.action_detail_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.app_function_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.audit_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.cate_action_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.cate_general_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.classify_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.comment_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.item_action_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.item_cate_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.item_general_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.item_tag_action_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.item_tag_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.page_dest_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.page_hot_click_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.page_source_field_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.page_source_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.page_static_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.platform_static_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.potential_item_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_ck_cate_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_ck_tag_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_effect_cate_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_effect_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_effect_tag_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_flow_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_overall_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_source_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.rec_total_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.referer_action_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.referer_daily_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_effect_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_effect_static_platform where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_empty_query where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_evaluate_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_item_query_click where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_leastclick_query where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_overall_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_query_item_click where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_recent_cold where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_recent_hot where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_relate_overall_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_source_info where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_suggest_overall_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_filter where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_flip where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_item where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_noresult where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_query where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.search_top_sort where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.spider_daily_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.tag_general_stat where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
# sql_str+="delete from siterec_dashboard_test.user_action_static where data_time < '"+time_days_before_str_all+"' ;"+"\r\n"
#
#
table_list_today=[
   'app_function_static',
]
table_list_yestoday=[
    'action_detail_static',
    'audit_static',
    'cate_action_static',
    'cate_general_stat',
    'classify_static',
    'comment_static',
    'item_action_static',
    'item_cate_static',
    'item_general_stat',
    'item_tag_action_static',
    'item_tag_static',
    'page_dest_info',
    'page_hot_click_info',
    'page_source_field_info',
    'page_source_info',
    'page_static_info',
    'platform_static_info',
    'potential_item_static',
    'rec_ck_cate_stat',
    'rec_ck_tag_stat',
    'rec_effect_cate_stat',
    'rec_effect_static',
    'rec_effect_tag_stat',
    'rec_flow_stat',
    'rec_overall_static',
    'rec_source_info',
    'rec_total_static',
    'referer_action_static',
    'referer_daily_static',
    'search_effect_static',
    'search_effect_static_platform',
    'search_empty_query',
    'search_evaluate_static',
    'search_item_query_click',
    'search_leastclick_query',
    'search_overall_static',
    'search_query_item_click',
    'search_recent_cold',
    'search_recent_hot',
    'search_relate_overall_static',
    'search_source_info',
    'search_suggest_overall_static',
    'search_top_filter',
    'search_top_flip',
    'search_top_item',
    'search_top_noresult',
    'search_top_query',
    'search_top_sort',
    'spider_daily_static',
    'tag_general_stat',
    'user_action_static',
]

mysql_con = PyMysql('rdsq3zsso4e737w4gwjq.mysql.rds.aliyuncs.com', 3306, 'siterec', 'siterec123456',
                    'siterec_dashboard_test')

def siterec_dashboard_text_sync_one_table(tablename,datatime_str,datatime_before,column_name):
    # tablename = "user_model_info"
    # datatime_str = '2016-04-01 15:14:57'
    sql = "select count(1) as s from siterec_dashboard_test."+tablename+" where "+column_name+" = '"+datatime_str+"' "
    ret = mysql_con.select(sql)
    print ret
    # print type(ret[0]['s'])

    if ret[0]['s'] == 0:
        print tablename + ' no catch'
        t_tuple = tuple([datatime_str])
        sql = """
                        INSERT into siterec_dashboard_test."""+tablename+""" SELECT * from siterec_dashboard."""+tablename+""" where """+column_name+""" = %s

                    """
        ret = mysql_con.excute(sql, "one", t_tuple)
        print ret
    else:
        print tablename + ' already in'

    t_tuple = tuple([datatime_before])
    sql = """
                delete from siterec_dashboard_test.""" + tablename + """ where """+column_name+""" < %s

            """

    # print sql
    ret = mysql_con.excute(sql, "one", t_tuple)
    print ret
    print tablename + ' already del'

# siterec_dashboard_text_sync_one_table("user_model_info", '2016-04-01 15:14:57', '2016-04-01 16:14:57','create_time')
#
#
# exit()
for i in table_list_today:
    siterec_dashboard_text_sync_one_table(i, time_day_str_all, time_days_before_str_all, 'data_time')
for i in table_list_yestoday:
    siterec_dashboard_text_sync_one_table(i, time_yesterday_str_all, time_days_before_str_all, 'data_time')

# with open('siterec_dashboard_test_sync.sql', 'wb') as f:
#     f.write(sql_str)

exit()