'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2023-07-31 16:56:25
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-18 13:53:14
FilePath: /database/heat_db/heat_db.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''

import psycopg2
import pandas as pd

iot_obix_control_zd={"ch_1_ch":['mubiao_t','dangqian_t','s','yx_yj_num','zf_rw_t','zf_sw_t','ln_rw_t','ln_sw_t','pq_t','zf_t','ln_t','ln_flw','zf_flw','bh','fd','yj_gz','yj_gy','yj_dy','f1','s2','yj_shiyong_1','yj_shiyong_2','yxms','t_set_cool','t_set_hot','ms',],#地源热泵机组
                     'cp_dyb_2':['p_5','p_6','bi_dyb_s','bi_dyb_s','bi_dyb_f','bi_dyb_am','ai_dyb_bp_fk','ao_dyb_bp_c',],#地源热泵井侧泵
                     'cp_yhb_3':['p_7','p_8','bi_yhb_s','bi_yhb_f','bi_yhb_am','ai_yhb_bp_fk','ao_yhb_bp_c'],#用户侧泵
                     'cp_hhwp5_1':['p_11','p_12','bi_hhwp_s','ai_hhwp_bp_fk','ao_hhwp_bp_c'],#锅炉循环泵5_1
                     'cp_hhwp5_2':['p_13','p_14','bi_hhwp_s','ai_hhwp_bp_fk','ao_hhwp_bp_c'],#锅炉循环泵5_2
                     'cp_hhwp5_3':['p_15','p_16','bi_hhwp_s','ai_hhwp_bp_fk','ao_hhwp_bp_c'],#锅炉循环泵5_3
                     'cp_xhb10_1':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_33','p_34',],#氢燃料电池预热循环泵10-1
                     'cp_xhb10_2':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_35','p_36',],#氢燃料电池预热循环泵10-2
                     'cp_xhb7_1':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_29','p_30',],#蓄水箱循环泵7_1
                     'cp_xhb7_2':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_31','p_32',],#蓄水箱循环泵7_2
                     'cp_xhb11_1':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_23','p_24',],#主循环泵11_1
                     'cp_xhb11_2':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_25','p_26',],#主循环泵11_2
                     'cp_xhb11_3':['bi_xhb_s','ai_xhb_bp_fk','ao_xhb_bp_c','p_27','p_28',],#主循环泵11_3
                     'drj_drj1':['f1_ll','t2','p38'],#进地热井1
                     'drj_drj2':['f2_ll','t4','p40'],#进地热井2
                     'drj_drj3':['f3_ll','t6','p42'],#进地热井3
                     'ch_1':['t_1','t_2','p_1','p_2','t_3','t_4','p_3','p_4'],#地源热泵进出水压力温度，用户侧进出水压力温度
                     'wt':['ai_est_l','ai_est_p','t_5','p_49','t_6'],#蓄水箱液位压力，出蓄水箱压力温度
                     'sxt':['ai_sw_p5','ai_sw_t14','ai_rw_t15','ai_rw_p6'],#接DK-1,DK-2供水管压力温度，锅炉回水温度压力
                     'e_f_e1':['e1_t2','e1_ll','e1_rl','e1_t1',],#"出地热井水管总管水管冷热量监测E1(灌热量)"
                     'e_f_e2':['e2_t2','e2_ll','e2_rl','e2_t1',],#"能源站供水管冷热量监测E2(能源站负荷)"
                     'e_f_e3':['e3_t2','e3_ll','e3_rl','e3_t1',],#"接DK-1及DK-2地块供水管冷热量监测E3(运动员村冷热负荷)"
                     'e_f_e4':['e4_t2','e4_ll','e4_rl','e4_t1',],#"氢燃料电池余热冬季蓄热时出水管冷热量监测E4()"
                     'e_f_e5':['e5_t2','e5_ll','e5_rl','e5_t1',],#"地源热泵夏季夜间往蓄水罐蓄冷时出水管冷热量监测E5"
                     'e_f_e6':['e6_t2','e6_ll','e6_rl','e6_t1',],#"电锅炉冬季蓄热时出水管冷热量监测E6"
                     'e_f_f':['f7_ll','f8_ll','f9_ll','f10_ll',]#进地热井总管流量，夏季夜间往蓄水罐蓄冷供水管冷热，地源热泵蒸发侧流量，地缘侧流量
}

# def select_from_rljc_gl():
#     # 连接数据库
#     try:
#         db = psycopg2.connect(host='192.168.3.13', port=5432, user='postgres', password='postgres',
#                               database='iot_obix_control')
#     except Exception as e_:
#         # logging.error('无法连接到iot_obix_control数据库,错误原因为')
#         # logging.error('失败,错误原因为{}'.format(e_))
#         raise e_
#     # db.close()
#     cursor = db.cursor()
#     gl1 = {'T_eb1_out':0,'T_eb1_in':0,'F_eb1':0}
#     try:
#         # sql语句 建表
#         sql = """SELECT  rw_t,create_time FROM gl1 where gl1.create_time<now()::timestamp and gl1.create_time>now()::timestamp + '-24 hour';"""
#         cursor.execute(sql)
#         rest = cursor.fetchall()
#         t_out_sum = 0
#         t_in_sum = 0
#         j=0
#         for i in range(len(rest)):
#             if float(rest[i][0]) > float(rest[i][1]) - 3:
#                 t_out_sum = t_out_sum + float(rest[i][0])
#                 t_in_sum = t_in_sum + float(rest[i][1])
#                 j=j+1
#         if j>0:
#             gl1['T_eb1_out'] = t_out_sum / j
#             gl1['T_eb1_in'] = t_in_sum / j
#         else:
#             gl1['T_eb1_out'] = 57.2
#             gl1['T_eb1_in'] = 46.01
#         gl1['F_eb1'] = 0
#     except Exception as e_:
#         # logging.error(f'读取iot_obix_control数据失败,数据为为{rest}，错误原因为')
#         # logging.error('失败,错误原因为{}'.format(e_))
#         raise e_
#     # 关闭数据库连接
#     db.close()
#     return gl1

def ts():
    # db = psycopg2.connect(host='192.168.3.13', port=5432, user='postgres', password='postgres',
    #                           database='iot_obix_control')
    db = psycopg2.connect(host='123.249.70.226', port=7004, user='postgres', password='postgres', database='iot_obix_control')

    with pd.ExcelWriter('heat_db/heat_data/heat_hydro_data.xlsx') as writer:
        for i in iot_obix_control_zd.keys():
            tmp=''
            for j in iot_obix_control_zd[i]:
                tmp=tmp+j+','
            sql="SELECT "+tmp+"create_time FROM "+i+" where "+i+".create_time<now()::timestamp and "+i+".create_time>now()::timestamp + '-120 hour';"
            cursor = db.cursor()
            cursor.execute(sql)
            rest = cursor.fetchall()
            rest_list=[list(rest[k])for k in range(len(rest))]            
            cocolumns_tmp=iot_obix_control_zd[i]
            cocolumns_tmp.append('create_time')
            rest_df=pd.DataFrame(rest_list,columns=cocolumns_tmp)
            tmp_r_df=rest_df.sort_values(by='create_time')
            tmp_r_df.set_index('create_time').to_excel(writer,sheet_name=i)
            print(i)
    db.close()
if __name__ == '__main__':
    # select_from_rljc_gl()
    ts()