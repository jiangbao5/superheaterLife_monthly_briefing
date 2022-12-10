# -*- coding: utf-8 -*-
import datetime
import time
from numpy import *
#from datetime import timedelta
import requests
import json
import pandas as pd
import re
import getConfig as GETCFG
#import base64

Huatuo_DATA_API_PREFIX = 'http://csass.huatuo.crepri:30109/'
INPUT_SUFFIX = '/briefing_input_data/'
OUTPUT_SUFFIX = '/briefing_output_data/'

def briefing_data_read_in(algorithm_name, power_plant_en_name, unit_id, start_time, end_time):
    read_in_url=Huatuo_DATA_API_PREFIX+algorithm_name+INPUT_SUFFIX
    headers = {
        'Content-Type': 'application/json',
    }
    paras_get = {
        "power_plant_en_name":power_plant_en_name,
        "unit_id":unit_id,
        "briefing_data_start_timestamp":start_time,
        "briefing_data_end_timestamp":end_time,
    }
    result = requests.get(read_in_url, params=paras_get,headers=headers)
    return [result.status_code, result.json()]

def briefing_data_write_out(algorithm_name, data):
    write_out_url = Huatuo_DATA_API_PREFIX+algorithm_name+OUTPUT_SUFFIX
    result = requests.post(write_out_url, data=json.dumps(data))
    return [result.status_code, result.json()]

def get_monthly_start_end_time():
    time_dic={}
    current_time = datetime.datetime.now()
    current_time = datetime.datetime.strptime("2023-01-01:01:00:00", '%Y-%m-%d:%H:%M:%S')    #测试
    print("current_time", current_time,current_time.strftime('%d:%H:%M:%S'))

    if current_time.strftime('%d:%H:%M:%S')=="01:01:00:00":
        if current_time.strftime('%m')=="01":
           this_month_start = datetime.datetime(current_time.year-1, 12, 1)        #本月第一天0点0分
           this_month_end =datetime.datetime(current_time.year, current_time.month, 1)            #本月最后一天0点0分
        else:
            this_month_start = datetime.datetime(current_time.year, current_time.month-1, 1)
            this_month_end =datetime.datetime(current_time.year, current_time.month, 1)
        print(this_month_start,this_month_end)

        if this_month_start.strftime('%m')=="01":
           last_month_start = datetime.datetime(this_month_start.year-1, 12, 1)       #上月第一天0点0分
           last_month_end =datetime.datetime(this_month_start.year, this_month_start.month, 1)     #上月最后一天0点0分
        else:
            last_month_start = datetime.datetime(this_month_start.year, this_month_start.month-1, 1)
            last_month_end =datetime.datetime(this_month_start.year, this_month_start.month, 1)

        last_month_start_time = last_month_start.strftime('%Y-%m-%d:%H:%M:%S')         # 数据起始时间（字符串）
        last_month_end_time = last_month_end.strftime('%Y-%m-%d:%H:%M:%S')            # 数据截止时间（字符串）
        print("last_month_start_time:", last_month_start_time,"\n","last_month_end_time:", last_month_end_time)

        this_month_start_time = this_month_start.strftime('%Y-%m-%d:%H:%M:%S')         # 数据起始时间（字符串）
        this_month_end_time = this_month_end.strftime('%Y-%m-%d:%H:%M:%S')            # 数据截止时间（字符串）
        print("this_month_start_time:", this_month_start_time,"\n","this_month_end_time:", this_month_end_time)

        time_dic["last_month_start_time"]=last_month_start_time
        time_dic["last_month_end_time"] = last_month_end_time
        time_dic["this_month_start_time"]=this_month_start_time
        time_dic["this_month_end_time"] = this_month_end_time

        return time_dic

# def get_figture_byte(path):
#     with open(path,'rb') as f:
#         img_byte=base64.b64encode(f.read())
#     img_str=img_byte.decode('ascii')
#     print("img_str",img_str)
#     return img_str

def get_monthly_avg_superheater_life(algorithm_name, power_plant_en_name, unit_id, start_time, end_time):

    global ret_read

    avg_superheater_life_month=0
    data_last={}

    while True:
        try:
            ret_read=briefing_data_read_in(algorithm_name, power_plant_en_name, unit_id, start_time, end_time)
            print("========","获取到",start_time,"至",end_time,"的数据","========")
            status_code = ret_read[0]
            # 检测函数返回结果中的状态码
            if status_code != 200:
                # 数据读取发生错误。
                error_msg = ret_read[1]
                print("status_code: ", status_code, ".", "error_info: ", error_msg)
                time.sleep(60)
                continue
            else:
                # 数据读取输入正确。
                break
        except Exception as error:
            print("error_info: ", error.__str__())
            time.sleep(600)
            continue

    if ret_read :
        data = ret_read[1]["input_data_list"]                     #获取本月的所有数据
        if data !=[] :
            input_data_list_num = len(data)                      # 获取input_data_list中数据的个数
            ret_read_first = data[0]                                  # 获取input_data_list中数据的第一条
            ret_read_last = data[input_data_list_num - 1]                 # 获取input_data_list中数据的最后一条
            data_first = ret_read_first["life_loss_current"]            #获取input_data_list中第一条信息中的life_loss_all
            data_last = ret_read_last["life_loss_all"]              #获取input_data_list中最后一条信息中的life_loss_all

            pointNameList = list(data_first.keys())
            df = pd.DataFrame([])
            for i in range(input_data_list_num):
                life_loss_current_list=list(data[i]["life_loss_current"].values())
                row = pd.DataFrame([life_loss_current_list])       # raw_timestamp
                df = df.append(row)
            df.columns = pointNameList
            #print("df", df, type(df))

            #月寿命损耗均值
            list_value = []
            for pointName in pointNameList:
                month_sum = df[pointName].sum()
                Nonzero_num = df[pointName][df[pointName] > 0].count()
                if Nonzero_num != 0:
                    list_value.append(month_sum/Nonzero_num)
                else:
                    list_value.append(0)
            avg_superheater_life_month = mean(list_value)
            avg_superheater_life_month = round(avg_superheater_life_month * 100, 6)

    return avg_superheater_life_month,data_last

def get_monthly_briefing_data(power_plant_belonging,power_plant_belonging_cn,algorithm_name, power_plant_en_name,power_plant_cn_name, unit_id,time_dic):

    global Tube_number11, Tube_number21, Tube_number31, Tube_number41, Tube_number51, Tube_number12, Tube_number22, Tube_number32, Tube_number42, Tube_number52, life_loss11, life_loss21, life_loss31, life_loss41, life_loss51, life_loss12, life_loss22, life_loss32, life_loss42, life_loss52, last_month_ret_read

    last_month_start_time = time_dic["last_month_start_time"]
    last_month_end_time = time_dic["last_month_end_time"]
    this_month_start_time = time_dic["this_month_start_time"]
    this_month_end_time = time_dic["this_month_end_time"]

    # algorithm_fig=get_figture_byte('test.png')

    #本月寿命损耗均值
    this_month_avg_superheater_life, this_month_data_last = get_monthly_avg_superheater_life(algorithm_name, power_plant_en_name,unit_id,this_month_start_time, this_month_end_time)
    print("this_month_avg_superheater_life", this_month_avg_superheater_life)

    #上月寿命损耗均值。
    last_month_avg_superheater_life, last_month_data_last = get_monthly_avg_superheater_life(algorithm_name, power_plant_en_name, unit_id, last_month_start_time, last_month_end_time)
    print("last_month_avg_superheater_life",last_month_avg_superheater_life)

    #本月较上月寿命损耗增长率
    if last_month_avg_superheater_life != 0 :
        if this_month_avg_superheater_life != 0 :
            superheater_life_growth_rate=(this_month_avg_superheater_life-last_month_avg_superheater_life)/last_month_avg_superheater_life
            superheater_life_growth_rate = round(superheater_life_growth_rate * 100, 6)
            superheater_life_growth_rate = str(superheater_life_growth_rate)
            print("superheater_life_growth_rate",superheater_life_growth_rate)
        else:
            superheater_life_growth_rate="机组未运行,本月寿命损耗为0"
    else:
        superheater_life_growth_rate="机组未运行,上月寿命损耗为0"

    this_month_avg_superheater_life = str(this_month_avg_superheater_life)
    #截止本月底总寿命损耗排序，取前10根管
    data_last = sorted(this_month_data_last.items(), key=lambda x: x[1], reverse=True)
    print("data_last ", data_last)
    data_last = data_last[0:10]
    print("data_last ", data_last)

    Tube_number11 = re.sub("\D", "", data_last[0][0])
    life_loss11 = round(data_last[0][1] * 100, 6)
    Tube_number21 = re.sub("\D", "", data_last[1][0])
    life_loss21 = round(data_last[1][1] * 100, 6)
    Tube_number31 = re.sub("\D", "", data_last[2][0])
    life_loss31 = round(data_last[2][1] * 100, 6)
    Tube_number41 = re.sub("\D", "", data_last[3][0])
    life_loss41 = round(data_last[3][1] * 100, 6)
    Tube_number51 = re.sub("\D", "", data_last[4][0])
    life_loss51 = round(data_last[4][1] * 100, 6)
    Tube_number12 = re.sub("\D", "", data_last[5][0])
    life_loss12 = round(data_last[5][1] * 100, 6)
    Tube_number22 = re.sub("\D", "", data_last[6][0])
    life_loss22 = round(data_last[6][1] * 100, 6)
    Tube_number32 = re.sub("\D", "", data_last[7][0])
    life_loss32 = round(data_last[7][1] * 100, 6)
    Tube_number42 = re.sub("\D", "", data_last[8][0])
    life_loss42 = round(data_last[8][1] * 100, 6)
    Tube_number52 = re.sub("\D", "", data_last[9][0])
    life_loss52 = round(data_last[9][1] * 100, 6)

    #月度简报中的页眉时间
    header_start_cn_tim=this_month_start_time[0:4]+"年"+this_month_start_time[5:7]+"月份"
    #月度简报中的计算时间段
    start_cn_time=this_month_start_time[0:4]+"年"+this_month_start_time[5:7]+"月"+this_month_start_time[8:10]+"日"
    #print("start_en_time",start_en_time)
    end_cn_time=this_month_end_time[0:4]+"年"+this_month_end_time[5:7]+"月"+this_month_end_time[8:10]+"日"
    #print("end_en_time",end_en_time)

    briefing_json_generating_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #简报提交内容
    output_buff = {
                    "algo_name": algorithm_name,
                    "briefing_data_start_timestamp": this_month_start_time,
                    "briefing_data_end_timestamp": this_month_end_time,
                    "briefing_json_generating_timestamp": briefing_json_generating_timestamp,
                    "power_plant_belonging": power_plant_belonging,
                    "power_plant_en_name": power_plant_en_name,
                    "unit_id": unit_id,

                    # 简报所需的所有变动数据
                    "briefing_data":{
                        "data": {
                        "algo_name": algorithm_name,
                        "briefing_data_start_timestamp": this_month_start_time,
                        "briefing_data_end_timestamp": this_month_end_time,
                        "briefing_json_generating_timestamp": briefing_json_generating_timestamp,
                        "power_plant_belonging": power_plant_belonging,
                        "power_plant_belonging_cn": power_plant_belonging_cn,
                        "power_plant_en_name": power_plant_en_name,
                        "power_plant_cn_name": power_plant_cn_name,
                        "unit_id": unit_id,
                        "header_start_cn_tim": header_start_cn_tim,
                        "start_cn_time": start_cn_time,
                        "end_cn_time": end_cn_time,
                        "this_month_avg_superheater_life": this_month_avg_superheater_life,
                        "superheater_life_growth_rate": superheater_life_growth_rate,

                        "Tube_number11": Tube_number11, "life_loss11": '%.4f' % life_loss11,
                        "Tube_number21": Tube_number21, "life_loss21": '%.4f' % life_loss21,
                        "Tube_number31": Tube_number31, "life_loss31": '%.4f' % life_loss31,
                        "Tube_number41": Tube_number41, "life_loss41": '%.4f' % life_loss41,
                        "Tube_number51": Tube_number51, "life_loss51": '%.4f' % life_loss51,
                        "Tube_number12": Tube_number12, "life_loss12": '%.4f' % life_loss12,
                        "Tube_number22": Tube_number22, "life_loss22": '%.4f' % life_loss22,
                        "Tube_number32": Tube_number32, "life_loss32": '%.4f' % life_loss32,
                        "Tube_number42": Tube_number42, "life_loss42": '%.4f' % life_loss42,
                        "Tube_number52": Tube_number52, "life_loss52": '%.4f' % life_loss52
                        },
                        "figures": {
                            "algorithm_fig1": "algorithm_fig"
                        }
                                       }
                    }
    print("output_buff: ", json.dumps(output_buff, ensure_ascii=False))
    while (True):
         try:
             ret_write = briefing_data_write_out(algorithm_name, output_buff)
             #print("ret_write",ret_write)
             status_code = ret_write[0]
             # 检测函数返回结果中的状态码
             if status_code != 200:
                 # 数据读取发生错误。
                 error_msg = ret_write[1]
                 print("status_code: ", status_code, ".", "error_info: ", error_msg)
                 time.sleep(60)
                 continue
             else:
                 # 数据读取输入正确。
                 break
         except Exception as error:
             print("error_info: ", error.__str__())
             time.sleep(600)
             print('catch exception', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
             continue
    print("=================输出数据,写出成功=================================")

if __name__ == '__main__':
    monthly_briefing_list = GETCFG.monthly_briefing_list
    algorithm_name="superheater_life"
    while True:
         time_dic= get_monthly_start_end_time()
         if not time_dic:
             continue
         else:
            for i in range(len(monthly_briefing_list)):
                for j in range(len(monthly_briefing_list[i]["units_id"])):
                    power_plant_belonging = monthly_briefing_list[i]["power_plant_belonging"]
                    power_plant_belonging_cn=monthly_briefing_list[i]["power_plant_belonging_cn"]
                    power_plant_en_name = monthly_briefing_list[i]["power_plant_en_name"]
                    power_plant_cn_name = monthly_briefing_list[i]["power_plant_cn_name"]
                    unit_id = monthly_briefing_list[i]["units_id"][j]
                    print("==========","获取",power_plant_cn_name,unit_id,"机组参数","=============")
                    get_monthly_briefing_data(power_plant_belonging,power_plant_belonging_cn,algorithm_name, power_plant_en_name,power_plant_cn_name, unit_id,time_dic)
                    print(power_plant_belonging,power_plant_cn_name,unit_id,"月度简报输出")

