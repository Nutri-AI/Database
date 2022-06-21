# 시스템에 필요한 데이터 DB 
from distutils.log import error
import boto3
import botocore
import logging

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
from decimal import *
from tqdm import tqdm
from glob import glob
import re

from aws_def_values import *



# get Table
def get_table(table_name:str, access:dict):
    db_resource = boto3.resource(
        'dynamodb',
        region_name = access['region_ap'],
        aws_access_key_id=access['aws_access_key_id'],
        aws_secret_access_key=access['aws_secret_access_key']
    )
    print('get DynamoDB resource')
    try:
        table = db_resource.Table(table_name)
        print(f'get {table.table_name} Table')
        return table
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        print(f'no {table_name} table')
        return None


################
### put_item ###
################

# convert type, str to num, before put_item
def convert_Decimal(data):
    for k, v in data.items():
        if type(v) == dict:
            convert_Decimal(v)
        else:
            try:
                data[k] = Decimal(str(v)).quantize(Decimal('.1'), rounding=ROUND_HALF_UP)
            except:
                pass
    return data

# put item: dict or dict list
def put_items(table, data):
    if type(data) == list:
        for item in tqdm(data):
            try:
                convert_Decimal(item)
                table.put_item(
                    Item=item,
                    # ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists(SK)'
                )
            except botocore.exceptions.ClientError as e:
                logging.error(e)
    elif type(data) == dict:
        try:
            convert_Decimal(data)
            table.put_item(
                Item=item,
                # ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists(SK)'
            )
        except botocore.exceptions.ClientError as e:
            logging.error(e)
    else:
        pass



#####################
### preprocessing ###
#####################

# 음식 영양 정보 : csv -> json
# column order : 'food_cat', 'food_name', 'serving_amount', 'serving_unit' and 'nutrients'
def preprocessing_food(data_path:str) -> list:
    data_df = pd.read_csv(data_path, na_values=0, dtype=str)
    food_df = pd.DataFrame(columns=['PK','SK','cmpny','qty','nutrients'])
    # SK
    food_df['SK'] = 'FOOD#' + data_df['food_name']
    # PK
    food_df['PK'] = 'FOOD#' + data_df['food_cat']
    # cmpny
    food_df['cmpny'] = 'not' #data_df['cmpny']
    # qty
    food_df['qty'] = data_df[['serving_amount','serving_unit']].apply(dict, axis=1)
    # nutrients
    drop0 = lambda row: row.dropna().to_dict()
    food_df['nutrients'] = data_df[data_df.columns[4:]].apply(drop0, axis=1)
    # to json
    food_json_str = food_df.to_json(orient='records', force_ascii=False)
    food_json_list = json.loads(food_json_str)
    return food_json_list

# 권장 섭취량 정보 : csv
# column : 'PK', 'SK' and nutrients 'RDI'
def preprocessing_rdi(data_path:str) -> list:
    data_df = pd.read_csv(data_path, dtype=str)
    rdi_df = pd.DataFrame(columns=['PK','SK','RDI'])
    # PK
    rdi_df['PK'] = data_df['PK']
    # SK
    rdi_df['SK'] = data_df['SK']
    # nutrients
    drop0 = lambda row: row.dropna().to_dict()
    rdi_df['RDI'] = data_df[data_df.columns[2:]].apply(drop0, axis=1)
    # to json
    rdi_json_str = rdi_df.to_json(orient='records', force_ascii=False)
    rdi_json_list = json.loads(rdi_json_str)
    return rdi_json_list

# 영양제 정보 : json
def preprocessing_nutrsuppl(data_path:str, nutrsuppl_cat:list) -> list:
    nutrsuppl_list = list()
    for cat in nutrsuppl_cat:
        file_path = glob(os.path.join(data_path,cat,'*'))
        for file in file_path:
            with open(file, 'r', encoding='utf-8-sig') as json_file:
                file = json.load(json_file)
                # PK attr
                file['PK'] = f'NUTRSUPPL#{cat}'
                # SK attr
                temp = file.pop('prod_cd')
                file['SK'] = f'NUTRSUPPL#{temp}'
                # nutrsuppl_url
                file['nutrsuppl_url'] = file.pop('url')
                # nutrients attr
                for nutr in file['nutrients']:
                    temp_nutr = file['nutrients'].get(nutr)[0]
                    file['nutrients'][nutr] = temp_nutr
                # serving attr
                temp_srv = file['serving']
                file['serving'] = {
                    'serving_amount': temp_srv[0],
                    'serving_unit': temp_srv[1]
                }
                nutrsuppl_list.append(file)
    return nutrsuppl_list

# 바코드 정보 : csv -> json
# column : 'prdt', 'brcd', 'cmpny', 'serving_amount', 'serving_unit' and 'nutrients'
def preprocessing_brcd(data_path:str) -> list:
    data_df = pd.read_csv(data_path, na_values=0, dtype=str)
    data_col = data_df.columns
    brcd_df = pd.DataFrame(columns=['PK','SK','food_name','cmpny','food_cat','qty','nutrients'])
    # SK
    brcd_df['SK'] = 'BRCD#' + data_df['brcd']
    # PK
    brcd_df['PK'] = 'BRCD#brcd'
    # food_name
    brcd_df['food_name'] = data_df['prdt']
    # cmpny
    brcd_df['cmpny'] = data_df['cmpny']
    # food_cat
    brcd_df['food_cat'] = data_df['food_cat']
    # qty
    brcd_df['qty'] = data_df[['serving_amount','serving_unit']].apply(dict, axis=1)
    # nutrients
    to_del = ['prdt', 'brcd', 'cmpny', 'year', 'food_cat', 'serving_amount', 'serving_unit',]
    nutr_col = [col for col in data_col if col not in to_del]
    drop0 = lambda row: row.dropna().to_dict()
    brcd_df['nutrients'] = data_df[nutr_col].apply(drop0, axis=1)
    # to json
    food_json_str = brcd_df.to_json(orient='records', force_ascii=False)
    food_json_list = json.loads(food_json_str)
    return food_json_list






# after nutriai_dynamo_create_table.py
if __name__=='__main__':
    table = get_table(
        table_name=table_nutriai,
        access=aws_access
    )

    # files path
    rdi_path = os.path.join(os.pardir,'dynamo','data','RDI_final.csv')
    food_path = os.path.join(os.pardir,'dynamo','data','food_final.csv')
    food_search_path = os.path.join(os.pardir,'dynamo','data','food_search_final.csv')
    nutrsuppl_path = os.path.join(os.pardir,'dynamo','data','nutrsuppl')
    nutrsuppl_cat = ['amino-acids','minerals','vitamins']
    brcd_path = os.path.join(os.pardir,'dynamo','data','brcd_final.csv')

    # data
    print("\nData Preprocessing...")
    rdi_data = preprocessing_rdi(rdi_path) # dict list
    food_data = preprocessing_food(food_path) # dict list
    food_search_data = preprocessing_food(food_search_path)
    nutrsuppl_data = preprocessing_nutrsuppl(nutrsuppl_path, nutrsuppl_cat)
    brcd_data = preprocessing_brcd(brcd_path)

    print('\nput RDI data...')
    put_items(table, rdi_data)

    print('\nput food data...')
    put_items(table, food_data)

    print('\nput food data for searching...')
    put_items(table, food_search_data)

    print('\nput nutrition supplement data...')
    put_items(table, nutrsuppl_data)

    print('\nput barcode data...')
    put_items(table, brcd_data)

    print('\nend uploading!!')


    # print('\nTable item count: ', table.item_count)


