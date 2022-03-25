# 시스템에 필요한 데이터 DB 

import boto3
import botocore
import logging

import os
import pandas as pd
import json
from decimal import Decimal
from tqdm import tqdm
from glob import glob

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
                data[k] = Decimal(str(round(v,1)))
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
    food_df = pd.DataFrame(columns=['PK','SK','qty','nutrients'])
    # PK
    food_df['PK'] = 'FOOD#' + data_df['food_cat']
    # SK
    food_df['SK'] = 'FOOD#' + data_df['food_name']
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









# after nutriai_dynamo_create_table.py
if __name__=='__main__':
    table = get_table(
        table_name=table_nutriai,
        access=aws_access
    )

    # files path
    rdi_path = os.path.join('data','RDI.csv')
    food_path = os.path.join('data','food_final.csv')
    nutrsuppl_path = os.path.join('data','nutrsuppl')
    nutrsuppl_cat = ['amino-acids','minerals','vitamins']

    # data
    rdi_data = preprocessing_rdi(rdi_path) # dict list
    food_data = preprocessing_food(food_path) # dict list
    nutrsuppl_data = preprocessing_nutrsuppl(nutrsuppl_path, nutrsuppl_cat)

    print('\nput RDI data : ')
    put_items(table, rdi_data)

    print('\nput food data : ')
    put_items(table, food_data)

    print('\nput nutrition supplement data : ')
    put_items(table, nutrsuppl_data)

    print('end uploading!!')


    print('\nTable item count: ', table.item_count)



