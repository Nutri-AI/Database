# Design NutriAI Database
Using the AWS SDK for Python [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) to create, configure, and manage AWS services, Amazon DynamoDB.
### [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- 아마존의 NoSQL 데이터베이스 서비스
- 완전 관리형의 서버리스 데이터베이스
- 키-값 쿼리 형식

## ERD
<div align="center">
  <img width="55%" alt="NutriAI_database_ERD" src="https://github.com/Nutri-AI/.github/blob/main/profile/images/ERD_key.drawio.png">
</div>

## Design the primary key
<table>
    <thead>
        <tr>
            <th>Entity</th>
            <th>HASH</th>
            <th>RANGE</th>
        </tr>
    </thead>
    <tbody>
      <tr>
        <td>사용자</td>
        <td rowspan=4>USER#<i>USERID</i></td>
        <td>USER#<i>USERID</i>#INFO</td>
      </tr>
      <tr>
        <td>영양상태로그</td>
        <td><i>DATE</i>#NUTRSTATUS#<i>MEAL or SUPPLTAKE</i></td>
      </tr>
      <tr>
        <td>식단로그</td>
        <td><i>DATE</i>#MEAL#<i>TIME</i></td>
      </tr>
      <tr>
        <td>영양제섭취로그</td>
        <td><i>DATE</i>#SUPPLTAKE#<i>TIME</i></td>
      </tr>
      <tr>
        <td>권장섭취량</td>
        <td>RDI#<i>age_range</i></td>
        <td>RDI#<i>sex</i></td>
      </tr>
      <tr>
        <td>식품</td>
        <td>FOOD#<i>food_cat</i></td>
        <td>FOOD#<i>food_name</i></td>
      </tr>
      <tr>
        <td>영양제</td>
        <td>NUTRSUPPL#<i>nutr_cat</i></td>
        <td>NUTRSUPPL#<i>product_code</i></td>
      </tr>
    </tbody>
</table>

## Prerequisites
Edit code/aws_def_values.py.example file
   ```python
   # code/aws_def_values.py
   ## for AWS Access 
  aws_access = {
      'region_ap': '', # AWS region
      'aws_access_key_id': '', # AWS Access key ID
      'aws_secret_access_key': '' # AWS Secret access key
  }

  # DynamoDB
  table_nutriai = "" # table name
   ```

Make data/ directory and put the data files into the data/ directory for DB.

## Start with Dockerfile
Build image
   ```sh
docker build . -t <name>:<tag>
   ```
Run image
   ```sh
docker run --rm -ti -v $(pws):/dynamo <name>:<tag> bash
   ```

## Implementation

### Create a table
   ```sh
python code/nutriai_dynamo_create_table.py 
   ```

### Put items
   ```sh
python code/nutriai_dynamo_put_item.py 
   ```

### Delete a table
   ```sh
python code/nutriai_dynamo_delete_table.py
   ```

