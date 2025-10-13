from datetime import datetime

import urllib3
from sqlalchemy.exc import SQLAlchemyError

from api import session
from models.models import BusinessContractPhone
from service.feishu_service import read_table_content

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tblalbScrLnrIavO'
view_id = 'vewV8R8ENw'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_field_value(item, field_name, default=''):
    """从item中获取字段值，支持嵌套结构"""
    value = item.get(field_name, default)
    if isinstance(value, list) and value:
        return value[0].get('text', default)
    return value

def get_nested_value(item, field_name, nested_key, default=''):
    """从item中获取嵌套字段值"""
    value = item.get(field_name, {})
    if isinstance(value, dict):
        return value.get(nested_key, default)
    return default

def add_message():
    data = read_table_content(app_token, table_id, view_id)
    try:
        for item in data:
            # 获取字段值
            contract_person = item.get('签约人')
            nickname = ''
            if item.get('昵称'):
                nickname = item.get('昵称')[0]['text']
            contract_person_group = ''
            contract_person_img = ''
            contract_person_name = ''
            contract_person_feishu = ''
            contract_person_phone = ''
            train_person_feishu = ''
            train_person_img = ''
            train_person_name = ''
            teacher = ''
            teacher_phone = ''
            xiaohongshu_url = ''
            if item.get('签约人组别'):
                contract_person_group = item.get('签约人组别').get('value', [])[0]['text']
            if item.get('签约人手机号'):
                contract_person_phone = item.get('签约人手机号').get('value', [])[0]['text']
            if item.get('班主任'):
                teacher = item.get('班主任')[0]['text']
            if item.get('班主任手机号'):
                teacher_phone = item.get('班主任手机号')[0]['text']
            if item.get('小红书主页链接'):
                xiaohongshu_url = item.get('小红书主页链接')[0]['text']
            if item.get('签约人飞书账号'):
                contract_person_feishu = item.get('签约人飞书账号').get('value', [])[0]['id']
                contract_person_img = item.get('签约人飞书账号').get('value', [])[0]['avatar_url']
                contract_person_name = item.get('签约人飞书账号').get('value', [])[0]['name']
            if item.get('培训飞书账号'):
                train_person_feishu = item.get('培训飞书账号').get('value', [])[0]['id']
                train_person_img = item.get('培训飞书账号').get('value', [])[0]['avatar_url']
                train_person_name = item.get('培训飞书账号').get('value', [])[0]['name']
            kol = session.query(BusinessContractPhone).filter(BusinessContractPhone.nickname == nickname).first()
            if kol:
                continue
            # 创建新记录
            parms = BusinessContractPhone(
                nickname=nickname,
                xiaohongshu_url=xiaohongshu_url,
                contract_person=contract_person,
                contract_person_feishu=contract_person_feishu,
                contract_person_img=contract_person_img,
                contract_person_name=contract_person_name,
                contract_person_group=contract_person_group,
                contract_person_phone=contract_person_phone,
                train_person_feishu=train_person_feishu,
                train_person_img=train_person_img,
                train_person_name=train_person_name,
                teacher=teacher,
                teacher_phone=teacher_phone,
            )
            session.add(parms)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()

add_message()