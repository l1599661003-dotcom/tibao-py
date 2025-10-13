import re

import urllib3

from service.feishu_service import update_record, read_table_content
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbl4SqKYo6V2gcj7'  # 微信信息表
view_id = 'vew3A1JXT3'

"""
    给修哥标签加上,并提取博主id
"""
def automatic_product():
    try:
        datas = read_table_content(app_token, table_id, view_id)
        for data in datas:
            record_id = data['record_id']

            # 检查蒲公英链接是否存在且非空
            pgy_link = data.get('fields', {}).get('蒲公英链接', [])
            if not pgy_link:
                print(f"无效的蒲公英链接，跳过记录: {record_id}")
                continue  # 跳过此条数据，继续下一个

            # 确保匹配存在
            match = re.search(r'/blogger-detail/([a-zA-Z0-9]+)', pgy_link[0]['text'] if pgy_link else '')
            if match:
                pgy_id = match.group(1)
            else:
                print(f"没有找到匹配的蒲公英ID，跳过记录: {record_id}")
                continue  # 跳过此条数据，继续下一个

            business_tag = data.get('fields', {}).get('修哥组标签', [])[0]['text']
            formatted_tag = ",".join(business_tag.strip().split()) if business_tag else ''

            fields = {
                '蒲公英id': str(pgy_id),
                '修哥组标签': str(formatted_tag),
            }

            result = update_record(app_token, table_id, record_id, fields)
            if result:
                print(f"更新成功: {record_id} -> {fields}")
            else:
                print(f"更新失败: {record_id}")

        print("查询剩余次数执行完成")

    except Exception as e:
        print(f"处理出错: {str(e)}")

automatic_product()