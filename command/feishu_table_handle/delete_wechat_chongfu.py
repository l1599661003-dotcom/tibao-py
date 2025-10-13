import requests
import urllib3
import time
import random

from service.feishu_service import get_feishu_token
from unitl.unitl import get_text_from_items
"""
    微信重复号脚本
"""
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'
view_id = 'vewFjbwzKs'

def search_feishu_record(app_token, table_id, view_id):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": '连续30天负增长',
                    "operator": "isNotEmpty",
                    "value": []
                } ,{
                    "field_name": '内容类目1',
                    "operator": "isEmpty",
                    "value": []
                }
            ]
        }
    }

    try:
        # 第一次查询
        response = requests.post(url, headers=headers, json=data, verify=False)
        response_data = response.json()

        # 获取总记录数和第一页数据
        total = response_data.get("data", {}).get("total", 0)
        items = response_data.get("data", {}).get("items", [])
        page_token = response_data.get("data", {}).get("page_token", "")

        # 如果总记录数大于 500，继续分页查询
        if total > 500:
            page = (total + 499) // 500  # 计算总页数
            for i in range(1, page):
                # 添加 page_token 参数
                paginated_url = f"{url}&page_token={page_token}"
                paginated_response = requests.post(paginated_url, headers=headers, json=data, verify=False)
                paginated_data = paginated_response.json()

                # 合并当前页数据
                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                # 如果没有下一页，退出循环
                if not page_token:
                    break

        return items

    except requests.RequestException as e:
        print(f"查询飞书单条信息失败：{e}")
        return None

def update_record(app_token, table_id, record_id, fields):
    """更新飞书记录"""
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    body = {
        'fields': fields
    }
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    try:
        response = requests.put(url, headers=headers, json=body, verify=False)
        response_data = response.json()
        if response_data.get('code') == 0:
            print(f'记录更新成功: {record_id}')
        else:
            print(f'记录更新失败: {response_data}')
    except requests.RequestException as e:
        print(f"更新记录失败: {e}")

def handle_wechat_duplicates():
    try:
        print("开始处理内容类目...")

        # 1. 获取金虎VX为空的重复微信号记录
        data = search_feishu_record(app_token, table_id, view_id)
        if not data:
            print('获取数据失败')
            return

        # 2. 遍历每条记录，更新内容类目1
        for item in data:
            fields = item.get('fields', {})
            record_id = item.get('record_id')  # 获取记录ID
            tags = fields.get('标签', [])  # 获取标签字段

            # 如果标签字段存在且不为空，取第一个值赋值给内容类目1
            if tags:  # 检查标签是否存在且不为空
                content_category = tags[0]  # 取标签的第一个值
                update_fields = {
                    '内容类目1': content_category  # 更新内容类目1字段
                }
                # 调用更新函数
                update_record(app_token, table_id, record_id, update_fields)
                print(f"记录 {record_id} 的 '内容类目1' 已更新为: {content_category}")
            else:
                print(f"记录 {record_id} 的 '标签' 字段为空，跳过更新")

    except Exception as e:
        print(f"处理过程中发生错误: {e}")

if __name__ == '__main__':
    handle_wechat_duplicates()