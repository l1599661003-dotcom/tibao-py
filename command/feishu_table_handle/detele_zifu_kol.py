import requests
import urllib3

from core.localhost_fp_project import session
from models.models import FrontTow
from service.feishu_service import get_feishu_token

"""
    删掉自孵化博主
"""

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'
view_id = 'vewFjbwzKs'

def search_feishu_record2(app_token, table_id, view_id, field_name, field_value):
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
                    "field_name": field_name,
                    "operator": "is",
                    "value": [field_value]
                },
                {
                    "field_name": '是否发送邀约',
                    "operator": "isEmpty",
                    "value": []
                },
                {
                    "field_name": '金虎VX',
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

def delete_feishu_record(app_token, table_id, record_id):
    """删除飞书记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    headers = {
        'Authorization': f'Bearer {get_feishu_token()}'
    }

    try:
        response = requests.delete(url, headers=headers, verify=False)
        if response.status_code == 200:
            return True
        else:
            print(f"删除记录失败: {response.json()}")
            return False
    except requests.RequestException as e:
        print(f"删除记录请求失败：{e}")
        return False

def search_and_delete_records(app_token, table_id, view_id, field_name, field_value):
    """查找并删除符合条件的记录"""
    # 查找记录
    items = search_feishu_record2(app_token, table_id, view_id, field_name, field_value)

    if not items:
        print(f"未找到符合条件的记录: {field_value}")
        return

    # 删除找到的记录
    success_count = 0
    fail_count = 0

    for item in items:
        record_id = item.get('record_id')
        if record_id:
            if delete_feishu_record(app_token, table_id, record_id):
                success_count += 1
            else:
                fail_count += 1

    print(f"机构 {field_value} 的处理结果：成功删除 {success_count} 条记录，失败 {fail_count} 条")

# 主程序
provinces = session.query(FrontTow).filter(FrontTow.业务模式 == '自孵').all()
for pro in provinces:
    print(f"正在处理机构：{pro.机构名称}")
    search_and_delete_records(app_token, table_id, view_id, '达人所属机构', pro.机构名称)