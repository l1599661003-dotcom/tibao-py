import requests

from command.培训用.UpdateTrainingBloggerDetails import search_feishu_record, update_feishu_status
from service.feishu_service import read_table_content

user_id = '62aad6fe000000001b02a4c8'
def get_pgy_token(use=""):
    """获取 Feishu Token"""
    data = read_table_content("PJISbPC5OaihG8sCfMpc4Wohnyb", "tblDO9VqC6EMHGiY", "vewUvYvleV")

    if not data:
        return ""

    token = ""

    for item in data:
        if "用途" in item['fields'] and item['fields']["用途"][0]["text"] == use:
            token = "".join(ite["text"] for ite in item['fields'].get("蒲公英token", []))

    return token.strip()
token = get_pgy_token('月初pgy18')
def blogger_info():
    url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{user_id}"
    headers = {"Cookie": token}

    response = requests.get(url, headers=headers, timeout=30, verify=False)
    print(response.json())
def notes_detail():
    url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v2/notes_detail?advertiseSwitch=1&orderType=1&pageNumber=1&pageSize=999&userId={user_id}&noteType=3&withComponent=false"
    headers = {"Cookie": token}

    response = requests.get(url, headers=headers, timeout=30, verify=False)
    print(response.json())
    if response.status_code == 200:
        print(response.json())

def main():
    search = search_feishu_record('PJISbPC5OaihG8sCfMpc4Wohnyb','tbl4buWQgvOt6bjs','vewysOdkI7','使用电脑',1)
    record_id = search[0]['record_id']
    update_feishu_status('PJISbPC5OaihG8sCfMpc4Wohnyb','tbl4buWQgvOt6bjs', record_id)
notes_detail()
# from core.database_tibao import session
# from models.models_tibao import TrainingBloggerDetails
#
# all_data = session.query(TrainingBloggerDetails.id).filter(
#     TrainingBloggerDetails.is_updated == 0,
#     TrainingBloggerDetails.id > 1908000
# ).order_by(TrainingBloggerDetails.id).all()
#
# # 转换为列表
# id_list = [row.id for row in all_data]
#
# # 按 13333 条数据分组
# num_groups = 23
# batch_size = len(id_list) // num_groups  # 计算每组的大小（平均分配）
#
# # 如果有剩余的数据，分配到前几组
# groups = [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]
#
# # 如果最后一组数据不满，则手动补齐
# if len(groups) > num_groups:
#     groups[num_groups - 1].extend(groups[num_groups])  # 合并最后两组
#     groups = groups[:num_groups]  # 确保最终有 30 组
#
# # 获取每组的第一个和最后一个 id
# result = [(group[0], group[-1]) for group in groups]
#
# # 打印结果
# for i, (first_id, last_id) in enumerate(result, start=1):
#     print(f"组 {i}: first_id={first_id}, last_id={last_id}")