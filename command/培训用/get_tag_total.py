import urllib3

from service.feishu_service import search_feishu_record2, update_record, read_table_content
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbl0SmOTT2KbgHe4'  # 微信信息表
view_id = 'vewtXRS3K8'
table_id2 = 'tbliGz3IRUgjz5Jg'  # 大于2000表
view_id2 = 'vewUY8H7K4'

"""
    获取各个内容类目所剩个数
"""

def automatic_product():
    print("自动打区分列以及发送邀约产品脚本已经启动")

    try:
        tags = read_table_content(app_token, table_id, view_id)
        for tag in tags:
            record_id = tag['record_id']
            small_tag_list = tag.get('fields', {}).get('小类', [])
            if not small_tag_list:
                continue
            small_tag = small_tag_list[0]

            datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
            update_count = len(datas) if datas else 0

            fields = {
                '个数': int(update_count),
            }
            result = update_record(app_token, table_id, record_id, fields)
            if result:
                print(f"更新成功: {record_id} -> {small_tag} ({update_count})")
            else:
                print(f"更新失败: {record_id}")

        print("查询剩余次数执行完成")

    except Exception as e:
        print(f"处理出错: {str(e)}")


automatic_product()