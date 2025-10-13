import requests

from service.feishu_service import search_feishu_record2, update_record

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
# 微信信息表
table_id1 = 'tbl69wH9DNnlNrTT'
view_id1 = 'vewW4ixEC6'
# 大于2000表
table_id2 = 'tbliGz3IRUgjz5Jg'
view_id2 = 'vewUY8H7K4'
# 标签表
table_id3 = 'tblnCPaLeLLyWJ5P'
view_id3 = 'vewtXRS3K8'
def automatic_product():
    print("自动打区分列以及发送邀约产品脚本已经启动")
    wx = '17660823693'
    small_tag = '母婴'
    # product = '麦当劳鲜萃有堡早餐'
    update_count = 0
    
    try:
        datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
        if not datas:
            return ["未找到符合条件的记录"]
            
        for data in datas:
            if update_count > 1:
                break
                
            record_id = data['record_id']
            fields = {
                '区分列': str(wx),
                '区分列-流量组用': str(wx),
                # '发送邀约产品': str(product),
            }
            
            # 更新记录
            result = update_record(app_token, table_id2, record_id, fields)
            update_count += 1
            
            # 返回每条处理结果
            print(f"当前微信号: {wx}, 更新次数: {update_count}")
            
        # 返回完成消息
        print("自动打标签执行完成")
        
    except Exception as e:
        print(f"处理出错: {str(e)}")

automatic_product()