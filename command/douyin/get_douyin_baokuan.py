import re
import json
import time
import signal
import sys

import requests
from datetime import datetime, timedelta

from core.database_text_tibao_2 import session
from models.models_tibao import BaokuanLink
from service.feishu_service import get_feishu_token

app = 'XNGibTTbzaCI58s9kd4coXsGnEQ'
table = 'tblQ8jtW0bK5LoxK'
view = 'vewu91eH0J'
def safe_get_text(field_data, default=""):
    """安全获取文本内容"""
    if isinstance(field_data, list) and len(field_data) > 0:
        return field_data[0].get('text', default)
    elif isinstance(field_data, str):
        return field_data
    return default

def safe_get_link(field_data):
    """安全获取链接"""
    if isinstance(field_data, dict):
        return field_data.get('text', '')
    return ''

def extract_video_id(link):
    """从链接中提取视频ID"""
    if not link:
        return None
    
    # 匹配多种链接格式
    patterns = [
        r'video/(\d+)/',  # 标准格式
        r'mid=(\d+)',     # mid参数格式
        r'/(\d+)/\?',     # 末尾格式
    ]
    
    for pattern in patterns:
        match = re.search(pattern, link)
        if match:
            return match.group(1)
    return None

def send_wechat_notification(content, video_id):
    """发送企业微信通知"""
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ae8f3d3e-acae-4ec6-a3dc-44abaea39fa6"
    headers = {"Content-Type": "application/json"}
    
    # 检查内容长度，防止超长
    if len(content) > 3800:
        print(f"⚠️ 内容过长({len(content)}字符)，截断发送")
        content = content[:3700] + "\n\n...(内容过长已截断)"
    
    body = {"msgtype": "markdown", "markdown": {"content": content}}

    try:
        response = requests.post(webhook_url, json=body, headers=headers, proxies={"http": None, "https": None})
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                print(f"✅ 企业微信通知发送成功: {video_id}")
                return True
            else:
                print(f"❌ 企业微信API错误: {result.get('errmsg', '未知错误')}")
                return False
        else:
            print(f"❌ HTTP请求失败: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ 请求发送异常: {str(e)}")
        return False

def main():
    try:
        print("开始查询飞书数据...")
        datas = search_feishu_record(app, table, view)
        
        if not datas:
            print("未查询到数据")
            return
            
        print(f"查询到 {len(datas)} 条数据")
        
        success_count = 0
        skip_count = 0
        
        for i, data in enumerate(datas, 1):
            
            fields = data.get("fields", {})
            
            # 获取视频链接和ID
            video_link = safe_get_link(fields.get("视频链接"))
            video_id = extract_video_id(video_link)
            
            if not video_id:
                print(f"⚠️ 无法提取视频ID，跳过: {video_link}")
                skip_count += 1
                continue
            
            # 检查是否已存在
            existing_record = session.query(BaokuanLink).filter(BaokuanLink.mid == video_id).first()
            if existing_record:
                skip_count += 1
                continue

            # 构建抖音链接
            douyin_link = f"https://www.iesdouyin.com/share/video/{video_id}/?region=CN&mid={video_id}"
            
            # 新建记录
            new_record = BaokuanLink(
                mid=video_id,
                message=json.dumps(fields, ensure_ascii=False),  # 将字典转换为JSON字符串
                douyin_link=douyin_link,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                status=0
            )

            # 处理发布时间（时间戳转正常时间）
            publish_time = fields.get("发布日期")
            formatted_time = ""
            if publish_time:
                try:
                    # 如果是毫秒时间戳，转换为秒
                    timestamp = int(publish_time)
                    if timestamp > 1e12:  # 毫秒时间戳
                        timestamp = timestamp / 1000
                    
                    # 转换为正常时间格式
                    dt = datetime.fromtimestamp(timestamp)
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    formatted_time = publish_time  # 如果转换失败，使用原值
            
            # 构建通知内容
            content = f"""🔥 全网爆款选题实时更新

【账号名】{safe_get_text(fields.get("账号名"))}
【粉丝量】{safe_get_text(fields.get("粉丝量"))}
【点赞数】{safe_get_text(fields.get("点赞数"))}
【评论数】{safe_get_text(fields.get("评论数"))}
【收藏数】{safe_get_text(fields.get("收藏数"))}
【转发数】{safe_get_text(fields.get("转发数"))}
【粉丝画像】女:{safe_get_text(fields.get("女粉占比"))}% / 男:{safe_get_text(fields.get("男粉占比"))}%
【抖音ID】{safe_get_text(fields.get("抖音id"))}
【是否为低粉爆赞】{safe_get_text(fields.get("是否为低粉爆赞"))}
【预估曝光】{safe_get_text(fields.get("预估曝光"))}
【视频类型】{safe_get_text(fields.get("视频类型"))}
【视频标题】{safe_get_text(fields.get("视频标题"))}
【发布时间】{formatted_time}

[点此查看视频]({douyin_link})"""

            try:
                # 保存到数据库
                session.add(new_record)
                session.commit()
                print(f"✅ 保存成功: {video_id}")
                time.sleep(3)
                # 发送企业微信通知
                if send_wechat_notification(content, video_id):
                    new_record.status = 1
                    new_record.updated_at = datetime.now()
                    session.commit()
                    success_count += 1
                else:
                    print(f"⚠️ 通知发送失败，但数据已保存: {video_id}")
                    
            except Exception as e:
                print(f"❌ 处理失败: {video_id}, 错误: {str(e)}")
                session.rollback()
                
        print(f"\n📊 处理完成:")
        print(f"✅ 成功处理: {success_count} 条")
        print(f"⏭️ 跳过: {skip_count} 条")
        print(f"📝 总计: {len(datas)} 条")
        
    except Exception as e:
        print(f"❌ 主程序执行失败: {str(e)}")
        session.rollback()
    finally:
        session.close()

def search_feishu_record(app_token, table_id, view_id):
    """查询飞书记录"""
    try:
        # 计算两天前0点的时间戳
        two_days_ago = datetime.now() - timedelta(days=2)
        two_days_ago_midnight = two_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp = int(two_days_ago_midnight.timestamp() * 1000)  # 转换为毫秒时间戳

        # 获取访问令牌
        access_token = get_feishu_token()
        if not access_token:
            print("❌ 获取飞书访问令牌失败")
            return None

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {access_token}'
        }
        data = {
            "view_id": view_id,
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {
                            "field_name": '是否推送该爆款选题',
                            "operator": "is",
                            "value": ['是']
                        },
                        {
                            "field_name": '发布日期',
                            "operator": "isGreater",
                            "value": ["ExactDate", str(timestamp)]
                        }
                    ]
                }
            }

        response = requests.post(url, headers=headers, json=data, verify=False, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        
        # 检查API响应
        if response_data.get('code') != 0:
            print(f"❌ 飞书API返回错误: {response_data.get('msg', '未知错误')}")
            return None
            
        items = response_data.get("data", {}).get("items", [])
        return items

    except requests.RequestException as e:
        print(f"❌ 查询飞书数据失败：{e}")
        return None
    except Exception as e:
        print(f"❌ 查询过程出现异常：{e}")
        return None

def signal_handler(signum, frame):
    """处理中断信号"""
    print(f"\n🛑 接收到中断信号，正在优雅退出...")
    print(f"📊 程序运行结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0)

def main1():
    """定时执行主程序"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    execution_count = 0
    start_time = datetime.now()
    
    print(f"🚀 爆款选题监控程序启动")
    print(f"📅 启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ 执行间隔: 30分钟")
    print(f"💡 按 Ctrl+C 可优雅退出程序")
    
    while True:
        execution_count += 1
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{'='*60}")
        print(f"🕐 第 {execution_count} 次执行 - {current_time}")
        print(f"{'='*60}")
        
        try:
            main()
            print(f"✅ 第 {execution_count} 次执行完成")
        except Exception as e:
            print(f"❌ 第 {execution_count} 次执行失败: {str(e)}")
            import traceback
            print(f"错误详情: {traceback.format_exc()}")
            # 即使出错也继续下一次执行，避免程序停止
        
        # 等待30分钟（1800秒）
        next_time = datetime.now() + timedelta(seconds=1800)
        print(f"⏰ 下次执行时间: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💤 等待30分钟后执行下一次...")
        time.sleep(1800)

if __name__ == "__main__":
    main1()