import re
import time
import requests
from core.database_text_tibao_2 import session
from models.models_tibao import KolProfileDataWaicai

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'www.xiaohongshu.com',
    'Priority': 'u=0, i',
    'Referer': 'https://pgy.xiaohongshu.com/',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

records = session.query(KolProfileDataWaicai).filter(
    (KolProfileDataWaicai.kol_intro.is_(None)) |
    (KolProfileDataWaicai.kol_intro == '')
).all()

for record in records:
    try:
        blogger_id = record.blogger_id
        print(f"处理博主ID: {blogger_id}")
        
        # 使用网页抓取方法（按照PHP代码逻辑）
        url = f"https://www.xiaohongshu.com/user/profile/{blogger_id}"
        print(f"访问链接: {url}")
        
        # 添加延迟避免请求过快
        time.sleep(1)
        
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        html = response.text
        
        # 使用正则表达式匹配用户简介内容（按照PHP代码逻辑）
        match = re.search(r'<div class="user-desc"[^>]*>(.*?)</div>', html, re.S)
        if match:
            user_desc = match.group(1).strip()
            print(f"用户简介: {user_desc}")
            
            # 更新数据库
            record.kol_intro = user_desc
            session.commit()
            print(f"已更新数据库: {blogger_id} → {user_desc[:20]}...")
        else:
            print(f"未找到用户简介: {blogger_id}")
            # 打印HTML片段用于调试
            print("HTML片段（用于调试）:")
            print(html[:1000] + "..." if len(html) > 1000 else html)

    except Exception as e:
        print(f"处理失败: {blogger_id} → {e}")
        session.rollback()