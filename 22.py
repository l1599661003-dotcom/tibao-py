import time
import random
import string

import requests

from core.localhost_fp_project import session
from models.models import PgyUser, PgyNoteDetail

def generate_trace_id():
    """生成追踪ID"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))

def generate_xray_trace_id():
    """生成X-Ray追踪ID"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=32))

def get_current_timestamp():
    """获取当前时间戳（毫秒）"""
    return str(int(time.time() * 1000))

def get_notes_detail(user_id, xsec_token, cursor=''):
    print(f"https://edith.xiaohongshu.com/api/sns/web/v1/user_posted?num=30&cursor={cursor}&user_id={user_id}&image_formats=jpg,webp,avif&xsec_token={xsec_token}&xsec_source=pc_search")
    exit()
    try:
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cache-Control': 'no-cache',
            'Cookie': 'a1=19882f48bcap04qq1r777p9ggafrdrroz4abocygo50000339517; webId=e7111fec356dc781ca1d14d236afda9a; customerClientId=016216997022661; abRequestId=e7111fec356dc781ca1d14d236afda9a; x-user-id-creator.xiaohongshu.com=634cc30badd08a00019ee4e3; gid=yjYSKK8dd0uiyjYYJi4YD4SvS0U84AAyFWWW0klUjkk0iD28FfFFll888qqj2yW8DY2KKijK; webBuild=4.81.0; xsecappid=xhs-pc-web; unread={%22ub%22:%2268d3cddb00000000070289e1%22%2C%22ue%22:%2268b65520000000001d027a7e%22%2C%22uc%22:31}; web_session=0400698efe0fc8579efecd53ef3a4bd2bc26a6; acw_tc=0a4ad0d317591267323444681e6d135ce8b2b3c82235c85d47568c9e9b1971; loadts=1759127019458',
            'Origin': 'https://www.xiaohongshu.com',
            'Pragma': 'no-cache',
            'Priority': 'u=1, i',
            'Referer': 'https://www.xiaohongshu.com/',
            'Sec-Ch-Ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            'X-B3-Traceid': generate_trace_id(),
            'X-S': 'XYS_2UQhPsHCH0c1Pjh9HjIj2erjwjQhyoPTqBPT49pjHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQTJdPIP/ZlgMrU4SmH4B4aqbq7arzQygSGwn4jyDkizLGlyL8j4gY12o8OzF+9N9kPPokSJBQ7JbmMGMzHyd8izrD94rl6ySS7GFSDJnh3+sRxLFIl+Lz1npYknLQfLez+G9+Q/nlcLbrlne892BSopomQtFYYqokkySmG4Mi7zrMz4rzgJM+tyrSzzDkO40bgz9phnfpCtApBLDYk/dzzzBYg8/Y84fhAPAQILBShzBk8HjIj2ecjwjHjKc==',
            'X-S-Common': '2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1Pjh9HjIj2eHjwjQgynEDJ74AHjIj2ePjwjQhyoPTqBPT49pjHjIj2ecjwjHFN0WlN0ZjNsQh+aHCH0rEweWU80chGf+YqeZFqgrlq0q7+7ZE894Y8dQDqdQ620zYGfR02n46+/ZIPeZAPADMP/qjNsQh+jHCHjHVHdW7H0ijHjIj2eWjwjQQPAYUaBzdq9k6qB4Q4fpA8b878FSet9RQzLlTcSiM8/+n4MYP8F8LagY/P9Ql4FpUzfpS2BcI8nT1GFbC/L88JdbFyrSiafpr8DMra7pFLDDAa7+8J7QgabmFz7Qjp0mcwp4fanD68p40+fp8qgzELLbILrDA+9p3JpH9LLI3+LSk+d+DJfpSL98lnLYl49IUqgcMcDbrcDShtMmozBD6qM8FyFSh8o+h4g4U+obFyLSi4nbQz/+SPFlnPrDApSzQcA4SPopFJeQmzBMA/o8Szb+NqM+c4ApQzg8Ayp8FaDRl4AYs4g4fLomD8pzBpFRQ2ezLanSM+Skc47Qc4gcMag8VGLlj87PAqgzhagYSqAbn4FYQy7pTanTQ2npx87+8NM4L89L78p+l4BL6ze4AzB+IygmS8Bp8qDzFaLP98Lzn4AQQzLEAL7bFJBEVL7pwyS8Fag868nTl4e+0n04ApfuF8FSbL7SQyrpoL/Sl4LShyBEl20YdanTQ8fRl49TQc7bga0qAq9zV/9pnLoqAag8m8/mf89pD8SHFanDMqA+QG0ZU4gzmanSNq9SD4fp3nDESpbmF+BEm/9pgLo4bag83wBPI+fpfqg4bqBG6qM+c4MmQPFMUagYb+LlM474Yqgq3qfpkGdbU/9p8yf4Ay7bF8FS38Bp88AmS2b4j2rSi87+f/npA+fk0JrS3cnLALozLanSU8bbl4Fih4gcEa/P98/+c4b8QyLESPpmFJrSk+npfpd4maopF/L4l47YQPMbpaL+b8LkYLrboJsRAygbF/LSipdzQynpSngp7J9pgG9+Iy08Azo+34LS3z7So4g4jJpm74DSe8Bp3pdzpLgmD8nSIadPl/rpPanY9qA8APBp3pd4QqM8FaFShP7PILoz/aLLAqAZEt9QQP9RAnLrAqAmc49kQ408SpobFpo4YzFTQyLp7a/P98/bCJ7+3PrEAySSt8nkM4FRQ4fMMaL+ypf4n47kQPFzLagWM8pzyGS8YwaRSLMm7cDSkyr4PLo4G4BR68p+C+g+/Loz7GM8FJFShzAbQzaTFJgpF+rRM4FkQP9zAyS4m8/bM49MIqgzoanYzwoQc4M4Q2b4e49+Q8DDA4d+nqfpAp9RD8/bM4F8zqgzUanSk/rDAP7P94gzbaLpO8p+/qB8QcM+wqFSIqrS9Gd4Q4DbSy7p7Pn4n4Bpy4gzma/+ozLSi+d+hGnRSyS87nf4jn0+QcFYVwobFq9QM4rEQ2rDla/+LwLSkLnYQyBzSprFMq9iEy9lcLo4Pag8yafbM49bUqg4gaLpN8/86qD4QyMk+/rI78n8n4bQH4gzTJMkPasRfpAFjNsQhwaHC+APA+eZ9+/PVHdWlPsHCPsIj2erlH0ijJfRUJnbVHdF=',
            'X-T': get_current_timestamp(),
            'X-Xray-Traceid': generate_xray_trace_id(),
        }
        url = f"https://edith.xiaohongshu.com/api/sns/web/v1/user_posted?num=30&cursor={cursor}&user_id={user_id}&image_formats=jpg,webp,avif&xsec_token={xsec_token}&xsec_source=pc_search"
        
        # 使用session发送请求，保持连接
        response = requests.get(url, headers=headers, verify=False)

        data = response.json()
        print(f"API响应数据: {data}")
        exit()
        
        if response.status_code != 200:
            print(f"HTTP请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return {}
        
        try:
            data = response.json()
            print(f"API响应数据: {data}")
        except Exception as json_error:
            print(f"解析JSON失败: {str(json_error)}")
            print(f"原始响应内容: {response.text}")
            return {}
        
        # 检查响应结构
        if data.get("msg") == '成功':
            result = data.get("data", {})
            return result
        else:
            print(f"获取博主{user_id}笔记信息失败: {data.get('msg', '未知错误')}")
            print(f"完整响应: {data}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}笔记信息出错: {str(e)}")
        return {}

def main():
    users = session.query(PgyUser).all()
    print(f"开始处理 {len(users)} 个博主的数据")
    
    for index, user in enumerate(users, 1):
        print(f"\n正在处理第 {index}/{len(users)} 个博主: {user.userId}")
        cursor_id = ''  # 第一页cursor为空
        page_count = 0
        max_pages = 10  # 设置最大页数限制，防止无限循环
        
        while page_count < max_pages:
            page_count += 1
            print(f"  抓取第 {page_count} 页数据...")
            notes = get_notes_detail(user.userId, user.xsec_token, cursor_id)
            print(notes)
            time.sleep(12)
            
            if not notes or 'notes' not in notes:
                print(f"  博主 {user.userId} 没有更多笔记数据")
                break
            
            notes_list = notes.get('notes', [])
            if not notes_list:
                print(f"  博主 {user.userId} 第 {page_count} 页没有笔记数据")
                break
            
            # 检查是否遇到重复数据
            has_new_data = False
            found_duplicate = False
            
            for note in notes_list:
                note_id = note.get('note_id')
                if not note_id:
                    continue
                
                # 检查该note_id是否已存在于数据库
                existing_note = session.query(PgyNoteDetail).filter_by(note_id=note_id).first()
                if existing_note:
                    print(f"  发现重复数据 note_id: {note_id}，停止抓取该博主后续数据")
                    found_duplicate = True
                    break
                
                # 创建新记录
                data = PgyNoteDetail(
                    user_id=note.get('user_id', ''),
                    display_title=note.get('display_title', ''),
                    likeNum=note.get('interact_info', {}).get('liked_count', 0),
                    note_id=note_id,
                    xsec_token=note.get('xsec_token', ''),
                )
                session.add(data)
                has_new_data = True
            
            # 如果发现重复数据，跳出循环
            if found_duplicate:
                break
            
            if not has_new_data:
                print(f"  博主 {user.userId} 第 {page_count} 页没有新数据")
                break
            
            # 提交当前页的数据
            try:
                session.commit()
                print(f"  第 {page_count} 页数据已保存，共 {len(notes_list)} 条记录")
            except Exception as e:
                print(f"  保存第 {page_count} 页数据时出错: {str(e)}")
                session.rollback()
                continue
            
            # 获取下一页的cursor
            next_cursor = notes.get('cursor', '')
            
            # 如果下一页cursor为空或None，说明已经到最后一页
            if not next_cursor or next_cursor == '':
                print(f"  博主 {user.userId} 所有数据已抓取完成")
                break
            
            # 更新cursor为下一页的cursor
            cursor_id = next_cursor
            
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
        
        if page_count >= max_pages:
            print(f"  博主 {user.userId} 已达到最大页数限制 ({max_pages})，停止抓取")
        
        print(f"博主 {user.userId} 处理完成")
    
    print("\n所有博主数据处理完成！")

if __name__ == "__main__":
    main()