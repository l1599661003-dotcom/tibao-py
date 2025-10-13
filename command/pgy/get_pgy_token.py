import json
import os
import time
import subprocess
import sys
import traceback
import uuid

import certifi
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 检查并安装必要的包
def install_requirements():
    required_packages = ['selenium', 'webdriver-manager', 'requests', 'certifi']
    
    # 使用清华源
    mirror_url = "https://pypi.tuna.tsinghua.edu.cn/simple"
    # 或者使用阿里源
    # mirror_url = "https://mirrors.aliyun.com/pypi/simple"
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            print(f"正在从{mirror_url}安装{package}...")
            subprocess.check_call([
                sys.executable, 
                "-m", 
                "pip", 
                "install", 
                package,
                "-i", 
                mirror_url,
                "--trusted-host", 
                mirror_url.split('/')[2]  # 获取域名部分
            ])

try:
    install_requirements()
    from selenium import webdriver
    from selenium.webdriver.edge.options import Options
    from selenium.webdriver.edge.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
except Exception as e:
    print(f"安装或导入包时出错: {str(e)}")
    print("详细错误信息:")
    print(traceback.format_exc())
    sys.exit(1)

def get_info(token):
    url = "https://pgy.xiaohongshu.com/api/solar/user/info"
    trace_id = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': token,
        'X-B3-Traceid': trace_id[:16]
    }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        return None
    return response.json()['data']

def get_cookies_from_url(url):
    """使用已登录的Edge浏览器打开URL并获取cookies"""
    try:
        # 清除所有代理环境变量
        for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
            if var in os.environ:
                del os.environ[var]
        
        # 设置无代理环境变量
        os.environ['no_proxy'] = '*'
        
        # 安装并获取Edge驱动
        print("正在下载Edge驱动...")
        driver_path = EdgeChromiumDriverManager().install()
        print(f"驱动已下载到: {driver_path}")
        
        # 动态获取当前用户的配置文件路径
        username = os.getenv('USERNAME') or os.getenv('USER')
        user_data_dir = fr"C:\Users\{username}\AppData\Local\Microsoft\Edge\User Data"
        
        if not os.path.exists(user_data_dir):
            print(f"Edge配置文件路径不存在: {user_data_dir}")
            user_data_dir = os.path.expanduser('~/.edge-automation')  # 备用路径
            os.makedirs(user_data_dir, exist_ok=True)
        
        # 确保Edge进程完全关闭
        try:
            subprocess.run(["taskkill", "/f", "/im", "msedge.exe"], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
            time.sleep(2)  # 给进程更多关闭时间
        except Exception as e:
            print(f"关闭Edge进程时出错: {e}")
        
        options = Options()
        options.add_argument(f"user-data-dir={user_data_dir}")
        
        # 尝试不同的配置文件目录
        profile_dirs = ["Default", "Profile 1", "Profile 2", "Profile 3"]
        
        for profile_dir in profile_dirs:
            try:
                print(f"尝试使用配置文件: {profile_dir}")
                
                # 设置配置文件
                options_copy = Options()
                options_copy.add_argument(f"user-data-dir={user_data_dir}")
                options_copy.add_argument(f"profile-directory={profile_dir}")
                
                # 禁用代理
                options_copy.add_argument("--no-proxy-server")
                options_copy.add_argument("--disable-extensions")
                options_copy.add_argument("--disable-gpu")
                options_copy.add_argument("--no-sandbox")
                
                # 创建服务对象
                service = Service(driver_path)
                
                # 创建浏览器实例
                print("正在启动Edge浏览器...")
                driver = webdriver.Edge(service=service, options=options_copy)
                
                # 访问URL
                print(f"正在访问: {url}")
                driver.get(url)
                time.sleep(3)  # 等待页面加载
                
                # 获取cookies
                cookies = driver.get_cookies()
                
                # 格式化并打印cookies字符串
                cookie_string = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
                
                # 关闭浏览器
                driver.quit()
                
                return cookie_string
                
            except Exception as e:
                print(f"使用配置文件 {profile_dir} 失败: {e}")
                continue
        
        print("所有配置文件都尝试失败")
        return None
        
    except Exception as e:
        print(f"获取cookies失败: {e}")
        return None
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {
        'app_id': 'cli_a6e824d4363b500d',
        'app_secret': 'nW4ff1Mviwr0ZuYkF1BBhciZGOyDeBP5'
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        response_data = response.json()
        return response_data.get('app_access_token', '')
    except requests.RequestException as e:
        print(f"获取飞书Token失败: {e}")
        return None
def search_feishu_record(app_token, table_id, view_id, field_name, field_value):
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
        print(f"查询飞书单条信息失败: {e}")
        return None
def update_record(app_token, table_id, record_id, fields):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    body = {
        'fields': fields
    }
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    try:
        response = requests.put(url, headers=headers, json=body, verify=certifi.where(), timeout=30)
        response_data = response.json()
        if response_data.get('msg') == 'success':
            print(f'飞书表格修改成功: {fields}')
        else:
            print(response_data)
    except requests.RequestException as e:
        print(f"更新飞书修改失败: {e}")
# 新增单条飞书记录
def insert_record(app_token, table_id, fields):
    response = requests.post(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers={
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {get_feishu_token()}'
        },
        data=json.dumps({'fields': fields}),
        verify=certifi.where()
    )
    print(f"飞书表格新增成功{response.json()}")
# 邀约蒲公英账号表
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tblDO9VqC6EMHGiY'
view_id = 'vewJK6XVP4'
def insert_feishu():
    url = "https://pgy.xiaohongshu.com/solar/pre-trade/home"
    # 获取cookies
    token = get_cookies_from_url(url)
    info = get_info(token)
    nickname = info["nickName"]
    userid = info["userId"]
    results = search_feishu_record(app_token, table_id, view_id, '账号id', userid)
    if results:
        record_id = results[0]['record_id']
        fields = {
            '蒲公英token': token,
        }
        update_record(app_token, table_id, record_id, fields)
    else:
        fields = {
            '账号简称': nickname,
            '账号id': userid,
            '蒲公英token': token,
        }
        insert_record(app_token, table_id, fields)
if __name__ == "__main__":
    insert_feishu()