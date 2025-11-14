"""
获取蒲公英邀约数据
"""

import requests
import urllib3
import time
import sys
import os
import configparser
import schedule
from datetime import datetime
from loguru import logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 固定配置
# API_BASE_URL = 'http://localhost:5666'
API_BASE_URL = 'https://tianji.fangpian999.com'
PGY_API_URL = 'https://pgy.xiaohongshu.com/api/solar/invite/get_invites_overview'
REQUEST_DELAY = 10  # 每次请求延迟（秒）
REQUEST_TIMEOUT = 30  # 请求超时时间（秒）
MAX_DAYS = 7  # Token最大有效天数


def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    
    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('pgy_invites_config.ini'),
        'pgy_invites_config.ini',
    ]
    
    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"已加载配置文件: {config_path}")
            break
    
    if not config_loaded:
        logger.error("未找到配置文件 pgy_invites_config.ini")
        raise FileNotFoundError("配置文件不存在")
    
    # 只解析调度器配置
    return {
        'enable_scheduler': config.getboolean('SCHEDULER', 'enable_scheduler'),
        'daily_time': config.get('SCHEDULER', 'daily_time'),
        'run_once': config.getboolean('SCHEDULER', 'run_once'),
        'check_interval': config.getint('SCHEDULER', 'check_interval'),
    }


def setup_logger():
    """设置日志配置"""
    # 设置日志目录
    if hasattr(sys, '_MEIPASS'):
        exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        log_path = os.path.join(exe_dir, 'logs')
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(current_dir, 'logs')
    
    os.makedirs(log_path, exist_ok=True)
    
    logger.remove()
    
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    logger.add(
        os.path.join(log_path, "pgy_invites_{time:YYYY-MM-DD}.log"),
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG",
        encoding="utf-8"
    )
    
    logger.info(f"日志文件保存路径: {log_path}")


def check_token_time(update_time):
    """检查token时间是否超过指定天数"""
    try:
        if isinstance(update_time, str):
            try:
                token_time = datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    token_time = datetime.strptime(update_time, '%Y-%m-%d')
                except ValueError:
                    return False, 0
        else:
            token_time = datetime.fromtimestamp(update_time)
        
        time_diff = datetime.now() - token_time
        days_diff = time_diff.days
        is_expired = days_diff > MAX_DAYS
        
        return is_expired, days_diff
    except Exception as e:
        logger.warning(f"时间检查出错: {str(e)}")
        return False, 0


def get_token_list():
    """获取token列表"""
    try:
        headers = {"Content-Type": "application/json"}
        api_url = f"{API_BASE_URL}/api/admin/spider/token/pgy/list"
        
        response = requests.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        if response.status_code != 200:
            logger.error(f"获取token失败，状态码: {response.status_code}")
            return None
        
        response_json = response.json()
        if 'data' not in response_json:
            logger.error(f"响应数据格式错误")
            return None
        
        return response_json['data']
    except Exception as e:
        logger.error(f"获取token列表出错: {str(e)}")
        return None


def check_invite_detail(invite_id):
    """
    检查指定inviteId是否已存在于数据库

    Args:
        invite_id: 邀约ID

    Returns:
        bool: True表示已存在（应停止分页），False表示不存在（继续分页）
    """
    try:
        headers = {"Content-Type": "application/json"}
        api_url = f"{API_BASE_URL}/api/admin/pgyInvites/getPgyInvitesDetail"

        params = {'invite_id': invite_id}

        response = requests.get(
            api_url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code != 200:
            logger.warning(f"检查inviteId {invite_id} 失败，状态码: {response.status_code}")
            return False

        result = response.json()

        # 如果返回有数据，说明已存在
        if result.get('data'):
            logger.info(f"✅ inviteId {invite_id} 已存在数据库，停止分页")
            return True
        else:
            logger.debug(f"inviteId {invite_id} 不存在，继续分页")
            return False

    except Exception as e:
        logger.warning(f"检查inviteId {invite_id} 出错: {str(e)}")
        return False


def get_invites_data(token_content, platform_user_id):
    """
    获取邀约数据（支持多页）

    Args:
        token_content: 蒲公英token
        platform_user_id: 用户ID

    Returns:
        list: 所有邀约数据列表，失败返回None
    """
    try:
        pgy_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            "cookie": token_content,
            "Content-Type": "application/json"
        }

        all_invites = []  # 存储所有邀约数据
        page_num = 1
        max_pages = 100  # 最大分页数，防止无限循环

        while page_num <= max_pages:
            logger.info(f"📄 请求第 {page_num} 页数据...")

            page_data = {
                "pageNum": page_num,
                "pageSize": 20,  # 改为每页20条
                "inviteStatus": "-1",
                "kolIntention": "-1",
                "kolType": 0,
                "searchDateType": 1,
                "showWechat": 0
            }

            # 延迟请求（第一页在外部已延迟）
            if page_num > 1:
                time.sleep(REQUEST_DELAY)

            response = requests.post(
                PGY_API_URL,
                headers=pgy_headers,
                json=page_data,
                verify=False,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                logger.error(f"请求第 {page_num} 页失败，状态码: {response.status_code}")
                break

            pgy_data = response.json().get('data', {})

            if 'invites' not in pgy_data:
                logger.warning(f"第 {page_num} 页无invites数据")
                break

            current_invites = pgy_data['invites']

            if not current_invites or len(current_invites) == 0:
                logger.info(f"第 {page_num} 页无数据，停止分页")
                break

            logger.success(f"✅ 第 {page_num} 页获取到 {len(current_invites)} 条数据")

            # 取最后一条数据的inviteId
            last_invite = current_invites[-1]
            last_invite_id = last_invite.get('inviteId')

            if not last_invite_id:
                logger.warning(f"第 {page_num} 页最后一条数据无inviteId")
                all_invites.extend(current_invites)
                break

            # 检查最后一条数据是否已存在
            logger.debug(f"检查最后一条数据 inviteId: {last_invite_id}")

            if check_invite_detail(last_invite_id):
                # 已存在，说明到达已有数据，停止分页
                logger.info(f"🛑 第 {page_num} 页已到达已有数据，停止分页")
                all_invites.extend(current_invites)
                break
            else:
                # 不存在，继续下一页
                all_invites.extend(current_invites)
                page_num += 1

        if page_num > max_pages:
            logger.warning(f"⚠️ 达到最大分页数 {max_pages}，停止请求")

        logger.info(f"📊 总共获取 {len(all_invites)} 条邀约数据（{page_num} 页）")

        return all_invites if all_invites else None

    except Exception as e:
        logger.error(f"获取邀约数据出错: {str(e)}")
        return None


def insert_invites_batch(invites, platform_user_id):
    """批量插入邀约数据"""
    try:
        headers = {"Content-Type": "application/json"}
        api_url = f"{API_BASE_URL}/api/admin/pgyInvites/pgyInvitesBatchInsert"
        
        insert_data = {
            'invites': invites,
            'pgy_user_id': platform_user_id,
            'check_interval': 'false'
        }
        
        response = requests.post(
            api_url,
            headers=headers,
            json=insert_data,
            timeout=REQUEST_TIMEOUT
        )
        print(response.json())
        
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 200 or result.get('success'):
                return True
        return False
    except Exception as e:
        logger.error(f"插入数据出错: {str(e)}")
        return False


def run_spider_task():
    """执行爬虫任务"""
    try:
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("🚀 蒲公英邀约数据同步程序启动")
        logger.info(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        # 获取token列表
        logger.info("📋 开始获取token列表...")
        tokens = get_token_list()
        
        if not tokens:
            logger.error("❌ 未能获取到token列表")
            return False
        
        logger.success(f"✅ 成功获取 {len(tokens)} 个token")
        
        # 处理每个token
        skipped_count = 0
        success_count = 0
        fail_count = 0
        
        for idx, token in enumerate(tokens, 1):
            try:
                platform_user_id = token.get('platform_user_id')
                # if platform_user_id != '62b43929000000001b0268e5':
                #     continue
                token_content = token.get('token_content')
                update_time = token.get('update_time')
                
                # 验证必填字段
                if not platform_user_id or not token_content or not update_time:
                    skipped_count += 1
                    continue
                
                logger.info(f"\n[{idx}/{len(tokens)}] 处理用户: {platform_user_id}")
                
                # 检查token时间
                is_expired, days_diff = check_token_time(update_time)
                if is_expired:
                    skipped_count += 1
                    continue
                
                # 获取邀约数据
                logger.info(f"📥 获取邀约数据(等待{REQUEST_DELAY}秒)...")
                time.sleep(REQUEST_DELAY)  # 第一页延迟
                invites = get_invites_data(token_content, platform_user_id)
                
                if invites is None:
                    logger.error("❌ 获取邀约数据失败")
                    fail_count += 1
                    continue
                
                if len(invites) == 0:
                    logger.warning("⚠️ 该用户没有邀约数据")
                    continue
                
                logger.success(f"✅ 获取到 {len(invites)} 条邀约数据")
                
                # 批量插入数据
                logger.info("💾 开始插入数据...")
                if insert_invites_batch(invites, platform_user_id):
                    logger.success(f"✅ 插入成功 (共{len(invites)}条)")
                    success_count += 1
                else:
                    logger.error("❌ 插入失败")
                    fail_count += 1
                    
            except Exception as e:
                logger.error(f"❌ 处理token时出错: {str(e)}")
                fail_count += 1
                continue
        
        # 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有数据处理完成")
        logger.info("📊 执行统计:")
        logger.info(f"   ⏱️  执行时长: {duration:.2f} 秒")
        logger.info(f"   📝 总token数: {len(tokens)}")
        logger.info(f"   ✅ 成功处理: {success_count}")
        logger.info(f"   ❌ 处理失败: {fail_count}")
        logger.info(f"   ⏭️  跳过记录: {skipped_count}")
        logger.info(f"   🏁 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 程序运行出错: {str(e)}")
        return False


def main():
    """主函数"""
    try:
        # 设置日志
        setup_logger()
        
        # 加载配置
        scheduler_config = load_config()
        
        logger.info("=" * 60)
        logger.info("🚀 蒲公英邀约数据同步程序")
        logger.info(f"📅 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        # 单次运行模式
        if scheduler_config['run_once']:
            logger.info("⚡ 单次运行模式")
            success = run_spider_task()
            return success
        
        # 调度器模式
        elif scheduler_config['enable_scheduler']:
            logger.info("🔄 调度器模式")
            logger.info(f"⏰ 执行时间: 每天 {scheduler_config['daily_time']}")
            logger.info(f"🔍 检查间隔: {scheduler_config['check_interval']}秒")
            logger.info("=" * 60)
            
            # 设置定时任务
            schedule.every().day.at(scheduler_config['daily_time']).do(run_spider_task)
            logger.info(f"✅ 已设置定时任务: 每天 {scheduler_config['daily_time']}")
            logger.info("\n🔄 调度器运行中，按 Ctrl+C 停止...\n")
            
            # 运行调度器
            while True:
                schedule.run_pending()
                time.sleep(scheduler_config['check_interval'])
        
        # 调度器未启用
        else:
            logger.info("⚡ 调度器未启用，执行单次任务")
            success = run_spider_task()
            return success
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户手动中断程序")
        return True
    except Exception as e:
        logger.error(f"❌ 程序启动失败: {str(e)}")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
