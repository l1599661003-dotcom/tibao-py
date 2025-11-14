# -*- coding: utf-8 -*-
"""
抖音MCN数据抓取程序
从星图API抓取MCN机构数据并存入数据库
"""

import json
import time
import requests
import urllib3
from datetime import datetime
from loguru import logger
import sys

from core.localhost_fp_project import session
from models.models import DouyinMcn
from unitl.unitl import random_sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    "cookie": 'passport_csrf_token=6849f54910d4b4c2979f4d8dabb93a6b; passport_csrf_token_default=6849f54910d4b4c2979f4d8dabb93a6b; is_staff_user=false; tt_webid=7552842709368161835; s_v_web_id=verify_mfv85tdl_F8gFQvfU_KTrW_4Yrj_9hxq_JS9ee8c9uWDH; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; star_sessionid=7d7d193e69c1086cbd1bd917a1efc674; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1758533208,1758851673,1760087389; HMACCOUNT=A9193F3F989E70E1; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760087394; passport_auth_status=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; passport_auth_status_ss=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; sid_guard=7b47b5c1e4302bc5075e27a3aa376cb0%7C1760087460%7C5184002%7CTue%2C+09-Dec-2025+09%3A11%3A02+GMT; uid_tt=ed7cb7326eed763cc0293bffa66419a2; uid_tt_ss=ed7cb7326eed763cc0293bffa66419a2; sid_tt=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid_ss=7b47b5c1e4302bc5075e27a3aa376cb0; session_tlb_tag=sttt%7C10%7Ce0e1weQwK8UHXiejqjdssP________-noV-qfG8S9nB4yyNwTYJ5h77Qhsn_HqhFuQgzccYHWgQ%3D; sid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; ssid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; possess_scene_star_id=1844017439585284',
    "Content-Type": "application/json"
}

API_URL = "https://www.xingtu.cn/gw/api/mcn/demander_mcn_list"
PAGE_SIZE = 30  # 每页数量
REQUEST_DELAY = 10  # 每次请求延迟（秒）


def setup_logger():
    """设置日志"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )


def save_mcn_to_db(mcn_data):
    """保存MCN数据到数据库，使用user_id查重"""
    try:
        user_id = mcn_data.get('user_id')
        
        # 查询是否已存在
        existing_mcn = session.query(DouyinMcn).filter(
            DouyinMcn.user_id == user_id
        ).first()
        
        # 拼接标签
        mcn_tags = '、'.join(mcn_data.get('mcn_tags', [])) if mcn_data.get('mcn_tags') else ''
        
        current_time = datetime.now()
        
        if existing_mcn:
            # 更新现有记录
            existing_mcn.author_num = mcn_data.get('author_num')
            existing_mcn.mcn_tags = mcn_tags
            existing_mcn.avatar_uri = mcn_data.get('avatar_uri')
            existing_mcn.complex_score = mcn_data.get('complex_score')
            existing_mcn.growth_score = mcn_data.get('growth_score')
            existing_mcn.introduction = mcn_data.get('introduction')
            existing_mcn.sum_follower = mcn_data.get('sum_follower')
            existing_mcn.name = mcn_data.get('name')
            existing_mcn.update_time = current_time
            
            logger.debug(f"更新MCN: {mcn_data.get('name')} (user_id: {user_id})")
            return 'updated'
        else:
            # 创建新记录
            new_mcn = DouyinMcn(
                author_num=mcn_data.get('author_num'),
                mcn_tags=mcn_tags,
                avatar_uri=mcn_data.get('avatar_uri'),
                complex_score=mcn_data.get('complex_score'),
                growth_score=mcn_data.get('growth_score'),
                introduction=mcn_data.get('introduction'),
                name=mcn_data.get('name'),
                user_id=user_id,
                sum_follower=mcn_data.get('sum_follower'),
                create_time=current_time,
                update_time=current_time,
                status=0
            )
            
            session.add(new_mcn)
            logger.debug(f"新增MCN: {mcn_data.get('name')} (user_id: {user_id})")
            return 'inserted'
            
    except Exception as e:
        logger.error(f"保存MCN数据时出错: {str(e)}")
        # 回滚当前事务
        session.rollback()
        return 'error'


def fetch_mcn_data(page):
    """获取指定页的MCN数据"""
    try:
        url = f"{API_URL}?page={page}&limit={PAGE_SIZE}&order_by=platform_scores"
        
        response = requests.get(
            url,
            headers=HEADERS,
            verify=False,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"请求失败，状态码: {response.status_code}")
            return None
        
        return response.json()
        
    except requests.RequestException as e:
        logger.error(f"请求MCN数据失败: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"获取MCN数据出错: {str(e)}")
        return None


def main():
    """主函数"""
    setup_logger()
    
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info("🚀 抖音MCN数据抓取程序启动")
    logger.info(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    try:
        page = 1
        total_mcns = 0
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        while True:
            logger.info(f"\n📄 正在处理第 {page} 页...")
            
            # 获取数据
            response_data = fetch_mcn_data(page)
            
            if not response_data:
                logger.error("获取数据失败，停止抓取")
                break
            
            # 检查响应格式
            if 'base_resp' not in response_data or response_data['base_resp'].get('status_code') != 0:
                logger.error(f"API响应异常: {response_data}")
                break
            
            # 获取MCN列表
            mcns = response_data.get('mcns', [])
            pagination = response_data.get('pagination', {})
            
            logger.info(f"📊 本页获取到 {len(mcns)} 条MCN数据")
            
            if len(mcns) == 0:
                logger.info("本页没有数据，抓取完成")
                break
            
            # 处理每条MCN数据
            for mcn in mcns:
                result = save_mcn_to_db(mcn)
                
                if result == 'inserted':
                    inserted_count += 1
                elif result == 'updated':
                    updated_count += 1
                elif result == 'error':
                    error_count += 1
                
                total_mcns += 1
            
            # 提交数据库事务
            try:
                session.commit()
                logger.success(f"✅ 第 {page} 页数据保存成功")
            except Exception as commit_error:
                session.rollback()
                logger.error(f"❌ 第 {page} 页数据保存失败: {str(commit_error)}")
            
            # 检查是否还有更多页
            has_more = pagination.get('has_more', False)
            total_count = pagination.get('total_count', 0)
            
            logger.info(f"📈 总MCN数: {total_count}, 已处理: {total_mcns}")
            
            if not has_more:
                logger.info("✅ 已到达最后一页，抓取完成")
                break
            
            # 翻页
            page += 1
            
            # 添加延迟，避免请求过快
            logger.info(f"⏳ 等待 {REQUEST_DELAY} 秒后继续...")
            random_sleep(REQUEST_DELAY, 15)
        
        # 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ 数据抓取完成")
        logger.info("📊 执行统计:")
        logger.info(f"   ⏱️  执行时长: {duration:.2f} 秒")
        logger.info(f"   📄 总页数: {page}")
        logger.info(f"   📝 总MCN数: {total_mcns}")
        logger.info(f"   ➕ 新增记录: {inserted_count}")
        logger.info(f"   🔄 更新记录: {updated_count}")
        logger.info(f"   ❌ 错误记录: {error_count}")
        logger.info(f"   🏁 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        return True
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户手动中断程序")
        session.rollback()
        return False
    except Exception as e:
        logger.error(f"❌ 程序运行出错: {str(e)}")
        session.rollback()
        import traceback
        logger.debug(traceback.format_exc())
        return False
    finally:
        # 关闭数据库连接
        session.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
