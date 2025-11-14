# -*- coding: utf-8 -*-
"""
抖音MCN详情数据抓取程序（简化版）
从数据库获取MCN列表，抓取每个MCN的博主数据并推送到接口
"""

import os
import time
import json
from datetime import datetime

from requests import RequestException

from core.localhost_fp_project import session
from models.models import DouyinMcn

import requests
import urllib3
from loguru import logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
BASE_URL = "https://www.xingtu.cn/gw/api/mcn/mcn_main_page_author_list"
SAVE_DATA_URL = "https://tianji.fangpian999.com/api/admin/creator/CreatorOut/saveData"
# SAVE_DATA_URL = "http://localhost:5666/api/admin/creator/CreatorOut/saveData"
PLATFORM_ID = 2  # 抖音平台ID
PAGE_SIZE = 150
REQUEST_DELAY = 10  # 请求延迟(秒)

# Cookie配置（直接使用固定Cookie）
COOKIE = "passport_csrf_token=6849f54910d4b4c2979f4d8dabb93a6b; passport_csrf_token_default=6849f54910d4b4c2979f4d8dabb93a6b; is_staff_user=false; tt_webid=7552842709368161835; s_v_web_id=verify_mfv85tdl_F8gFQvfU_KTrW_4Yrj_9hxq_JS9ee8c9uWDH; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; star_sessionid=7d7d193e69c1086cbd1bd917a1efc674; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1758533208,1758851673,1760087389; HMACCOUNT=A9193F3F989E70E1; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760087394; passport_auth_status=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; passport_auth_status_ss=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; sid_guard=7b47b5c1e4302bc5075e27a3aa376cb0%7C1760087460%7C5184002%7CTue%2C+09-Dec-2025+09%3A11%3A02+GMT; uid_tt=ed7cb7326eed763cc0293bffa66419a2; uid_tt_ss=ed7cb7326eed763cc0293bffa66419a2; sid_tt=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid_ss=7b47b5c1e4302bc5075e27a3aa376cb0; session_tlb_tag=sttt%7C10%7Ce0e1weQwK8UHXiejqjdssP________-noV-qfG8S9nB4yyNwTYJ5h77Qhsn_HqhFuQgzccYHWgQ%3D; sid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; ssid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; possess_scene_star_id=1844017439585284"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Content-Type': 'application/json',
    'Cookie': COOKIE
}


def save_creator_data(data_to_save):
    """保存创作者数据到后端接口"""
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(SAVE_DATA_URL, headers=headers, json=data_to_save, timeout=3000, verify=False)
        if response.status_code == 200:
            logger.info(f"数据保存成功: creator_mcn={data_to_save.get('creator_mcn')}, 共 {len(data_to_save.get('raw_data', []))} 条数据")
            return True
        else:
            logger.error(f"数据保存失败: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"保存数据时出错: {str(e)}")
        return False


def extract_creator_info(author_data):
    """提取创作者关键信息，按照fp_creator_business_out表结构"""
    tags = '、'.join(author_data.get('tags', [])) if author_data.get('tags') else ''

    return {
        "userId": str(author_data.get('author_id', '')),  # 平台唯一user_id
        "name": author_data.get('nick_name', ''),  # 昵称
        "gender": author_data.get('gender', ''),  # 性别
        "location": author_data.get('location', ''),  # 区域
        "fansCount": author_data.get('sum_follower', 0),  # 粉丝数
        "likeCollectCountInfo": author_data.get('like_collect_count', 0),  # 赞藏数
        "picturePrice": author_data.get('picture_price', 0),  # 图文价格
        "videoPrice": author_data.get('video_price', 0),  # 视频价格
        # "content_tags": tags,  # 内容标签
        "contentTags": [],  # 内容标签
    }


def fetch_mcn_authors(mcn_id):
    """获取指定MCN的所有博主数据"""
    logger.info(f"\n开始抓取MCN: {mcn_id}")
    all_authors = []
    page = 1
    max_pages = 50
    consecutive_empty_pages = 0
    MAX_RETRY = 3  # 最大重试次数
    RETRY_DELAY = 6

    while page <= max_pages:
        try:
            url = f"{BASE_URL}?page={page}&limit={PAGE_SIZE}&mcn_id={mcn_id}"
            logger.info(f"请求第 {page} 页: {url}")
            response = ''

            success = False
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    response = requests.get(url, headers=HEADERS, verify=False, timeout=30)

                    if response.status_code == 200:
                        success = True
                        break
                    else:
                        logger.warning(f"请求失败，状态码: {response.status_code}，第 {attempt}/{MAX_RETRY} 次重试中...")
                        time.sleep(RETRY_DELAY)

                except RequestException as e:
                    logger.warning(f"请求异常: {e}，第 {attempt}/{MAX_RETRY} 次重试中...")
                    time.sleep(RETRY_DELAY)

            if not success:
                logger.error(f"第 {page} 页请求失败，已重试 {MAX_RETRY} 次，跳过该页。")
                continue

            data = response.json()

            # 检查响应格式
            if 'base_resp' not in data or data['base_resp'].get('status_code') != 0:
                logger.error(f"API响应异常: {data}")
                break

            # 获取作者列表
            authors = data.get('top_follower_authors', [])
            pagination = data.get('pagination', {})

            logger.info(f"第 {page} 页获取到 {len(authors)} 条博主数据")
            time.sleep(RETRY_DELAY)

            if len(authors) == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    logger.info(f"连续{consecutive_empty_pages}页无数据，停止抓取")
                    break
            else:
                consecutive_empty_pages = 0
                # 提取博主信息
                for author in authors:
                    creator_info = extract_creator_info(author)
                    all_authors.append(creator_info)

            # 检查是否还有更多页
            has_more = pagination.get('has_more', False)
            if not has_more and len(authors) < PAGE_SIZE:
                logger.info(f"已到最后一页，停止抓取")
                break

            page += 1
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.error(f"抓取第 {page} 页时出错: {str(e)}")
            break

    logger.info(f"MCN {mcn_id} 抓取完成，共 {len(all_authors)} 个博主")
    return all_authors


def get_mcn_list():
    """获取MCN列表"""
    try:
        mcn_list = session.query(DouyinMcn).filter(DouyinMcn.status > 5).all()
        logger.info(f"获取到 {len(mcn_list)} 个MCN")
        return mcn_list
    except Exception as e:
        logger.error(f"获取MCN列表时出错: {str(e)}")
        return []


def main():
    """主函数"""
    logger.info("抖音MCN博主数据抓取程序启动")
    logger.info("=" * 70)

    try:
        # 获取MCN列表
        mcn_list = get_mcn_list()

        if not mcn_list:
            logger.warning("没有MCN数据")
            return

        # 收集所有博主数据
        all_creators = []
        total_success = 0
        total_failed = 0

        # 遍历每个MCN
        for mcn in mcn_list:
            try:
                mcn_id = mcn.user_id  # ORM对象，直接访问属性
                creator_mcn = mcn.status  # 使用MCN的id作为creator_mcn
                logger.info(f"\n开始处理MCN: {mcn_id} (creator_mcn={creator_mcn})")

                # 抓取该MCN的所有博主数据
                authors = fetch_mcn_authors(mcn_id)

                if authors and len(authors) > 0:
                    all_creators.extend(authors)
                    total_success += len(authors)
                    logger.info(f"MCN {mcn_id} 成功抓取 {len(authors)} 个博主")
                else:
                    logger.warning(f"MCN {mcn_id} 没有返回数据")
                    total_failed += 1

                # 组装数据并保存
                data_to_save = {
                    "creator_mcn": str(creator_mcn),
                    "platform_id": PLATFORM_ID,
                    "raw_data": all_creators
                }

                # 调用保存接口
                if len(all_creators) > 0:
                    logger.info(f"开始推送数据到后端...")
                    save_creator_data(data_to_save)
                else:
                    logger.warning(f"没有数据需要保存")

                # 延迟，避免请求过快
                time.sleep(6)

            except Exception as e:
                total_failed += 1
                logger.error(f"处理MCN时出错: {str(e)}")
                time.sleep(3)

        logger.info(f"\n数据收集完成:")
        logger.info(f"- 成功抓取 {total_success} 个博主")
        logger.info(f"- 失败 {total_failed} 个MCN")
        logger.info(f"- 总计 {len(all_creators)} 条博主数据")

        # 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("所有数据处理完成!")
        logger.info(f"执行时长: {duration:.2f} 秒")
        logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())


if __name__ == "__main__":
    main()
