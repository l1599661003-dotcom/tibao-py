# -*- coding: utf-8 -*-
'''
抖音星图达人挂靠数据抓取程序
从星图API抓取达人详细数据并导出到Excel
'''

import json
import re
import time
import requests
import urllib3
from datetime import datetime
from loguru import logger
import sys
import pandas as pd
from tkinter import Tk, filedialog
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    "cookie": 'tt_webid=7511610314087384602; is_staff_user=false; s_v_web_id=verify_mgpwe45p_geWSojSo_bpHy_4R1X_8gPm_5Sjmw0GQ6ASh; passport_csrf_token=2440411645279c3662caa19f03c9e0fc; passport_csrf_token_default=2440411645279c3662caa19f03c9e0fc; csrf_session_id=dc4e391bb86a33a5c467cb130448e521; star_sessionid=4ab6f35f16caaa63effd38bee8df9f44; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1761010634,1762242663,1762421008,1762503966; HMACCOUNT=95640B6539E8B81A; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1762503986; passport_auth_status=0e7e168f243bcbea8627b72a0dcd40c0%2C; passport_auth_status_ss=0e7e168f243bcbea8627b72a0dcd40c0%2C; sid_guard=282de951106f898635c4e3a8f1c751e3%7C1762504075%7C5184002%7CTue%2C+06-Jan-2026+08%3A27%3A57+GMT; uid_tt=c826a2f50038388d53a238ab3c8f46f5; uid_tt_ss=c826a2f50038388d53a238ab3c8f46f5; sid_tt=282de951106f898635c4e3a8f1c751e3; sessionid=282de951106f898635c4e3a8f1c751e3; sessionid_ss=282de951106f898635c4e3a8f1c751e3; session_tlb_tag=sttt%7C17%7CKC3pURBviYY1xOOo8cdR4__________1nmgKNlcPbMJu-y8exHnyfhM9loOVo9w4mpGv92uZ-yM%3D; session_tlb_tag_bk=sttt%7C17%7CKC3pURBviYY1xOOo8cdR4__________1nmgKNlcPbMJu-y8exHnyfhM9loOVo9w4mpGv92uZ-yM%3D; sid_ucp_v1=1.0.0-KGMzNzA1NTI1ZGNkMjUxZGZjYjA4NWJjN2IwYTM2MTgyMmExNGU1YWUKFgjZopCY0q1sEIvbtsgGGKYMOAFA6wcaAmxmIiAyODJkZTk1MTEwNmY4OTg2MzVjNGUzYThmMWM3NTFlMw; ssid_ucp_v1=1.0.0-KGMzNzA1NTI1ZGNkMjUxZGZjYjA4NWJjN2IwYTM2MTgyMmExNGU1YWUKFgjZopCY0q1sEIvbtsgGGKYMOAFA6wcaAmxmIiAyODJkZTk1MTEwNmY4OTg2MzVjNGUzYThmMWM3NTFlMw; possess_scene_star_id=1844031649796107',
    "Content-Type": "application/json"
}

REQUEST_DELAY = 6  # 每次请求延迟（秒）


def setup_logger():
    '''设置日志'''
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )


def select_excel_file():
    '''选择Excel文件'''
    logger.info("请选择包含星图链接的Excel文件...")
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)

    file_path = filedialog.askopenfilename(
        title='选择Excel文件',
        filetypes=[('Excel files', '*.xlsx *.xls'), ('All files', '*.*')]
    )

    root.destroy()

    if not file_path:
        logger.error("未选择文件")
        return None

    logger.info(f"已选择文件: {file_path}")
    return file_path


def extract_author_id(url):
    '''从星图链接中提取author_id'''
    if not url or pd.isna(url):
        return None

    # 匹配模式: douyin-video/数字
    pattern = r'douyin-video/(\d+)'
    match = re.search(pattern, str(url))

    if match:
        return match.group(1)

    return None


def call_api(url, author_id):
    '''调用API接口'''
    try:
        full_url = url.replace('7184805142959489082', author_id)

        response = requests.get(
            full_url,
            headers=HEADERS,
            verify=False,
            timeout=30
        )

        data = response.json()
        print(data)

        # 检查响应状态
        if 'base_resp' in data and data['base_resp'].get('status_code') != 0:
            logger.error(f"API返回错误: {data['base_resp'].get('status_message')}")
            return None

        return data

    except Exception as e:
        logger.error(f"API调用失败: {str(e)}")
        return None


def get_author_base_info(author_id):
    '''获取基本信息'''
    url = "https://www.xingtu.cn/gw/api/author/get_author_base_info?o_author_id=7184805142959489082&platform_source=1&platform_channel=1&recommend=true&need_sec_uid=true&need_linkage_info=true"
    return call_api(url, author_id)


def get_author_display(author_id):
    '''获取粉丝数赞藏'''
    url = "https://www.xingtu.cn/gw/api/data_sp/check_author_display?o_author_id=7184805142959489082&platform_source=1&platform_channel=1"
    return call_api(url, author_id)


def get_author_marketing_info(author_id):
    '''获取报价信息'''
    url = "https://www.xingtu.cn/gw/api/author/get_author_marketing_info?o_author_id=7184805142959489082&platform_source=1&platform_channel=1"
    return call_api(url, author_id)


def get_author_spread_info_xingtu(author_id):
    '''获取传播价值(星图)'''
    url = "https://www.xingtu.cn/gw/api/data_sp/get_author_spread_info?o_author_id=7184805142959489082&platform_source=1&platform_channel=1&type=2&flow_type=0&only_assign=true&range=2"
    return call_api(url, author_id)


def get_author_spread_info_daily(author_id):
    '''获取传播价值(日常)'''
    url = "https://www.xingtu.cn/gw/api/data_sp/get_author_spread_info?o_author_id=7184805142959489082&platform_source=1&platform_channel=1&type=1&flow_type=0&only_assign=false&range=2"
    return call_api(url, author_id)


def get_author_commerce_seed_info(author_id):
    '''获取种草价值'''
    url = "https://www.xingtu.cn/gw/api/aggregator/get_author_commerce_seed_base_info?o_author_id=7184805142959489082&range=30"
    return call_api(url, author_id)


def get_author_convert_ability(author_id):
    '''获取转化价值'''
    url = "https://www.xingtu.cn/gw/api/data_sp/get_author_convert_ability?o_author_id=7184805142959489082&platform_source=1&platform_channel=1&industry_id=0&range=2"
    return call_api(url, author_id)


def get_author_link_card(author_id):
    '''获取连接用户分布'''
    url = "https://www.xingtu.cn/gw/api/data_sp/author_link_card?o_author_id=7184805142959489082&platform_source=1&platform_channel=1&industry_id=0"
    return call_api(url, author_id)


def get_author_fans_distribution(author_id):
    '''获取粉丝数据'''
    url = "https://www.xingtu.cn/gw/api/data_sp/get_author_fans_distribution?o_author_id=7184805142959489082&platform_source=1&author_type=1"
    return call_api(url, author_id)


def get_author_commerce_spread_info(author_id):
    '''获取预估cpe/cpm'''
    url = "https://www.xingtu.cn/gw/api/aggregator/get_author_commerce_spread_info?o_author_id=7184805142959489082"
    return call_api(url, author_id)


def extract_author_data(author_id, responses):
    '''从API响应中提取数据'''
    result = {
        '达人昵称': '',
        '达人ID': author_id,
        '抖音主页链接': '',
        '星图链接': f'https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{author_id}',
        '地区': '',
        '性别': '',
        '达人类型': '',
        '粉丝数': '',
        '赞藏': '',
        '1-20s报价': '',
        '20-60s报价': '',
        '60+s报价': '',
        '短直种草报价': '',
        '商单播放量': '',
        '商单互动量': '',
        '商单平均时长': '',
        '商单完播率': '',
        '商单互动率': '',
        '商单点赞': '',
        '商单分享': '',
        '商单评论': '',
        '商单CPE': '',
        '商单CPC': '',
        '日常播放量': '',
        '日常互动量': '',
        '日常平均时长': '',
        '日常完播率': '',
        '日常互动率': '',
        '日常点赞': '',
        '日常分享': '',
        '日常评论': '',
        '日常CPE': '',
        '日常CPC': '',
        '1-20秒预估CPE': '',
        '20-60秒预估CPE': '',
        '60+秒预估CPE': '',
        '1-20秒预估CPM': '',
        '20-60秒预估CPM': '',
        '60+秒预估CPM': '',
        '爆文率': '',
        '预期播放量': '',
        '看后搜次数': '',
        '看后搜率': '',
        'A3增长数': '',
        '进店成本': '',
        '商单播放中位数': '',
        '商单组件点击量': '',
        '商单组件点击率': '',
        '商单转化CPC': '',
        '了解用户数': '',
        '兴趣用户成本': '',
        '喜欢用户数': '',
        '连接用户数': '',
        '男粉占比': '',
        '女粉占比': '',
        '低于占比': '',
    }

    # 1. 基本信息
    base_info = responses.get('base_info')
    if base_info:
        result['达人昵称'] = base_info.get('nick_name', '')
        result['达人ID'] = base_info.get('id', author_id)
        sec_uid = base_info.get('sec_uid', '')
        if sec_uid:
            result['抖音主页链接'] = f'https://www.douyin.com/user/{sec_uid}'
        result['地区'] = base_info.get('city', '')

        # 性别转换
        gender = base_info.get('gender', '')
        if gender == 1:
            result['性别'] = '女'
        elif gender == 2:
            result['性别'] = '男'

        # 达人类型
        tags_relation = base_info.get('tags_relation', {})
        if tags_relation:
            # 取第一个键作为达人类型
            result['达人类型'] = list(tags_relation.keys())[0] if tags_relation else ''

    # 2. 粉丝数赞藏
    display = responses.get('display')
    if display:
        result['粉丝数'] = display.get('follower', '')
        result['赞藏'] = display.get('link_cnt', '')

    # 3. 报价
    marketing_info = responses.get('marketing_info')
    if marketing_info and 'price_info' in marketing_info:
        price_list = marketing_info['price_info']
        for price in price_list:
            video_type = price.get('video_type')
            price_value = price.get('price', 0)
            if video_type == 1:
                result['1-20s报价'] = price_value
            elif video_type == 2:
                result['20-60s报价'] = price_value
            elif video_type == 71:
                result['60+s报价'] = price_value
            elif video_type == 150:
                result['短直种草报价'] = price_value

    # 4. 传播价值(星图)
    spread_xingtu = responses.get('spread_xingtu')
    if spread_xingtu:
        result['商单播放量'] = spread_xingtu.get('play_mid', '')

        like_avg = spread_xingtu.get('like_avg', 0)
        share_avg = spread_xingtu.get('share_avg', 0)
        comment_avg = spread_xingtu.get('comment_avg', 0)
        result['商单互动量'] = like_avg + share_avg + comment_avg

        result['商单平均时长'] = spread_xingtu.get('avg_duration', '')
        result['商单点赞'] = like_avg
        result['商单分享'] = share_avg
        result['商单评论'] = comment_avg

        # 完播率和互动率
        play_over_rate = spread_xingtu.get('play_over_rate', {})
        if isinstance(play_over_rate, dict):
            result['商单完播率'] = play_over_rate.get('value', '')

        interact_rate = spread_xingtu.get('interact_rate', {})
        if isinstance(interact_rate, dict):
            result['商单互动率'] = interact_rate.get('value', '')

        # 计算CPE和CPC
        price_20_60 = result['20-60s报价']
        play_mid = spread_xingtu.get('play_mid', 0)
        if price_20_60 and result['商单互动量']:
            try:
                result['商单CPE'] = round(float(price_20_60) / float(result['商单互动量']), 2)
            except:
                pass

        if price_20_60 and play_mid:
            try:
                result['商单CPC'] = round(float(price_20_60) / float(play_mid), 2)
            except:
                pass

    # 5. 传播价值(日常)
    spread_daily = responses.get('spread_daily')
    if spread_daily:
        result['日常播放量'] = spread_daily.get('play_mid', '')

        like_avg = spread_daily.get('like_avg', 0)
        share_avg = spread_daily.get('share_avg', 0)
        comment_avg = spread_daily.get('comment_avg', 0)
        result['日常互动量'] = like_avg + share_avg + comment_avg

        result['日常平均时长'] = spread_daily.get('avg_duration', '')
        result['日常点赞'] = like_avg
        result['日常分享'] = share_avg
        result['日常评论'] = comment_avg

        # 完播率和互动率
        play_over_rate = spread_daily.get('play_over_rate', {})
        if isinstance(play_over_rate, dict):
            result['日常完播率'] = play_over_rate.get('value', '')

        interact_rate = spread_daily.get('interact_rate', {})
        if isinstance(interact_rate, dict):
            result['日常互动率'] = interact_rate.get('value', '')

        # 计算CPE和CPC
        price_20_60 = result['20-60s报价']
        play_mid = spread_daily.get('play_mid', 0)
        if price_20_60 and result['日常互动量']:
            try:
                result['日常CPE'] = round(float(price_20_60) / float(result['日常互动量']), 2)
            except:
                pass

        if price_20_60 and play_mid:
            try:
                result['日常CPC'] = round(float(price_20_60) / float(play_mid), 2)
            except:
                pass

    # 6. 预估CPE/CPM
    commerce_spread = responses.get('commerce_spread')
    if commerce_spread:
        result['1-20秒预估CPE'] = commerce_spread.get('cpe_1_20', '')
        result['20-60秒预估CPE'] = commerce_spread.get('cpe_20_60', '')
        result['60+秒预估CPE'] = commerce_spread.get('cpe_60', '')
        result['1-20秒预估CPM'] = commerce_spread.get('cpm_1_20', '')
        result['20-60秒预估CPM'] = commerce_spread.get('cpm_20_60', '')
        result['60+秒预估CPM'] = commerce_spread.get('cpm_60', '')
        result['爆文率'] = commerce_spread.get('platform_hot_rate', '')
        result['预期播放量'] = commerce_spread.get('vv', '')

    # 7. 种草价值
    seed_info = responses.get('seed_info')
    if seed_info:
        data = seed_info
        result['看后搜次数'] = data.get('avg_search_after_view_cnt', '')
        result['看后搜率'] = data.get('avg_search_after_view_rate', '')
        result['A3增长数'] = data.get('avg_a3_incr_cnt', '')
        result['进店成本'] = data.get('shop_cost', '')

    # 8. 转化价值
    convert_ability = responses.get('convert_ability')
    if convert_ability:
        data = convert_ability
        video_vv_median = data.get('video_vv_median', {})
        if isinstance(video_vv_median, dict):
            result['商单播放中位数'] = video_vv_median.get('value', '')

        result['商单组件点击量'] = data.get('component_click_cnt_range', '')
        result['商单组件点击率'] = data.get('component_click_rate_range', '')
        result['商单转化CPC'] = data.get('related_cpc_range', '')

    # 9. 连接用户分布
    link_card = responses.get('link_card')
    if link_card and 'link_struct' in link_card:
        link_struct = link_card['link_struct']
        # link_struct 是字典，键为字符串 '1', '2', '3', '4', '5'
        if isinstance(link_struct, dict):
            result['了解用户数'] = link_struct.get('1', {}).get('value', '')
            result['兴趣用户成本'] = link_struct.get('2', {}).get('value', '')
            result['喜欢用户数'] = link_struct.get('3', {}).get('value', '')
            result['连接用户数'] = link_struct.get('5', {}).get('value', '')

    # 10. 粉丝数据
    fans_distribution = responses.get('fans_distribution')
    if fans_distribution and 'distributions' in fans_distribution:
        distributions = fans_distribution['distributions']

        for dist in distributions:
            dist_type = dist.get('type')
            distribution_list = dist.get('distribution_list', [])

            # 性别分布 type=1
            if dist_type == 1:
                total = sum([item.get('distribution_value', 0) for item in distribution_list])
                for item in distribution_list:
                    key = item.get('distribution_key')
                    value = item.get('distribution_value', 0)
                    if key == 'male' and total > 0:
                        result['男粉占比'] = f"{round(value / total * 100, 2)}%"
                    elif key == 'female' and total > 0:
                        result['女粉占比'] = f"{round(value / total * 100, 2)}%"

            # 年龄分布 type=2
            elif dist_type == 2:
                # 计算总和
                total = sum([item.get('distribution_value', 0) for item in distribution_list])
                if total > 0:
                    for item in distribution_list:
                        key = item.get('distribution_key', '')
                        value = item.get('distribution_value', 0)
                        if key:
                            # 计算占比：单个值/总和
                            percentage = round(value / total * 100, 2)
                            result[f'{key}'] = f"{percentage}%"

            # 八大人群分布 type=1024
            elif dist_type == 1024:
                # 计算总和
                total = sum([item.get('distribution_value', 0) for item in distribution_list])
                if total > 0:
                    for item in distribution_list:
                        key = item.get('distribution_key', '')
                        value = item.get('distribution_value', 0)
                        if key:
                            # 计算占比：单个值/总和
                            percentage = round(value / total * 100, 2)
                            result[f'{key}'] = f"{percentage}%"

            # 低于占比 type=256
            elif dist_type == 256:
                result['低于占比'] = dist.get('description', '')

    return result


def fetch_author_data(author_id):
    '''获取单个达人的所有数据'''
    logger.info(f"正在获取达人 {author_id} 的数据...")

    responses = {}

    # 调用所有API
    print("  - 获取基本信息")
    responses['base_info'] = get_author_base_info(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取粉丝数赞藏")
    responses['display'] = get_author_display(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取报价信息")
    responses['marketing_info'] = get_author_marketing_info(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取传播价值(星图)")
    responses['spread_xingtu'] = get_author_spread_info_xingtu(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取传播价值(日常)")
    responses['spread_daily'] = get_author_spread_info_daily(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取种草价值")
    responses['seed_info'] = get_author_commerce_seed_info(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取转化价值")
    responses['convert_ability'] = get_author_convert_ability(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取连接用户分布")
    responses['link_card'] = get_author_link_card(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取粉丝数据")
    responses['fans_distribution'] = get_author_fans_distribution(author_id)
    time.sleep(REQUEST_DELAY)

    print("  - 获取预估CPE/CPM")
    responses['commerce_spread'] = get_author_commerce_spread_info(author_id)
    time.sleep(REQUEST_DELAY)

    # 提取数据
    try:
        print(f"\n开始提取数据，author_id={author_id}")
        print(f"responses keys: {list(responses.keys())}")

        # 检查base_info结构
        if responses.get('base_info'):
            print(f"base_info keys: {list(responses['base_info'].keys())}")

        author_data = extract_author_data(author_id, responses)
        print(f"数据提取成功，达人昵称: {author_data.get('达人昵称', '未知')}")
    except Exception as e:
        print(f"!!! 提取数据时出错: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    print(f"达人 {author_data['达人昵称']} 数据获取完成")

    return author_data


def main():
    '''主函数'''
    setup_logger()

    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info("抖音星图达人挂靠数据抓取程序启动")
    logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    try:
        # 1. 选择Excel文件
        excel_path = select_excel_file()
        if not excel_path:
            return False

        # 2. 读取Excel
        logger.info("正在读取Excel文件...")
        df = pd.read_excel(excel_path)

        # 查找星图链接列
        xingtu_column = None
        for col in df.columns:
            if '星图' in str(col) and '链接' in str(col):
                xingtu_column = col
                break

        if not xingtu_column:
            # 尝试其他可能的列名
            for col in df.columns:
                if 'xingtu' in str(col).lower() or 'url' in str(col).lower() or '链接' in str(col):
                    xingtu_column = col
                    break

        if not xingtu_column:
            logger.error("未找到星图链接列，请确保Excel中有包含'星图链接'的列")
            return False

        logger.info(f"找到星图链接列: {xingtu_column}")
        logger.info(f"共有 {len(df)} 条数据")

        # 3. 提取author_id并获取数据
        results = []
        success_count = 0
        fail_count = 0

        for index, row in df.iterrows():
            logger.info(f"\n处理第 {index + 1}/{len(df)} 条数据...")

            xingtu_url = row[xingtu_column]
            author_id = extract_author_id(xingtu_url)

            if not author_id:
                logger.warning(f"无法从链接中提取author_id: {xingtu_url}")
                fail_count += 1
                continue

            try:
                author_data = fetch_author_data(author_id)
                results.append(author_data)
                success_count += 1
            except Exception as e:
                logger.error(f"获取达人 {author_id} 数据失败: {str(e)}")
                fail_count += 1
                continue

        # 4. 导出到Excel
        if results:
            logger.info("\n正在导出数据到Excel...")

            result_df = pd.DataFrame(results)

            # 生成输出文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(excel_path).parent / f'抖音达人挂靠数据_{timestamp}.xlsx'

            result_df.to_excel(output_path, index=False, engine='openpyxl')

            logger.success(f"数据已导出到: {output_path}")
        else:
            logger.warning("没有成功获取任何数据")

        # 5. 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("数据抓取完成")
        logger.info("执行统计:")
        logger.info(f"   执行时长: {duration:.2f} 秒")
        logger.info(f"   总数据数: {len(df)}")
        logger.info(f"   成功: {success_count}")
        logger.info(f"   失败: {fail_count}")
        logger.info(f"   结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        return True

    except KeyboardInterrupt:
        logger.warning("\n用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
