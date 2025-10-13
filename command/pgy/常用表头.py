import json
import configparser
import logging
from pathlib import Path
from datetime import datetime
import time

import requests

from service.feishu_service import read_table_content, update_record
from service.pgy_service import get_data_summary, get_fans_summary, get_core_data, get_fans_profile, get_notes_detail, \
    get_notes_rate

"""
从飞书获取博主ID，然后请求小红书接口获取报价和指标数据，并更新回飞书
"""

def setup_logger():
    """设置日志"""
    # 创建logs目录（如果不存在）
    log_dir = Path.cwd() / 'logs'
    log_dir.mkdir(exist_ok=True)

    # 生成日志文件名（使用当前时间）
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'pgy_data_waicai_log_{current_time}.log'

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    # 使用当前工作目录，确保与exe文件在同一目录
    config_path = Path.cwd() / 'config.ini'
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    config.read(config_path, encoding='utf-8')
    return config

def get_blogger_info(user_id, headers):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :param headers: 请求头
    :return: 视频报价和图文报价
    """
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{user_id}"
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            return result
        else:
            logging.warning(f"获取博主{user_id}报价信息失败: {data.get('msg')}")
            return []
    except Exception as e:
        logging.error(f"获取博主{user_id}报价信息出错: {str(e)}")
        return {}

def main():
    logger = setup_logger()
    try:
        # 加载配置文件
        config = load_config()
        logger.info("成功加载配置文件")

        # 从配置文件读取设置
        app_token = config.get('Feishu', 'app_token')
        table_id = config.get('Feishu', 'table_id')
        view_id = config.get('Feishu', 'view_id')
        cookie = config.get('PGY', 'cookie', raw=True)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            "cookie": cookie
        }

        # 从飞书获取数据
        records = read_table_content(app_token, table_id, view_id)

        if not records:
            logger.warning("未从飞书获取到记录")
            return

        logger.info(f"从飞书获取到{len(records)}条记录")

        # 处理每条记录
        success_count = 0
        error_count = 0
        total_records = len(records)

        for index, record in enumerate(records, 1):
            try:
                record_id = record.get("record_id")
                fields = record.get("fields", {})
                # 获取博主ID
                user_id = fields.get("博主id")['value'][0]['text']
                if not user_id:
                    logger.warning(f"记录{record_id}无博主ID，跳过")
                    continue

                logger.info(f"处理博主ID: {user_id} ({index}/{total_records})")
                
                # 获取博主信息
                blogger_info = get_blogger_info(user_id, headers)
                content_tags = blogger_info.get("contentTags", [])
                personal_tags = blogger_info.get("personalTags", [])
                tag_list = []
                for tag in content_tags:
                    if "taxonomy1Tag" in tag:
                        tag_list.append(tag["taxonomy1Tag"])
                    if "taxonomy2Tags" in tag and isinstance(tag["taxonomy2Tags"], list):
                        tag_list.extend(tag["taxonomy2Tags"])
                tag_string = "、".join(tag_list)
                personal_list = []
                if personal_tags:
                    for tag in personal_tags:
                        personal_list.append(tag)
                personal_string = "、".join(personal_list)
                level_map = {
                    0: "异常",
                    1: "普通",
                    2: "优秀"
                }
                currentLevel = blogger_info.get("currentLevel", 0)
                current_level = level_map.get(currentLevel, "")
                time.sleep(6)
                bloggers = get_notes_detail1(user_id, headers)
                last_publish_date = None

                notes_total = len(bloggers)
                video_total = 0
                like_total = 0
                for blogger in bloggers:
                    is_video = blogger.get('isVideo')
                    if is_video:
                        video_total += 1
                    date_str = blogger.get('date')
                    like_num = blogger.get('likeNum', 0)
                    if not date_str:
                        continue
                    try:
                        note_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except:
                        continue
                    if not last_publish_date or note_date > last_publish_date:
                        last_publish_date = note_date
                    like_total += like_num
                time.sleep(6)

                bloggers1 = get_notes_detail(user_id, headers)
                brand_set = set()
                brand_date_map = set()

                for item in bloggers1:
                    brandName = item.get("brandName")
                    if brandName:
                        brand_set.add(brandName)
                        date_obj = datetime.strptime(item.get("date"), "%Y-%m-%d")
                        date_str = f"{date_obj.month}月{date_obj.day}日"
                        brandName_str = f"{brandName}:{date_str}"
                        brand_date_map.add(brandName_str)
                # 构造"已合作品牌"格式
                brand_list = list(brand_set)
                brand_list.sort()  # 如需要排序，否则可去掉
                brand_names_str = "、".join([f"{name}" for i, name in enumerate(brand_list)])

                # 构造"品牌合作日期"格式
                brand_date_map = list(brand_date_map)
                brand_date_map.sort()  # 如需要排序，否则可去掉
                brand_dates_str = "、".join([f"{name}" for i, name in enumerate(brand_date_map)])
                time.sleep(6)
                notes_rate = get_notes_rate(user_id, 0, 3, 1, 1, headers)
                time.sleep(6)
                data_summary = get_data_summary(user_id, business=1, header=headers)
                time.sleep(6)
                rc_data_summary = get_data_summary(user_id, business=0, header=headers)
                time.sleep(6)
                fans_summary = get_fans_summary(user_id, headers)
                time.sleep(6)
                rc_core_data = get_core_data(user_id, business=0, noteType=3, advertiseSwitch=1, header=headers)
                rc_metrics = extract_sum_data(rc_core_data)
                time.sleep(6)
                hz_core_data = get_core_data(user_id, business=1, noteType=3, advertiseSwitch=1, header=headers)
                hz_metrics = extract_sum_data(hz_core_data)
                time.sleep(6)
                fans_profile = get_fans_profile(user_id, headers)
                gender_infos = fans_profile.get('gender', {})
                time.sleep(6)
                # 更新飞书记录
                # 健壮性处理：所有字段访问前都加类型和空值判断，防止 NoneType 报错
                tag_string = tag_string if isinstance(tag_string, str) else ''
                # 机构名健壮性
                note_sign = blogger_info.get("noteSign")
                org_name = note_sign.get("name") if isinstance(note_sign, dict) else ''
                # tradeNames 健壮性
                trade_names = data_summary.get("tradeNames") if isinstance(data_summary, dict) else []
                trade_name = ", ".join(str(name) for name in trade_names) if isinstance(trade_names, list) and trade_names else ""

                # 性别健壮性
                female_percent = 0.0
                if isinstance(gender_infos, dict):
                    female_percent = gender_infos.get('female', 0.0)
                # 年龄分布健壮性
                def get_age_percent(ages, idx):
                    if isinstance(ages, list) and len(ages) > idx and isinstance(ages[idx], dict):
                        return round(float(ages[idx].get('percent', 0.0)) * 100, 2)
                    return 0.0
                ages = fans_profile.get('ages', []) if isinstance(fans_profile, dict) else []
                provinces = fans_profile.get('provinces', []) if isinstance(fans_profile, dict) else []
                interests = fans_profile.get('interests', []) if isinstance(fans_profile, dict) else []
                devices = fans_profile.get('devices', {})
                apple_percent = next((d["percent"] for d in devices if d["name"].lower() == "apple inc."), 0)
                huawei_percent = next((d["percent"] for d in devices if d["name"].lower() == "huawei"), 0)
                apple_percent_str = f"苹果:{apple_percent * 100:.2f}%,华为:{huawei_percent * 100:.2f}%"
                # 地域分布健壮性
                province_str = " 、".join([
                    f"{p['name']} ({round(p['percent'] * 100, 1)}%)"
                    for p in provinces[:7] if isinstance(p, dict) and 'name' in p and 'percent' in p
                ])
                # 兴趣健壮性
                interest_str = " 、".join([
                    f"{p['name']} ({round(p['percent'] * 100, 1)}%)"
                    for p in interests[:5] if isinstance(p, dict) and 'name' in p and 'percent' in p
                ])
                update_fields = {
                    # blogger_info接口
                    "达人昵称": str(blogger_info.get("name") or ""),
                    '小红书链接': f"https://www.xiaohongshu.com/user/profile/{user_id}",
                    "小红书ID": str(blogger_info.get("redId") or ""),
                    "粉丝量": f"{(blogger_info.get('fansCount') or 0) / 10000:.1f}",
                    "赞藏": f"{(blogger_info.get('likeCollectCountInfo') or 0) / 10000:.1f}",
                    "所在地区": str(blogger_info.get("location") or ""),
                    "标签": tag_string,
                    '图文价格': str(blogger_info.get("picturePrice") or 0),
                    '视频价格': str(blogger_info.get("videoPrice") or 0),
                    "所属机构": str(org_name),
                    "蒲公英状态": str(current_level),
                    "博主人设": str(personal_string),
                    "最新笔记更新时间": last_publish_date.strftime('%Y-%m-%d') if last_publish_date else '',
                    '前两页视频笔记占比': str(safe_divide(video_total, notes_total)),
                    '前两页笔记点赞中位数': str(safe_divide(like_total, notes_total)),
                    '博主已合作品牌': str(brand_names_str),
                    '博主已合作品牌及合作日期': str(brand_dates_str),
                    '百赞比例': str(notes_rate.get("hundredLikePercent")),
                    '千赞比例': str(notes_rate.get("thousandLikePercent")),
                    "邀约48小时回复率": str(data_summary.get("responseRate", "")) if isinstance(data_summary, dict) else '',
                    "合作行业": str(trade_name),
                    '活跃粉丝占比': f"{fans_summary.get('activeFansRate', 0)}%" if isinstance(fans_summary, dict) else '0%',
                    "图文预估cpm": float(data_summary.get("estimatePictureCpm", 0)) if isinstance(data_summary, dict) else 0.0,
                    "视频预估cpm": float(data_summary.get("estimateVideoCpm", 0)) if isinstance(data_summary, dict) else 0.0,
                    "图文预估cpc": float(data_summary.get("estimatePictureCpm", 0)) if isinstance(data_summary, dict) else 0.0,
                    "视频预估cpc": float(data_summary.get("videoReadCostV2", 0)) if isinstance(data_summary, dict) else 0.0,
                    "图文预估cpe": float(data_summary.get("estimatePictureEngageCost", 0)) if isinstance(data_summary, dict) else 0.0,
                    "视频预估cpe": float(data_summary.get("estimateVideoEngageCost", 0)) if isinstance(data_summary, dict) else 0.0,
                    "日常曝光中位数": str(rc_data_summary.get("mAccumImpNum", "")) if isinstance(rc_data_summary, dict) else '',
                    "日常阅读中位数": str(rc_data_summary.get("mValidRawReadFeedNum", "")) if isinstance(rc_data_summary, dict) else '',
                    "日常互动中位数": str(rc_data_summary.get("mEngagementNum", "")) if isinstance(rc_data_summary, dict) else '',
                    "合作曝光中位数": str(data_summary.get("mAccumImpNum", "")) if isinstance(data_summary, dict) else '',
                    "合作阅读中位数": str(data_summary.get("mValidRawReadFeedNum", "")) if isinstance(data_summary, dict) else '',
                    "合作互动中位数": str(data_summary.get("mEngagementNum", "")) if isinstance(data_summary, dict) else '',
                    "日常图文+视频cpm": float(rc_metrics.get("cpm", 0)),
                    "合作图文+视频cpm": float(hz_metrics.get("cpm", 0)),
                    "日常图文+视频cpc": float(rc_metrics.get("cpc", 0)),
                    "合作图文+视频cpc": float(hz_metrics.get("cpc", 0)),
                    "日常图文+视频cpe": float(rc_metrics.get("cpe", 0)),
                    "合作图文+视频cpe": float(hz_metrics.get("cpe", 0)),
                    "女性粉丝占比": f"{round(float(female_percent)*100, 2)}%",
                    "年龄＜18": f"{get_age_percent(ages, 0)}%",
                    "年龄18-24": f"{get_age_percent(ages, 1)}%",
                    "年龄25-34": f"{get_age_percent(ages, 2)}%",
                    "年龄35-44": f"{get_age_percent(ages, 3)}%",
                    "年龄＞44": f"{get_age_percent(ages, 4)}%",
                    "地域分布": province_str,
                    "用户兴趣": interest_str,
                    "设备苹果华为": apple_percent_str,
                }

                update_record(app_token, table_id, record_id, update_fields)
                success_count += 1

                # 每处理10条记录输出一次进度
                if success_count % 10 == 0:
                    progress = (index / total_records) * 100
                    logger.info(f"同步进度: {progress:.2f}% ({success_count}/{total_records})")

            except Exception as e:
                error_count += 1
                logger.error(f"处理记录失败 (记录 {index}, 博主ID: {user_id if 'user_id' in locals() else 'unknown'}): {str(e)}")
                continue

        # 输出最终统计信息
        logger.info("数据更新完成!")
        logger.info(f"总记录数: {total_records}")
        logger.info(f"成功更新: {success_count}")
        logger.info(f"处理失败: {error_count}")

    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")

def safe_min(*args):
    return min([x for x in args if x > 0], default=0)

# 计算 max 值（非0）
def safe_max(*args):
    return max([x for x in args if x > 0], default=0)

def safe_divide(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0.0

def extract_sum_data(core_data):
    """提取 core_data 中的关键字段，默认值为 0"""
    sum_data = core_data.get("sumData", {}) if core_data else {}
    return {
        "cpe": sum_data.get("cpe", 0),
        "cpm": sum_data.get("cpm", 0),
        "cpc": sum_data.get("cpv", 0),
        "read": sum_data.get("read", 0),
        "imp": sum_data.get("imp", 0),
        "engage": sum_data.get("engage", 0),
    }

def get_notes_detail1(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v2/notes_detail?advertiseSwitch=1&orderType=1&pageNumber=1&pageSize=2&userId={user_id}&noteType=4&isThirdPlatform=0"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {}).get('list', [])
            return result
        else:
            print(f"获取博主{user_id}笔记信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}笔记信息出错: {str(e)}")
        return {}

if __name__ == '__main__':
    main()