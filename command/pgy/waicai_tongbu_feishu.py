import json
import re
import configparser
import logging
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import requests

from core.database_text_tibao_2 import session
from core.database_text_tibao_3 import session as session_tibao_3
from models.models_tibao import ChinaProvince, ChinaCity, FpOutBloggerInfo, FpOutBloggerNoteRate, \
    FpOutBloggerDataSummary, FpOutBloggerFansHistory, FpOutBloggerFansSummary, FpOutBloggerFansProfile, \
    FpOutBloggerNoteDetail, KolMediaAccount, KolOrder

from service.feishu_service import update_record, get_feishu_headers


def setup_logger():
    """设置日志"""
    # 创建logs目录（如果不存在）
    log_dir = Path.cwd() / 'logs'
    log_dir.mkdir(exist_ok=True)

    # 生成日志文件名（使用当前时间）
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'sync_log_{current_time}.log'

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


def calculate_metric(numerator, denominator, multiplier=1, decimal_places=2):
    try:
        if not denominator or float(denominator) == 0:
            return 0.0

        result = (float(numerator) / float(denominator)) * multiplier
        # 使用Decimal进行精确四舍五入
        return float(Decimal(str(result)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    except (ValueError, TypeError, ZeroDivisionError):
        return 0.0


def calculate_engagement_metrics(price, engagement_num, imp_median, read_median):
    return {
        'cpe': calculate_metric(price, engagement_num),  # CPE = 价格/互动数
        'cpm': calculate_metric(price, imp_median, 1000),  # CPM = (价格/曝光数)*1000
        'cpc': calculate_metric(price, read_median),  # CPC = 价格/阅读数
        'cpr': calculate_metric(read_median, imp_median)  # CPR = 阅读数/曝光数
    }


def get_blogger_note_rates(video_price, graphic_price, user_id):
    """
    获取博主的笔记数据统计
    :param user_id: 用户ID
    :return: 处理后的数据字典
    """
    # 初始化默认指标
    default_metrics = {
        'pic_text_cpc': 0,
        'pic_text_cpe': 0,
        'pic_text_cpm': 0,
        'pic_text_cpr': 0,
        'video_cpc': 0,
        'video_cpe': 0,
        'video_cpm': 0,
        'video_cpr': 0,
    }

    # 初始化结果字典
    result = {
        # 30天数据
        'daily_30': default_metrics.copy(),
        'coop_30': default_metrics.copy(),
        # 90天数据
        'daily_90': default_metrics.copy(),
        'coop_90': default_metrics.copy()
    }

    # 查询所有相关记录
    note_rates = session.query(FpOutBloggerNoteRate).filter(
        FpOutBloggerNoteRate.user_id == user_id
    ).all()
    for note in note_rates:
        # 确定数据类型（30天/90天）
        date_key = 'daily_30' if note.date_type == 1 else 'daily_90'
        if note.business == 1:  # 合作笔记
            date_key = 'coop_30' if note.date_type == 1 else 'coop_90'

        # 只处理全流量数据
        if note.advertise_switch != 1:
            continue

        # 定义字段映射关系
        field_mappings = {
            'imp': 'impMedian',
            'read': 'readMedian',
            'likeMedian': 'likeMedian',
            'collectMedian': 'collectMedian',
            'commentMedian': 'commentMedian',
            'shareMedian': 'shareMedian',
            'mFollowCnt': 'mFollowCnt',
            'mEngagementNum': 'mEngagementNum',
            'interactionRate': ('interactionRate', 100),
            'thousandLikePercent': ('thousandLikePercent', 100),
            'videoFullViewRate': ('videoFullViewRate', 100),
            'picture3sViewRate': ('picture3sViewRate', 100)
        }

        # 处理日常30天数据
        if note.date_type == 1 and note.business == 0 and note.advertise_switch == 1:
            try:
                page_percent = json.loads(note.pagePercentVo or "{}")
            except Exception:
                page_percent = {}
            result['imp_homefeed_percent'] = page_percent.get('impHomefeedPercent', 0)
            result['read_homefeed_percent'] = page_percent.get('readHomefeedPercent', 0)
            result['daily_30_readMedianBeyondRate'] = note.readMedianBeyondRate
            result['daily_30_interactionBeyondRate'] = note.interactionBeyondRate
            metrics2 = calculate_engagement_metrics(
                graphic_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'pic_text_cpe': metrics2['cpe'],
                'pic_text_cpm': metrics2['cpm'],
                'pic_text_cpc': metrics2['cpc'],
                'pic_text_cpr': metrics2['cpr']
            })
            metrics1 = calculate_engagement_metrics(
                video_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'video_cpe': metrics1['cpe'],
                'video_cpm': metrics1['cpm'],
                'video_cpc': metrics1['cpc'],
                'video_cpr': metrics1['cpr']
            })

            # 使用循环处理字段映射
            for result_key, source_key in field_mappings.items():
                if isinstance(source_key, tuple):
                    source_field, divisor = source_key
                    result[f'daily_30_{result_key}'] = (getattr(note, source_field) or 0) / divisor
                else:
                    result[f'daily_30_{result_key}'] = getattr(note, source_key) or 0

        # 处理合作30天数据
        if note.date_type == 1 and note.business == 1 and note.advertise_switch == 1:
            result['coop_30_readMedianBeyondRate'] = note.readMedianBeyondRate
            result['coop_30_interactionBeyondRate'] = note.interactionBeyondRate
            metrics2 = calculate_engagement_metrics(
                graphic_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'pic_text_cpe': metrics2['cpe'],
                'pic_text_cpm': metrics2['cpm'],
                'pic_text_cpc': metrics2['cpc'],
                'pic_text_cpr': metrics2['cpr']
            })
            metrics1 = calculate_engagement_metrics(
                video_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'video_cpe': metrics1['cpe'],
                'video_cpm': metrics1['cpm'],
                'video_cpc': metrics1['cpc'],
                'video_cpr': metrics1['cpr']
            })
            # 使用循环处理字段映射
            for result_key, source_key in field_mappings.items():
                if isinstance(source_key, tuple):
                    source_field, divisor = source_key
                    result[f'coop_30_{result_key}'] = (getattr(note, source_field) or 0) / divisor
                else:
                    result[f'coop_30_{result_key}'] = getattr(note, source_key) or 0

        # 处理合作90天数据
        if note.date_type == 2 and note.business == 1 and note.advertise_switch == 1:
            metrics2 = calculate_engagement_metrics(
                graphic_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'pic_text_cpe': metrics2['cpe'],
                'pic_text_cpm': metrics2['cpm'],
                'pic_text_cpc': metrics2['cpc'],
                'pic_text_cpr': metrics2['cpr']
            })
            metrics1 = calculate_engagement_metrics(
                video_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'video_cpe': metrics1['cpe'],
                'video_cpm': metrics1['cpm'],
                'video_cpc': metrics1['cpc'],
                'video_cpr': metrics1['cpr']
            })
            # 使用循环处理字段映射
            for result_key, source_key in field_mappings.items():
                if isinstance(source_key, tuple):
                    source_field, divisor = source_key
                    result[f'coop_90_{result_key}'] = (getattr(note, source_field) or 0) / divisor
                else:
                    result[f'coop_90_{result_key}'] = getattr(note, source_key) or 0

        # 处理日常90天数据
        if note.date_type == 2 and note.business == 0 and note.advertise_switch == 1:
            metrics2 = calculate_engagement_metrics(
                graphic_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'pic_text_cpe': metrics2['cpe'],
                'pic_text_cpm': metrics2['cpm'],
                'pic_text_cpc': metrics2['cpc'],
                'pic_text_cpr': metrics2['cpr']
            })
            metrics1 = calculate_engagement_metrics(
                video_price,
                note.mEngagementNum or 0,
                note.impMedian or 0,
                note.readMedian or 0
            )
            result[date_key].update({
                'video_cpe': metrics1['cpe'],
                'video_cpm': metrics1['cpm'],
                'video_cpc': metrics1['cpc'],
                'video_cpr': metrics1['cpr']
            })
            # 使用循环处理字段映射
            for result_key, source_key in field_mappings.items():
                if isinstance(source_key, tuple):
                    source_field, divisor = source_key
                    result[f'daily_90_{result_key}'] = (getattr(note, source_field) or 0) / divisor
                else:
                    result[f'daily_90_{result_key}'] = getattr(note, source_key) or 0

    return result


def format_data_for_feishu(info):
    """格式化数据为飞书格式"""
    note_sign = None
    try:
        noteSign = json.loads(info.noteSign or "{}")
    except Exception:
        noteSign = {}
    if noteSign:
        note_sign = noteSign.get("name")
    personal_string = ''
    try:
        contentTags = json.loads(info.contentTags or "[]")
        featureTags = json.loads(info.featureTags or "[]")
        personal_tags = json.loads(info.personalTags or "[]")
        personal_list = []
        if personal_tags:
            for tag in personal_tags:
                personal_list.append(tag)
        personal_string = "、".join(personal_list)
    except Exception:
        contentTags = []
        featureTags = []
        personal_tags = []
    tags = set()
    for tag in contentTags:
        if isinstance(tag, dict):
            if tag.get("taxonomy1Tag"):
                tags.add(tag["taxonomy1Tag"])
            if tag.get("taxonomy2Tag"):
                tags.add(tag["taxonomy2Tag"])
    for tag in featureTags:
        if tag:
            tags.add(tag)

    nationality = '国外'
    city = '非一线'
    shipping_address = info.location.strip() if info.location else ''

    if shipping_address:
        province_prefix = shipping_address[:2]
        # 模糊匹配省份
        province_exists = session.query(ChinaProvince).filter(
            ChinaProvince.province_name.like(f"{province_prefix}%")
        ).first()
        if province_exists:
            nationality = '国内'

        province_suffix = shipping_address[-2:]
        # 模糊匹配城市
        city_exists = session.query(ChinaCity).filter(
            ChinaCity.city_name.like(f"{province_suffix}%")
        ).first()
        if city_exists:
            city = '一线'

    # 数据信息
    note_rates = get_blogger_note_rates(info.videoPrice, info.picturePrice, info.userId)
    # 内容类目占比
    summary = session.query(FpOutBloggerDataSummary).filter(FpOutBloggerDataSummary.user_id == info.userId).first()
    try:
        note_type = json.loads(summary.noteType or "[]") if summary and summary.noteType else []
    except Exception:
        note_type = []
    content_tag_1 = [note_type[0]['contentTag']] if len(note_type) > 0 and isinstance(note_type[0], dict) and note_type[0].get('contentTag') else []
    content_tag_2 = note_type[1]['contentTag'] if len(note_type) > 1 and isinstance(note_type[1], dict) and note_type[1].get('contentTag') else ''
    content_percent_1 = note_type[0]['percent'] if len(note_type) > 0 and isinstance(note_type[0], dict) else ''
    content_percent_2 = note_type[1]['percent'] if len(note_type) > 1 and isinstance(note_type[1], dict) else ''
    pictureReadCost = summary.picReadCost if summary and hasattr(summary, 'picReadCost') else None
    videoReadCost = summary.videoReadCostV2 if summary and hasattr(summary, 'videoReadCostV2') else None
    notes_published = summary.noteNumber if summary and hasattr(summary, 'noteNumber') else 0

    # 活跃粉丝占比
    fans_summary = session.query(FpOutBloggerFansSummary).filter(
        FpOutBloggerFansSummary.user_id == info.userId).first()
    activeFansRate = (getattr(fans_summary, 'activeFansRate', 0) or 0) / 100 if fans_summary else 0.0
    fansIncreaseNum = (getattr(fans_summary, 'fansIncreaseNum', 0) or 0) / 100 if fans_summary else 0.0
    fansGrowthRate = (getattr(fans_summary, 'fansGrowthRate', 0) or 0) / 100 if fans_summary else 0.0

    # 粉丝画像数据
    summary = session.query(FpOutBloggerFansProfile).filter(FpOutBloggerFansProfile.user_id == info.userId).first()
    try:
        ages = json.loads(summary.ages or "[]") if summary and summary.ages else []
    except Exception:
        ages = []
    try:
        gender = json.loads(summary.gender or "{}") if summary and summary.gender else {}
    except Exception:
        gender = {}
    try:
        interests = json.loads(summary.interests or "[]") if summary and summary.interests else []
    except Exception:
        interests = []
    try:
        provinces = json.loads(summary.provinces or "[]") if summary and summary.provinces else []
    except Exception:
        provinces = []
    try:
        cities = json.loads(summary.cities or "[]") if summary and summary.cities else []
    except Exception:
        cities = []
    try:
        devices = json.loads(summary.devices or "[]") if summary and summary.devices else []
    except Exception:
        devices = []

    age_less_than_18 = round(float(ages[0]['percent']), 2) if len(ages) > 0 and 'percent' in ages[0] else 0.0
    age_18_to_24 = round(float(ages[1]['percent']), 2) if len(ages) > 1 and 'percent' in ages[1] else 0.0
    age_25_to_34 = round(float(ages[2]['percent']), 2) if len(ages) > 2 and 'percent' in ages[2] else 0.0
    age_35_to_44 = round(float(ages[3]['percent']), 2) if len(ages) > 3 and 'percent' in ages[3] else 0.0
    age_greater_than_44 = round(float(ages[4]['percent']), 2) if len(ages) > 4 and 'percent' in ages[4] else 0.0

    female_fan_percentage = round(float(gender.get('female', 0)), 2) if isinstance(gender, dict) else 0.0

    interest_top1 = f"{interests[0]['name']}({round(float(interests[0]['percent']) * 100, 2)}%)" if len(interests) > 0 and 'name' in interests[0] and 'percent' in interests[0] else ""
    interest_top2 = f"{interests[1]['name']}({round(float(interests[1]['percent']) * 100, 2)}%)" if len(interests) > 1 and 'name' in interests[1] and 'percent' in interests[1] else ""
    interest_top3 = f"{interests[2]['name']}({round(float(interests[2]['percent']) * 100, 2)}%)" if len(interests) > 2 and 'name' in interests[2] and 'percent' in interests[2] else ""
    interest_top4 = f"{interests[3]['name']}({round(float(interests[3]['percent']) * 100, 2)}%)" if len(interests) > 3 and 'name' in interests[3] and 'percent' in interests[3] else ""
    interest_top5 = f"{interests[4]['name']}({round(float(interests[4]['percent']) * 100, 2)}%)" if len(interests) > 4 and 'name' in interests[4] and 'percent' in interests[4] else ""

    province_top1 = f"{provinces[0]['name']}({round(float(provinces[0]['percent']) * 100, 2)}%)" if len(provinces) > 0 and 'name' in provinces[0] and 'percent' in provinces[0] else ""
    province_top2 = f"{provinces[1]['name']}({round(float(provinces[1]['percent']) * 100, 2)}%)" if len(provinces) > 1 and 'name' in provinces[1] and 'percent' in provinces[1] else ""
    province_top3 = f"{provinces[2]['name']}({round(float(provinces[2]['percent']) * 100, 2)}%)" if len(provinces) > 2 and 'name' in provinces[2] and 'percent' in provinces[2] else ""

    city_top1 = f"{cities[0]['name']}({round(float(cities[0]['percent']) * 100, 2)}%)" if len(cities) > 0 and 'name' in cities[0] and 'percent' in cities[0] else ""
    city_top2 = f"{cities[1]['name']}({round(float(cities[1]['percent']) * 100, 2)}%)" if len(cities) > 1 and 'name' in cities[1] and 'percent' in cities[1] else ""
    city_top3 = f"{cities[2]['name']}({round(float(cities[2]['percent']) * 100, 2)}%)" if len(cities) > 2 and 'name' in cities[2] and 'percent' in cities[2] else ""

    device_top1 = f"{devices[0]['name']}({round(float(devices[0]['percent']) * 100, 2)}%)" if len(devices) > 0 and 'name' in devices[0] and 'percent' in devices[0] else ""
    device_top2 = f"{devices[1]['name']}({round(float(devices[1]['percent']) * 100, 2)}%)" if len(devices) > 1 and 'name' in devices[1] and 'percent' in devices[1] else ""
    device_top3 = f"{devices[2]['name']}({round(float(devices[2]['percent']) * 100, 2)}%)" if len(devices) > 2 and 'name' in devices[2] and 'percent' in devices[2] else ""
    device_top_name1 = re.sub(r'\(.*\)', '', device_top1).strip() if device_top1 else ''
    device_top_name2 = re.sub(r'\(.*\)', '', device_top2).strip() if device_top2 else ''
    device_top_name3 = re.sub(r'\(.*\)', '', device_top3).strip() if device_top3 else ''

    # 提取百分比数字并转换为小数
    match1 = re.search(r'\(([\d.]+)%\)', device_top1) if device_top1 else None
    device_top1_percentage = float(match1.group(1)) / 100 if match1 else 0.0

    match2 = re.search(r'\(([\d.]+)%\)', device_top2) if device_top2 else None
    device_top2_percentage = float(match2.group(1)) / 100 if match2 else 0.0

    match3 = re.search(r'\(([\d.]+)%\)', device_top3) if device_top3 else None
    device_top3_percentage = float(match3.group(1)) / 100 if match3 else 0.0

    # 订单数统计
    details = session.query(FpOutBloggerNoteDetail).filter(FpOutBloggerNoteDetail.user_id == info.userId).all()
    day30 = day90 = video_order = graphic_order = all_count = 0

    current_date = datetime.now()
    thirty_days_ago = current_date - timedelta(days=30)
    ninety_days_ago = current_date - timedelta(days=90)
    brandName = ''
    brandDate = ''
    last_publish_date = None
    notes_total = len(details)
    video_total = 0
    picture_total = 0
    for item in details:
        is_video = item.isVideo
        if is_video == '1':
            video_total += 1
        else:
            picture_total += 1
        order_date = datetime.strptime(item.date, "%Y-%m-%d")
        if not last_publish_date or order_date > last_publish_date:
            last_publish_date = order_date
        brandName = item.brandName
        if brandName:
            brandDate = item.date
            if is_video == '1':
                video_order += 1
            else:
                graphic_order += 1

            if thirty_days_ago <= order_date <= current_date:
                day30 += 1
            if ninety_days_ago <= order_date <= current_date:
                day90 += 1

            all_count += 1
    pgy_total_orders = all_count
    pgy_orders_30_days = day30
    pgy_orders_90_days = day90
    system_orders_30_days = 0
    system_orders_90_days = 0
    system_total_orders = 0
    half_year_graphic_orders = graphic_order
    half_year_video_orders = video_order

    # 查系统订单数据
    # 查系统订单数据
    account = session.query(KolMediaAccount).filter_by(dandelion_platform_id=info.userId).first()
    if account:
        media_account_id = account.id

        zf_order30 = session.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12]),
            KolOrder.created_at >= thirty_days_ago
        ).count()

        zf_order90 = session.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12]),
            KolOrder.created_at >= ninety_days_ago
        ).count()

        qy_order30 = session_tibao_3.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12]),
            KolOrder.created_at >= thirty_days_ago
        ).count()

        qy_order90 = session_tibao_3.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12]),
            KolOrder.created_at >= ninety_days_ago
        ).count()

        zf_all = session.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12])
        ).count()

        qy_all = session_tibao_3.query(KolOrder).filter(
            KolOrder.media_account_id == media_account_id,
            KolOrder.stage.notin_([11, 12])
        ).count()

        pgy_total_orders = all_count
        pgy_orders_30_days = day30
        pgy_orders_90_days = day90
        system_total_orders = zf_all + qy_all
        system_orders_30_days = zf_order30 + qy_order30
        system_orders_90_days = zf_order90 + qy_order90
        half_year_graphic_orders = graphic_order
        half_year_video_orders = video_order

    picture_price = info.picturePrice or 0
    video_price = info.videoPrice or 0

    # 构建返回数据
    formatted_data = {
        'fields': {
            # 基本信息
            '博主id': info.userId,
            '蒲公英链接': {
                "link": f'https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{info.userId}',
                "text": f'https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{info.userId}'
            },
            '达人昵称': info.name,
            '博主人设': personal_string,
            '达人简介': info.user_desc,
            '小红书链接': f"https://www.xiaohongshu.com/user/profile/{info.userId}",
            '小红书ID': str(info.redId) or None,
            '粉丝量': int(info.fansCount or 0),
            '图文价格': int(info.picturePrice or 0),
            '视频价格': int(info.videoPrice or 0),
            '粉丝数': str(info.fansCount or 0),
            '赞藏': str(info.likeCollectCountInfo or 0),
            '所在地区': info.location,
            '博主机构': note_sign,
            '性别': str(info.gender),
            '标签': list(tags),
            '国籍': nationality,
            '一线城市占比': city,
            '是否抓取': '是',

            # 内容类目信息
            '内容类目1': content_tag_1,
            '内容类目2': str(content_tag_2),
            '内容占比1': float(content_percent_1 or 0),
            '内容占比2': float(content_percent_2 or 0),
            '发布笔记数量': str(notes_published or 0),

            # fp_out_blogger_fans_summary表
            '活跃粉丝占比': str(activeFansRate or 0),
            '粉丝增量': str(fansIncreaseNum or 0),
            '粉丝量变化幅度': str(fansGrowthRate or 0),

            # fp_out_blogger_fans_profile表
            '年龄<18': str(age_less_than_18 or 0),
            '年龄18_24': str(age_18_to_24 or 0),
            '年龄25_34': str(age_25_to_34 or 0),
            '年龄35_44': str(age_35_to_44 or 0),
            '年龄>44': str(age_greater_than_44 or 0),
            '女粉丝占比': str(female_fan_percentage or 0),
            '用户兴趣top1': interest_top1 or None,
            '用户兴趣top2': interest_top2 or None,
            '用户兴趣top3': interest_top3 or None,
            '用户兴趣top4': interest_top4 or None,
            '用户兴趣top5': interest_top5 or None,
            '省份top1': province_top1 or None,
            '省份top2': province_top2 or None,
            '省份top3': province_top3 or None,
            '城市top1': city_top1 or None,
            '城市top2': city_top2 or None,
            '城市top3': city_top3 or None,
            '设备top1': device_top1 or None,
            '设备top2': device_top2 or None,
            '设备top3': device_top3 or None,
            '设备top1名称': device_top_name1 or None,
            '设备top2名称': device_top_name2 or None,
            '设备top3名称': device_top_name3 or None,
            '设备top1百分比': str(device_top1_percentage or 0),
            '设备top2百分比': str(device_top2_percentage or 0),
            '设备top3百分比': str(device_top3_percentage or 0),

            # 订单相关数据
            '系统订单总数': str(system_total_orders or 0),
            '30天内系统订单数': str(system_orders_30_days or 0),
            '90天内系统订单数': str(system_orders_90_days or 0),
            '蒲公英商单总数': str(pgy_total_orders or 0),
            '30天蒲公英商单数': str(pgy_orders_30_days or 0),
            '90天蒲公英商单数': str(pgy_orders_90_days or 0),
            '图文商单量': int(half_year_graphic_orders or 0),
            '视频商单量': int(half_year_video_orders or 0),
            '图文营收': int(picture_price * half_year_graphic_orders),
            '月总营收': int(picture_price * half_year_graphic_orders + video_price * half_year_video_orders),

            # 其他指标
            '阅读量来源的【发现页】占比': note_rates.get('read_homefeed_percent', 0),
            '曝光量来源的【发现页】占比': note_rates.get('imp_homefeed_percent', 0),
            '日常阅读中位数优于同类博主': note_rates['daily_30_readMedianBeyondRate'],
            '日常互动中位数优于同类博主': note_rates['daily_30_interactionBeyondRate'],
            '合作阅读中位数优于同类博主': note_rates['coop_30_readMedianBeyondRate'],
            '合作互动中位数优于同类博主': note_rates['coop_30_interactionBeyondRate'],
            '预估阅读单价（图文）': str(pictureReadCost or 0),
            '预估阅读单价（视频）': str(videoReadCost or 0),
            '笔记总数': notes_total,
            '图文笔记占比': safe_divide(picture_total, notes_total),
            '视频笔记占比': safe_divide(video_total, notes_total),
            '已合作品牌': brandName,
            '合作日期': brandDate,
            "笔记最后发布时间": last_publish_date.strftime('%Y-%m-%d') if last_publish_date else '',

            # 90天合作图文+视频数据
            '90天合作图文+视频-图文CPC': str(note_rates['coop_90']['pic_text_cpc'] or 0),
            '90天合作图文+视频-图文CPE': str(note_rates['coop_90']['pic_text_cpe'] or 0),
            '90天合作图文+视频-图文CPM': str(note_rates['coop_90']['pic_text_cpm'] or 0),
            '90天合作图文+视频-图文CPR': str(note_rates['coop_90']['pic_text_cpr'] or 0),
            '90天合作图文+视频-视频CPC': str(note_rates['coop_90']['video_cpc'] or 0),
            '90天合作图文+视频-视频CPE': str(note_rates['coop_90']['video_cpe'] or 0),
            '90天合作图文+视频-视频CPM': str(note_rates['coop_90']['video_cpm'] or 0),
            '90天合作图文+视频-视频CPR': str(note_rates['coop_90']['video_cpr'] or 0),
            '90天合作图文+视频点赞中位数': str(note_rates['coop_90_likeMedian'] or 0),
            '90天合作图文+视频收藏中位数': str(note_rates['coop_90_collectMedian'] or 0),
            '90天合作图文+视频评论中位数': str(note_rates['coop_90_commentMedian'] or 0),
            '90天合作图文+视频分享中位数': str(note_rates['coop_90_shareMedian'] or 0),
            '90天合作图文+视频关注中位数': str(note_rates['coop_90_mFollowCnt'] or 0),
            '90天合作图文+视频3秒阅读率': str(note_rates['coop_90_picture3sViewRate'] or 0),
            '90天合作图文+视频互动中位数': str(note_rates['coop_90_mEngagementNum'] or 0),
            '90天合作图文+视频互动率': str(note_rates['coop_90_interactionRate'] or 0),
            '90天合作图文+视频千赞笔记比例': str(note_rates['coop_90_thousandLikePercent'] or 0),
            '90天合作图文+视频完播率': str(note_rates['coop_90_videoFullViewRate'] or 0),
            '90天合作图文+视频曝光中位数': str(note_rates['coop_90_imp'] or 0),
            '90天合作图文+视频阅读中位数': str(note_rates['coop_90_read'] or 0),

            # 90天日常图文+视频数据
            '90天日常视频+图文-图文CPC': str(note_rates['daily_90']['pic_text_cpc'] or 0),
            '90天日常视频+图文-图文CPE': str(note_rates['daily_90']['pic_text_cpe'] or 0),
            '90天日常视频+图文-图文CPM': str(note_rates['daily_90']['pic_text_cpm'] or 0),
            '90天日常视频+图文-图文CPR': str(note_rates['daily_90']['pic_text_cpr'] or 0),
            '90天日常视频+图文-视频CPC': str(note_rates['daily_90']['video_cpc'] or 0),
            '90天日常视频+图文-视频CPE': str(note_rates['daily_90']['video_cpe'] or 0),
            '90天日常视频+图文-视频CPM': str(note_rates['daily_90']['video_cpm'] or 0),
            '90天日常视频+图文-视频CPR': str(note_rates['daily_90']['video_cpr'] or 0),
            '90天日常视频+视频点赞中位数': str(note_rates['daily_90_likeMedian'] or 0),
            '90天日常视频+视频收藏中位数': str(note_rates['daily_90_collectMedian'] or 0),
            '90天日常视频+视频评论中位数': str(note_rates['daily_90_commentMedian'] or 0),
            '90天日常视频+视频分享中位数': str(note_rates['daily_90_shareMedian'] or 0),
            '90天日常视频+视频关注中位数': str(note_rates['daily_90_mFollowCnt'] or 0),
            '90天日常视频+图文3秒阅读率': str(note_rates['daily_90_picture3sViewRate'] or 0),
            '90天日常视频+图文互动中位数': str(note_rates['daily_90_mEngagementNum'] or 0),
            '90天日常视频+图文互动率': str(note_rates['daily_90_interactionRate'] or 0),
            '90天日常视频+图文千赞笔记比例': str(note_rates['daily_90_thousandLikePercent'] or 0),
            '90天日常视频+图文完播率': str(note_rates['daily_90_videoFullViewRate'] or 0),
            '90天日常视频+图文曝光中位数': str(note_rates['daily_90_imp'] or 0),
            '90天日常视频+图文阅读中位数': str(note_rates['daily_90_read'] or 0),

            # 合作图文+视频数据（30天）
            '合作图文+视频-图文CPC': str(note_rates['coop_30']['pic_text_cpc'] or 0),
            '合作图文+视频-图文CPE': str(note_rates['coop_30']['pic_text_cpe'] or 0),
            '合作图文+视频-图文CPM': str(note_rates['coop_30']['pic_text_cpm'] or 0),
            '合作图文+视频-图文CPR': str(note_rates['coop_30']['pic_text_cpr'] or 0),
            '合作图文+视频-视频CPC': str(note_rates['coop_30']['video_cpc'] or 0),
            '合作图文+视频-视频CPE': str(note_rates['coop_30']['video_cpe'] or 0),
            '合作图文+视频-视频CPM': str(note_rates['coop_30']['video_cpm'] or 0),
            '合作图文+视频-视频CPR': str(note_rates['coop_30']['video_cpr'] or 0),
            '合作图文+视频3秒阅读率': str(note_rates['coop_30_picture3sViewRate'] or 0),
            '合作图文+视频互动中位数': str(note_rates['coop_30_mEngagementNum'] or 0),
            '合作图文+视频互动率': str(note_rates['coop_30_interactionRate'] or 0),
            '合作图文+视频千赞笔记比例': str(note_rates['coop_30_thousandLikePercent'] or 0),
            '合作图文+视频完播率': str(note_rates['coop_30_videoFullViewRate'] or 0),
            '合作图文+视频点赞中位数': str(note_rates['coop_30_likeMedian'] or 0),
            '合作图文+视频收藏中位数': str(note_rates['coop_30_collectMedian'] or 0),
            '合作图文+视频评论中位数': str(note_rates['coop_30_commentMedian'] or 0),
            '合作图文+视频分享中位数': str(note_rates['coop_30_shareMedian'] or 0),
            '合作图文+视频关注中位数': str(note_rates['coop_30_mFollowCnt'] or 0),
            '合作图文+视频曝光中位数': str(note_rates['coop_30_imp'] or 0),
            '合作图文+视频阅读中位数': str(note_rates['coop_30_read'] or 0),

            # 日常图文+视频数据（30天）
            '日常图文+视频-图文CPC': str(note_rates['daily_30']['pic_text_cpc'] or 0),
            '日常图文+视频-图文CPE': str(note_rates['daily_30']['pic_text_cpe'] or 0),
            '日常图文+视频-图文CPM': str(note_rates['daily_30']['pic_text_cpm'] or 0),
            '日常图文+视频-图文CPR': str(note_rates['daily_30']['pic_text_cpr'] or 0),
            '日常图文+视频-视频CPC': str(note_rates['daily_30']['video_cpc'] or 0),
            '日常图文+视频-视频CPM': str(note_rates['daily_30']['video_cpm'] or 0),
            '日常图文+视频-视频CPR': str(note_rates['daily_30']['video_cpr'] or 0),
            '日常图文+视频点赞中位数': str(note_rates['daily_30_likeMedian'] or 0),
            '日常图文+视频收藏中位数': str(note_rates['daily_30_collectMedian'] or 0),
            '日常图文+视频评论中位数': str(note_rates['daily_30_commentMedian'] or 0),
            '日常图文+视频分享中位数': str(note_rates['daily_30_shareMedian'] or 0),
            '日常图文+视频关注中位数': str(note_rates['daily_30_mFollowCnt'] or 0),
            '日常图文+视频3秒阅读率': str(note_rates['daily_30_picture3sViewRate'] or 0),
            '日常图文+视频互动中位数': str(note_rates['daily_30_mEngagementNum'] or 0),
            '日常图文+视频互动率': str(note_rates['daily_30_interactionRate'] or 0),
            '日常图文+视频千赞笔记比例': str(note_rates['daily_30_thousandLikePercent'] or 0),
            '日常图文+视频完播率': str(note_rates['daily_30_videoFullViewRate'] or 0),
            '日常图文+视频曝光中位数': str(note_rates['daily_30_imp'] or 0),
            '日常图文+视频阅读中位数': str(note_rates['daily_30_read'] or 0),
        }
    }
    return formatted_data


def sync_data_to_feishu():
    """同步数据到飞书"""
    logger = setup_logger()
    try:
        # 加载配置文件
        config = load_config()
        logger.info("成功加载配置文件")

        app_token = 'O5uNbHYLdaWQV1sPn8FcdsbSnmb'
        table_id = 'tbl1NdootwK1xnMz'
        view_id = 'vew5rRPpZE'
        # 从配置文件读取设置
        start_id = config.getint('Sync', 'start_id')
        end_id = config.getint('Sync', 'end_id')

        logger.info(f"开始同步数据，ID范围: {start_id} - {end_id}")

        # 获取所有需要同步的数据
        accounts = search_feishu_record(app_token, table_id, view_id, 'id', start_id, end_id)
        if not accounts:
            logger.error("未找到需要同步的记录")
            return

        total_records = len(accounts)
        logger.info(f"共找到 {total_records} 条记录需要同步")

        # 格式化数据并同步到飞书
        success_count = 0
        error_count = 0
        skip_count = 0

        for index, account in enumerate(accounts, 1):
            try:
                user_id = account['fields'].get('博主id')
                if not user_id or not isinstance(user_id, list) or not user_id[0].get('text'):
                    logger.warning(f"记录 {index} 缺少博主ID，跳过")
                    skip_count += 1
                    continue

                user_id = user_id[0]['text']
                record_id = account.get('record_id')
                if not record_id:
                    logger.warning(f"记录 {index} (博主ID: {user_id}) 缺少record_id，跳过")
                    skip_count += 1
                    continue

                # 查询数据库记录
                existing_record = session.query(FpOutBloggerInfo).filter(
                    FpOutBloggerInfo.userId == user_id
                ).first()

                if not existing_record:
                    logger.warning(f"博主ID {user_id} 在数据库中不存在，跳过")
                    skip_count += 1
                    continue

                # 格式化并更新数据
                formatted_data = format_data_for_feishu(existing_record)
                update_record(app_token, table_id, record_id, formatted_data['fields'])
                
                success_count += 1
                logger.info(f"更新记录成功 ({index}/{total_records}): {user_id}")

                # 每更新10条记录输出一次进度
                if success_count % 10 == 0:
                    progress = (index / total_records) * 100
                    logger.info(f"同步进度: {progress:.2f}% ({success_count}/{total_records})")

            except Exception as e:
                error_count += 1
                logger.error(f"处理记录失败 (记录 {index}, 博主ID: {user_id if 'user_id' in locals() else 'unknown'}): {str(e)}")
                continue

        # 输出最终统计信息
        logger.info("同步完成!")
        logger.info(f"总记录数: {total_records}")
        logger.info(f"成功更新: {success_count}")
        logger.info(f"处理失败: {error_count}")
        logger.info(f"跳过记录: {skip_count}")

    except Exception as e:
        logger.error(f"同步数据到飞书时发生错误: {str(e)}")
    finally:
        session.close()
        logger.info("数据库连接已关闭")

def safe_divide(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0.0

def search_feishu_record(app_token, table_id, view_id, field_name, field_value1, field_value2):
    """
    搜索飞书记录
    :param app_token: 应用token
    :param table_id: 表格ID
    :param view_id: 视图ID
    :param field_name: 字段名
    :param field_value1: 起始值
    :param field_value2: 结束值
    :return: 记录列表
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = get_feishu_headers()
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": field_name,
                    "operator": "isGreaterEqual",
                    "value": [field_value1]
                },
                {
                    "field_name": field_name,
                    "operator": "isLessEqual",
                    "value": [field_value2]
                }
            ]
        }
    }

    try:
        # 第一次查询
        response = requests.post(url, headers=headers, json=data, verify=False)
        response.raise_for_status()  # 检查HTTP响应状态
        response_data = response.json()

        # 检查API响应状态
        if response_data.get('code') != 0:
            raise Exception(f"飞书API返回错误: {response_data.get('msg')}")

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
                paginated_response.raise_for_status()
                paginated_data = paginated_response.json()

                if paginated_data.get('code') != 0:
                    raise Exception(f"飞书API返回错误: {paginated_data.get('msg')}")

                # 合并当前页数据
                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                # 如果没有下一页，退出循环
                if not page_token:
                    break

        return items

    except requests.RequestException as e:
        print(f"查询飞书记录失败：{str(e)}")
        return None
    except Exception as e:
        print(f"处理飞书记录时出错：{str(e)}")
        return None

if __name__ == '__main__':
    sync_data_to_feishu() 