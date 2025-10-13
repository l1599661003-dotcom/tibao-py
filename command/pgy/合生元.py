import configparser
import logging
from pathlib import Path
from datetime import datetime, timedelta
import time
import traceback
from service.feishu_service import read_table_content, update_record
from service.pgy_service import get_fans_overall_new_history, get_fans_profile, get_notes_detail, \
    get_notes_rate, get_blogger_info
from command.pgy.waicai_tongbu_feishu import safe_divide

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


def calculate_fans_growth_trend(fans_history, point_count=7):
    """
        判断粉丝数是否呈上升趋势（从 fans_data 中均匀抽取 point_count 个点进行判断）
    """
    data_len = len(fans_history)
    if data_len < 2:
        return "否"

    # 取关键点索引（等间隔）
    indices = [round(i * (data_len - 1) / (point_count - 1)) for i in range(point_count)]

    key_points = [fans_history[i]['num'] for i in indices]

    # 判断是否严格上升或持平
    for i in range(len(key_points) - 1):
        if key_points[i] > key_points[i + 1]:
            return "否"  # 出现下降 → 非上升趋势

    return "是"

def main():
    logger = setup_logger()
    try:
        # 加载配置文件
        config = load_config()
        logger.info("成功加载配置文件")

        # 从配置文件读取设置
        app_token = config.get('Feishu1', 'app_token')
        table_id = config.get('Feishu1', 'table_id')
        view_id = config.get('Feishu1', 'view_id')
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

        for record_index, record in enumerate(records, 1):
            try:
                record_id = record.get("record_id")
                fields = record.get("fields", {})
                # 获取博主ID
                if not fields.get("博主id") or not fields.get("博主id").get('value'):
                    logger.warning(f"记录{record_id}无博主ID，跳过")
                    continue
                
                user_id = fields.get("博主id")['value'][0]['text']
                if not user_id:
                    logger.warning(f"记录{record_id}无博主ID，跳过")
                    continue

                logger.info(f"处理博主ID: {user_id} ({record_index}/{total_records})")
                
                # 获取博主信息
                blogger_info = get_blogger_info(user_id, headers)
                if not blogger_info:
                    logger.warning(f"无法获取博主信息: {user_id}，跳过")
                    continue

                content_tags = blogger_info.get("contentTags", [])
                personal_tags = blogger_info.get("personalTags", [])
                tag_list = []
                for tag in content_tags:
                    if "taxonomy1Tag" in tag:
                        tag_list.append(tag["taxonomy1Tag"])
                    if "taxonomy2Tags" in tag and isinstance(tag["taxonomy2Tags"], list):
                        tag_list.extend(tag["taxonomy2Tags"])
                tag_string = "、".join(tag_list)
                time.sleep(6)

                bloggers = get_notes_detail(user_id, headers)
                note_index = 1
                notes_total = len(bloggers) if bloggers else 0
                last_publish_date = None
                now = datetime.now()
                one_months_ago = now - timedelta(days=240)
                brands_1_months = set()

                brand_set = set()
                brand_date_map = set()
                video_total = 0  # 视频笔记总数
                video_totals = 0  # 视频笔记总数
                total_notes_count = 0  # 所有笔记总数
                collect_total = 0
                collect = 0
                coop_name = ''
                he_sheng_yuan_dates = []

                if bloggers:
                    for blogger in bloggers:
                        is_video = blogger.get('isVideo', False)
                        brandName = blogger.get("brandName")
                        date_str = blogger.get('date')
                        like_num = blogger.get('likeNum', 0)
                        collectNum = blogger.get('collectNum', 0)
                        total_notes_count += 1

                        if not date_str:
                            continue
                        try:
                            note_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        except:
                            continue
                        # 笔记最后发布时间
                        if not last_publish_date or note_date > last_publish_date:
                            last_publish_date = note_date
                        # 计算视频总占比
                        if is_video:
                            video_totals +=1
                            # 前8页视频笔记收藏数据
                            if note_index <= 8:
                                collect_total += collectNum
                                collect += 1

                            # 前16页视频笔记占比计数
                            if note_index <= 16:
                                video_total += 1
                                note_index += 1

                        if note_date >= one_months_ago.date() and brandName:
                            if "合生元" in brandName:
                                he_sheng_yuan_dates.append(str(note_date))
                        if brandName:
                            brand_set.add(brandName)
                            try:
                                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                date_str = f"{date_obj.month}月{date_obj.day}日"
                                brandName_str = f"{brandName}:{date_str}"
                                brand_date_map.add(brandName_str)
                            except:
                                logger.warning(f"日期格式错误: {date_str}")

                # 计算视频占比
                video_ratio = safe_divide(video_totals, total_notes_count)

                # 生成逗号拼接字段
                if he_sheng_yuan_dates:
                    coop_name = f"合生元 + {','.join(he_sheng_yuan_dates)}"

                # 构造"已合作品牌"格式
                brand_list = list(brand_set)
                brand_names_str = "、".join([f"{name}" for i, name in enumerate(brand_list)])

                # 构造"品牌合作日期"格式
                brand_date_list = list(brand_date_map)
                brand_dates_str = "、".join([f"{name}" for i, name in enumerate(brand_date_list)])
                time.sleep(6)

                # 获取各类数据
                notes_rates = get_notes_rate(user_id, 1, 3, 1, 1, headers) or {}
                time.sleep(6)
                notes_rate = get_notes_rate(user_id, 1, 1, 1, 1, headers) or {}
                time.sleep(6)
                shipin_notes_rate = get_notes_rate(user_id, 1, 2, 1, 1, headers) or {}
                time.sleep(6)
                notes_rate_90 = get_notes_rate(user_id, 1, 1, 2, 1, headers) or {}
                time.sleep(6)
                shipin_notes_rate_90 = get_notes_rate(user_id, 1, 2, 2, 1, headers) or {}
                time.sleep(6)
                fans_profile = get_fans_profile(user_id, headers) or {}
                time.sleep(6)
                fans_overall_new_historys = get_fans_overall_new_history(user_id, headers) or {'list': []}
                time.sleep(6)
                
                # 粉丝是否增长
                fans_growth_trend = "否"
                if fans_overall_new_historys and 'list' in fans_overall_new_historys and fans_overall_new_historys['list']:
                    fans_growth_trend = calculate_fans_growth_trend(fans_overall_new_historys['list'])
                
                # 健壮性处理：所有字段访问前都加类型和空值判断，防止 NoneType 报错
                tag_string = tag_string if isinstance(tag_string, str) else ''

                # 年龄分布健壮性
                def get_age_percent(ages, idx):
                    if isinstance(ages, list) and len(ages) > idx and isinstance(ages[idx], dict):
                        return round(float(ages[idx].get('percent', 0.0)) * 100, 2)
                    return 0.0

                ages = fans_profile.get('ages', []) if isinstance(fans_profile, dict) else []
                fans = blogger_info.get("fansCount") or 0

                # 根据视频占比确定建议合作类型
                suggested_coop_type = "报备视频" if video_ratio > 0.5 else "报备图文"
                
                # 根据视频占比确定互动成本计算方式
                interaction_cost = 0
                if video_ratio > 0.5:
                    # 使用视频价格计算互动成本
                    video_price = blogger_info.get("videoPrice") or 0
                    engagement_num = notes_rates.get("mEngagementNum") or 1  # 防止除零
                    interaction_cost = safe_divide(video_price, engagement_num)
                else:
                    # 使用图文价格计算互动成本
                    picture_price = blogger_info.get("picturePrice") or 0
                    engagement_num = notes_rates.get("mEngagementNum") or 1  # 防止除零
                    interaction_cost = safe_divide(picture_price, engagement_num)

                # 安全获取数据
                read_median = notes_rates.get("readMedian", 0)
                imp_median = notes_rates.get("impMedian", 0) or 1  # 防止除零
                read_cost = safe_divide(read_median, imp_median)

                # 格式化数值为保留两位小数的字符串
                def format_number(value):
                    try:
                        return f"{float(value):.2f}"
                    except (ValueError, TypeError):
                        return "0.00"
                
                update_fields = {
                    # blogger_info接口
                    "达人量级": ("头部" if fans >= 500000 else "腰部" if fans > 50000 else "底部"),
                    "KOL_Name": str(blogger_info.get("name") or ""),
                    'URL': f"https://www.xiaohongshu.com/user/profile/{user_id}",
                    "Fans": f"{(blogger_info.get('fansCount') or 0) / 10000:.2f}",
                    "赞藏": f"{(blogger_info.get('likeCollectCountInfo') or 0) / 10000:.2f}",
                    "类型": tag_string,
                    "建议合作类型": suggested_coop_type,
                    "25-34岁粉丝占比": f"{get_age_percent(ages, 2):.2f}%",
                    '图文合作30天3s阅读率': format_number(notes_rate.get("picture3sViewRate", 0)),
                    '视频合作30天完播率': format_number(shipin_notes_rate.get("videoFullViewRate", 0)),
                    '图文合作近90天互动率': format_number(notes_rate_90.get("interactionRate", 0)),
                    '视频合作近90天互动率': format_number(shipin_notes_rate_90.get("interactionRate", 0)),
                    '阅读成本': format_number(read_cost),
                    '互动成本': format_number(interaction_cost),
                    '阅读中位数合作30天': format_number(read_median),
                    '互动中位数合作30天': format_number(notes_rates.get("mEngagementNum", 0)),
                    '图文报备裸价': format_number(blogger_info.get("picturePrice") or 0),
                    '视频报备裸价': format_number(blogger_info.get("videoPrice") or 0),
                    '前16篇笔记视频占比': f"{safe_divide(video_total, min(16, total_notes_count)):.2f}",
                    '粉丝量-近30天呈上涨趋势': fans_growth_trend,
                    "最近更新时间": last_publish_date.strftime('%Y-%m-%d') if last_publish_date else '',
                    '蒲公英第一页笔记收藏中位数': format_number(safe_divide(collect_total, collect)),
                    '30天内是否接过合生元': coop_name,
                    '已合作品牌': str(brand_names_str),
                    '已合作品牌及日期': str(brand_dates_str),
                }

                update_record(app_token, table_id, record_id, update_fields)
                success_count += 1

                # 每处理10条记录输出一次进度
                if success_count % 10 == 0:
                    progress = (record_index / total_records) * 100
                    logger.info(f"同步进度: {progress:.2f}% ({success_count}/{total_records})")

            except Exception as e:
                error_count += 1
                logger.error(f"处理记录失败 (记录 {record_index}, 博主ID: {user_id if 'user_id' in locals() else 'unknown'}): {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                continue

        # 输出最终统计信息
        logger.info("数据更新完成!")
        logger.info(f"总记录数: {total_records}")
        logger.info(f"成功更新: {success_count}")
        logger.info(f"处理失败: {error_count}")

    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")

if __name__ == '__main__':
    main()