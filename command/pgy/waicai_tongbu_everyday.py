import json
import configparser
import logging
from pathlib import Path
from datetime import datetime

import requests

from core.database_text_tibao_2 import session
from models.models_tibao import ChinaProvince, ChinaCity, FpOutBloggerInfo
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

def format_data_for_feishu(info):
    """格式化数据为飞书格式"""
    try:
        # 基本信息
        note_sign = None
        noteSign = json.loads(info.noteSign or "{}")
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
                '是否抓取': datetime.now().strftime("%Y%m%d") + "已抓完"
            }
        }
        return formatted_data

    except Exception as e:
        print(f"格式化数据时出错: {str(e)}")
        raise


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