import pandas as pd
import json
import os
from datetime import datetime
from models.models import DouyinSearchList, DouyinMcnDetail, DouyinMcn
from core.localhost_fp_project import session
from loguru import logger
from sqlalchemy import and_, or_
import sys

"""
抖音搜索列表与MCN详情匹配数据导出程序
功能：
1. 导出DouyinSearchList与DouyinMcnDetail都存在的博主
2. 导出DouyinMcnDetail中存在但DouyinSearchList中不存在的博主
3. 根据mcn_id关联DouyinMcn表，带出MCN名字
"""

def setup_logger():
    """设置日志配置"""
    # 移除默认处理器，避免重复输出
    logger.remove()

    # 添加控制台输出
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

    # 添加文件输出
    logger.add(
        "logs/export_douyin_matched_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )

def generate_export_path():
    """自动生成导出路径"""
    try:
        # 获取当前脚本所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 创建导出目录
        export_dir = os.path.join(current_dir, 'exports')
        os.makedirs(export_dir, exist_ok=True)

        # 生成文件名（包含时间戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"douyin_matched_export_{timestamp}.xlsx"

        # 完整路径
        file_path = os.path.join(export_dir, filename)

        logger.info(f"自动生成导出路径: {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"生成导出路径时出错: {str(e)}")
        return None

def parse_attribute_datas(attribute_datas_str):
    """解析attribute_datas字段"""
    try:
        if not attribute_datas_str:
            return {}

        # 尝试解析JSON
        if isinstance(attribute_datas_str, str):
            return json.loads(attribute_datas_str)
        elif isinstance(attribute_datas_str, dict):
            return attribute_datas_str
        else:
            return {}
    except Exception as e:
        logger.warning(f"解析attribute_datas失败: {str(e)}")
        return {}

def parse_task_infos(task_infos_str):
    """解析task_infos字段"""
    try:
        if not task_infos_str:
            return {}

        # 尝试解析JSON
        if isinstance(task_infos_str, str):
            return json.loads(task_infos_str)
        elif isinstance(task_infos_str, dict):
            return task_infos_str
        else:
            return {}
    except Exception as e:
        logger.warning(f"解析task_infos失败: {str(e)}")
        return {}

def extract_field_value(data, field_path):
    """从嵌套数据中提取字段值"""
    try:
        keys = field_path.split('.')
        value = data

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return ''

        return str(value) if value is not None else ''
    except Exception as e:
        return ''

def get_mcn_name_dict():
    """获取MCN ID到名称的映射字典"""
    try:
        mcn_list = session.query(DouyinMcn.user_id, DouyinMcn.introduction).all()
        mcn_dict = {str(mcn.user_id): mcn.introduction or '' for mcn in mcn_list}
        logger.info(f"获取到 {len(mcn_dict)} 个MCN机构")
        return mcn_dict
    except Exception as e:
        logger.error(f"获取MCN名称映射失败: {str(e)}")
        return {}

def query_and_export_data():
    """查询数据并导出Excel"""
    try:
        # 创建数据库会话
        logger.info("开始查询数据库...")

        # 获取MCN名称映射
        mcn_name_dict = get_mcn_name_dict()

        # 1. 查询DouyinMcnDetail表中所有的author_id
        mcn_detail_records = session.query(DouyinMcnDetail).all()
        mcn_detail_dict = {str(record.author_id): record for record in mcn_detail_records}
        logger.info(f"DouyinMcnDetail表中共有 {len(mcn_detail_dict)} 条记录")

        # 2. 查询DouyinSearchList表中所有的star_id
        search_list_records = session.query(DouyinSearchList).all()
        search_list_dict = {str(record.star_id): record for record in search_list_records if record.star_id}
        logger.info(f"DouyinSearchList表中共有 {len(search_list_dict)} 条记录")

        # 3. 找出两个表都存在的博主
        matched_ids = set(mcn_detail_dict.keys()) & set(search_list_dict.keys())
        logger.info(f"两个表都存在的博主数量: {len(matched_ids)}")

        # 4. 找出只在DouyinMcnDetail表中存在的博主
        mcn_only_ids = set(mcn_detail_dict.keys()) - set(search_list_dict.keys())
        logger.info(f"只在DouyinMcnDetail表中存在的博主数量: {len(mcn_only_ids)}")

        # 准备Excel数据
        excel_data = []

        # 处理两个表都存在的博主（有完整的attribute_datas信息）
        logger.info("处理两个表都存在的博主...")
        for author_id in matched_ids:
            try:
                search_record = search_list_dict[author_id]
                mcn_detail_record = mcn_detail_dict[author_id]

                # 解析attribute_datas
                attribute_datas = parse_attribute_datas(search_record.attribute_datas)

                # 解析task_infos
                task_infos = parse_task_infos(search_record.task_infos)

                # 获取MCN名称
                mcn_id = str(mcn_detail_record.mcn_id) if mcn_detail_record.mcn_id else ''
                mcn_name = mcn_name_dict.get(mcn_id, '')

                # 提取各个字段
                row_data = {
                    '数据来源': '两表都有',
                    'MCN名称': mcn_name,
                    'MCN ID': mcn_id,
                    '抖音昵称': extract_field_value(attribute_datas, 'nick_name') or mcn_detail_record.nick_name or '',
                    '达人类型': extract_field_value(attribute_datas, 'tags_relation'),
                    '内容主题': extract_field_value(attribute_datas, 'content_theme_labels_180d'),
                    '星图主页链接': f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{author_id}",
                    '星图ID': author_id,
                    '粉丝(万)': extract_field_value(attribute_datas, 'follower') or str(mcn_detail_record.sum_follower) if mcn_detail_record.sum_follower else '',
                    '20s视频报价': extract_field_value(attribute_datas, 'price_1_20'),
                    '60s视频报价': extract_field_value(attribute_datas, 'price_20_60'),
                    '60s+视频报价': extract_field_value(attribute_datas, 'price_60'),
                    '种草平台裸价': '',
                    '性别': extract_field_value(attribute_datas, 'gender'),
                    '所在地区': extract_field_value(attribute_datas, 'city'),
                    '标签': mcn_detail_record.tags or ''
                }

                # 提取种草平台裸价
                if 'price_infos' in task_infos:
                    price_infos = task_infos['price_infos']
                    if isinstance(price_infos, list):
                        for price_info in price_infos:
                            if isinstance(price_info, dict) and price_info.get('video_type') == 150:
                                row_data['种草平台裸价'] = str(price_info.get('price', ''))
                                break

                excel_data.append(row_data)

            except Exception as e:
                logger.error(f"处理两表都有的记录 {author_id} 时出错: {str(e)}")
                continue

        # 处理只在DouyinMcnDetail表中存在的博主（没有attribute_datas信息）
        logger.info("处理只在DouyinMcnDetail表中存在的博主...")
        for author_id in mcn_only_ids:
            try:
                mcn_detail_record = mcn_detail_dict[author_id]

                # 获取MCN名称
                mcn_id = str(mcn_detail_record.mcn_id) if mcn_detail_record.mcn_id else ''
                mcn_name = mcn_name_dict.get(mcn_id, '')

                # 提取各个字段（只能从DouyinMcnDetail表中获取）
                row_data = {
                    '数据来源': '仅MCN表',
                    'MCN名称': mcn_name,
                    'MCN ID': mcn_id,
                    '抖音昵称': mcn_detail_record.nick_name or '',
                    '达人类型': '',
                    '内容主题': '',
                    '星图主页链接': f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{author_id}",
                    '星图ID': author_id,
                    '粉丝(万)': str(mcn_detail_record.sum_follower) if mcn_detail_record.sum_follower else '',
                    '20s视频报价': '',
                    '60s视频报价': '',
                    '60s+视频报价': '',
                    '种草平台裸价': '',
                    '性别': '',
                    '所在地区': '',
                    '标签': mcn_detail_record.tags or ''
                }

                excel_data.append(row_data)

            except Exception as e:
                logger.error(f"处理仅MCN表的记录 {author_id} 时出错: {str(e)}")
                continue

        logger.info(f"成功处理 {len(excel_data)} 条数据")

        if not excel_data:
            logger.warning("没有有效数据可以导出")
            return False

        # 创建DataFrame
        df = pd.DataFrame(excel_data)

        # 按数据来源排序，两表都有的在前面
        df = df.sort_values(by=['数据来源', 'MCN名称', '粉丝(万)'], ascending=[True, True, False])

        # 自动生成保存路径
        save_path = generate_export_path()
        if not save_path:
            logger.error("生成保存路径失败，导出取消")
            return False

        # 保存到Excel
        df.to_excel(save_path, index=False, engine='openpyxl')
        logger.info(f"数据已成功导出到: {save_path}")
        logger.info(f"共导出 {len(excel_data)} 条记录")
        logger.info(f"其中两表都有的记录: {len(matched_ids)} 条")
        logger.info(f"其中仅MCN表有的记录: {len(mcn_only_ids)} 条")
        print(f"✅ 数据已导出到: {save_path}")
        print(f"✅ 共导出 {len(excel_data)} 条记录")
        print(f"   - 两表都有: {len(matched_ids)} 条")
        print(f"   - 仅MCN表: {len(mcn_only_ids)} 条")

        return True

    except Exception as e:
        logger.error(f"查询和导出数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if session:
            session.close()

def main():
    """主函数"""
    try:
        # 设置日志
        setup_logger()

        logger.info("=== 抖音匹配数据导出程序启动 ===")

        # 执行查询和导出
        success = query_and_export_data()

        if success:
            logger.info("✅ 数据导出成功完成")
            print("✅ 数据导出成功完成")
        else:
            logger.error("❌ 数据导出失败")
            print("❌ 数据导出失败")

        return success

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        print(f"❌ 程序运行出错: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        print(f"程序启动失败: {str(e)}")
        sys.exit(1)
