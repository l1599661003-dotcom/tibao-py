import pandas as pd
import json
import os
from datetime import datetime
from models.models import DouyinSearchList, DouyinMcnDetail
from core.localhost_fp_project import session
from loguru import logger
import sys

"""
抖音搜索列表数据导出程序
功能：
1. 查询DouyinSearchList表数据
2. 排除在DouyinMcnDetail表中已存在的记录
3. 按照指定字段导出Excel
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
        "logs/export_douyin_search_{time:YYYY-MM-DD}.log",
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
        filename = f"douyin_search_export_{timestamp}.xlsx"
        
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

def query_and_export_data():
    """查询数据并导出Excel"""
    try:
        # 创建数据库会话
        logger.info("开始查询数据库...")

        # 查询DouyinMcnDetail表中已存在的author_id
        existing_author_ids = session.query(DouyinMcnDetail.author_id).distinct().all()
        existing_author_ids_set = {str(author_id[0]) for author_id in existing_author_ids}
        logger.info(f"DouyinMcnDetail表中已存在的author_id数量: {len(existing_author_ids_set)}")

        # 查询DouyinSearchList表数据，排除已存在的记录
        query = session.query(DouyinSearchList).filter(
            ~DouyinSearchList.star_id.in_(existing_author_ids_set)
        )
        
        results = query.all()
        logger.info(f"查询到 {len(results)} 条DouyinSearchList数据")

        if not results:
            logger.warning("没有查询到数据")
            return False

        # 准备Excel数据
        excel_data = []
        
        for record in results:
            try:
                # 解析attribute_datas
                attribute_datas = parse_attribute_datas(record.attribute_datas)
                
                # 解析task_infos
                task_infos = parse_task_infos(record.task_infos)
                
                # 提取各个字段
                row_data = {
                    '抖音昵称': extract_field_value(attribute_datas, 'nick_name'),
                    '达人类型': extract_field_value(attribute_datas, 'tags_relation'),
                    '内容主题': extract_field_value(attribute_datas, 'content_theme_labels_180d'),
                    '星图主页链接': f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{record.star_id}" if record.star_id else '',
                    '星图ID': record.star_id or '',
                    '粉丝(万)': extract_field_value(attribute_datas, 'follower'),
                    '20s视频报价': extract_field_value(attribute_datas, 'price_1_20'),
                    '60s视频报价': extract_field_value(attribute_datas, 'price_20_60'),
                    '60s+视频报价': extract_field_value(attribute_datas, 'price_60'),
                    '种草平台裸价': '',  # 需要从task_infos中提取
                    '所在地区': extract_field_value(attribute_datas, 'city')
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
                logger.error(f"处理记录 {record.id} 时出错: {str(e)}")
                continue

        logger.info(f"成功处理 {len(excel_data)} 条数据")

        if not excel_data:
            logger.warning("没有有效数据可以导出")
            return False

        # 创建DataFrame
        df = pd.DataFrame(excel_data)

        # 添加循环序号列（1-16循环）
        df.insert(0, '序号', [(i % 16) + 1 for i in range(len(df))])

        # 自动生成保存路径
        save_path = generate_export_path()
        if not save_path:
            logger.error("生成保存路径失败，导出取消")
            return False

        # 保存到Excel
        df.to_excel(save_path, index=False, engine='openpyxl')
        logger.info(f"数据已成功导出到: {save_path}")
        logger.info(f"共导出 {len(excel_data)} 条记录")
        print(f"✅ 数据已导出到: {save_path}")
        
        return True

    except Exception as e:
        logger.error(f"查询和导出数据时出错: {str(e)}")
        return False
    finally:
        if session:
            session.close()

def main():
    """主函数"""
    try:
        # 设置日志
        setup_logger()
        
        logger.info("=== 抖音搜索列表数据导出程序启动 ===")
        
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
