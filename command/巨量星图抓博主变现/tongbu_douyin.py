import pandas as pd
from datetime import datetime, timedelta
from core.database_text_fangpian import session
from models.models_tibao import DouYinKolRealization, DouYinKolNote
from loguru import logger
import os
from typing import Dict, List, Optional

"""
获取抖音博主的月总营收并导出到Excel
优化版本：支持灵活的日期配置和Excel导出
"""


class DouYinDataExporter:
    def __init__(self, output_dir: str = "data"):
        """
        初始化导出器
        :param output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"输出目录: {os.path.abspath(output_dir)}")

    def get_month_name(self, month_number: int) -> str:
        """将月份数字转换为中文月份名"""
        month_names = {
            1: "一月", 2: "二月", 3: "三月", 4: "四月",
            5: "五月", 6: "六月", 7: "七月", 8: "八月",
            9: "九月", 10: "十月", 11: "十一月", 12: "十二月"
        }
        return month_names.get(month_number, f"{month_number}月")

    def generate_date_ranges(self, start_year: int, start_month: int,
                             end_year: int, end_month: int) -> List[Dict[str, any]]:
        """
        根据起始和结束年月生成日期范围列表
        :param start_year: 起始年份
        :param start_month: 起始月份
        :param end_year: 结束年份
        :param end_month: 结束月份
        :return: 日期范围列表
        """
        date_ranges = []
        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            # 生成当月的开始和结束日期
            start_date = f"{current_year}-{current_month:02d}-01"

            if current_month == 12:
                end_date = f"{current_year + 1}-01-01"
                next_year = current_year + 1
                next_month = 1
            else:
                end_date = f"{current_year}-{current_month + 1:02d}-01"
                next_year = current_year
                next_month = current_month + 1

            date_ranges.append({
                'year': current_year,
                'month': current_month,
                'start_date': start_date,
                'end_date': end_date,
                'column_name': f"{current_year}年{self.get_month_name(current_month)}",
                'video_count_column': f"{current_year}年{self.get_month_name(current_month)}视频商单数量",
                'revenue_column': f"{current_year}年{self.get_month_name(current_month)}总营收"
            })

            current_year = next_year
            current_month = next_month

        return date_ranges

    def get_monthly_video_count(self, kol_name: str, start_date: str, end_date: str) -> int:
        """
        获取指定时间范围内的视频数量
        :param kol_name: KOL名称
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 视频数量
        """
        try:
            notes = (session.query(DouYinKolNote)
                     .filter(DouYinKolNote.douyin_user_id == kol_name)
                     .filter(DouYinKolNote.douyin_item_date >= start_date)
                     .filter(DouYinKolNote.douyin_item_date < end_date)
                     .all())

            return len(notes)

        except Exception as e:
            logger.error(f"获取用户 {kol_name} 在 {start_date} 到 {end_date} 的视频数据时出错: {str(e)}")
            return 0

    def export_kol_revenue_data(self, start_year: int = 2025, start_month: int = 1,
                                end_year: Optional[int] = None, end_month: Optional[int] = None,
                                kol_id_range: Optional[tuple] = None) -> str:
        """
        导出KOL营收数据到Excel
        :param start_year: 起始年份
        :param start_month: 起始月份
        :param end_year: 结束年份（默认为当前年份）
        :param end_month: 结束月份（默认为当前月份）
        :param kol_id_range: KOL ID范围，格式为(min_id, max_id)，None表示全部
        :return: 导出文件路径
        """
        try:
            # 设置默认的结束时间为当前月份
            if end_year is None:
                end_year = datetime.now().year
            if end_month is None:
                end_month = datetime.now().month

            logger.info(f"开始导出数据：{start_year}年{start_month}月 到 {end_year}年{end_month}月")

            # 生成日期范围
            date_ranges = self.generate_date_ranges(start_year, start_month, end_year, end_month)
            logger.info(f"生成了 {len(date_ranges)} 个月份的数据列")

            # 获取KOL数据
            query = session.query(DouYinKolRealization)
            if kol_id_range:
                min_id, max_id = kol_id_range
                query = query.filter(DouYinKolRealization.id >= min_id, DouYinKolRealization.id < max_id)

            kols = query.all()
            logger.info(f"共找到 {len(kols)} 个KOL记录")

            # 准备数据列表
            data_list = []
            success_count = 0
            error_count = 0

            for i, kol in enumerate(kols, 1):
                try:
                    logger.info(f"处理第 {i}/{len(kols)} 个KOL: {kol.douyin_user_id}")

                    # 基础数据
                    row_data = {
                        '博主名': kol.douyin_user_id,
                        '星图链接': kol.douyin_link,
                        '1-20秒视频报价': kol.realization1_20 or 0,
                        '21-60秒视频报价': kol.realization21_60 or 0,
                        '60秒+视频报价': kol.realization60 or 0,
                    }

                    # 为每个月份添加数据
                    for date_range in date_ranges:
                        # 获取该月的视频数量
                        video_count = self.get_monthly_video_count(
                            kol.douyin_user_id,
                            date_range['start_date'],
                            date_range['end_date']
                        )

                        # 计算营收（使用60秒+视频报价）
                        monthly_revenue = video_count * (kol.realization60 or 0)

                        # 添加到行数据
                        row_data[date_range['video_count_column']] = video_count
                        row_data[date_range['revenue_column']] = monthly_revenue

                    # 计算总计
                    total_videos = sum(row_data[col] for col in row_data.keys() if '视频商单数量' in col)
                    total_revenue = sum(row_data[col] for col in row_data.keys() if '总营收' in col)

                    row_data['总视频商单数量'] = total_videos
                    row_data['总营收'] = total_revenue

                    data_list.append(row_data)
                    success_count += 1

                except Exception as e:
                    error_count += 1
                    logger.error(f"处理KOL {kol.douyin_user_id} 数据时出错: {str(e)}")
                    continue

            # 创建DataFrame
            df = pd.DataFrame(data_list)

            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"抖音KOL营收数据_{start_year}年{start_month}月到{end_year}年{end_month}月_{timestamp}.xlsx"
            filepath = os.path.join(self.output_dir, filename)

            # 导出到Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 主数据表
                df.to_excel(writer, sheet_name='KOL营收数据', index=False)

            logger.info(f"数据导出完成！")
            logger.info(f"文件路径: {os.path.abspath(filepath)}")
            logger.info(f"处理统计: 成功 {success_count} 条，失败 {error_count} 条")

            return filepath

        except Exception as e:
            logger.error(f"导出数据时发生错误: {str(e)}")
            raise
        finally:
            # 确保关闭数据库会话
            session.close()

    def export_recent_months(self, months_count: int = 6, kol_id_range: Optional[tuple] = None) -> str:
        """
        导出最近几个月的数据
        :param months_count: 最近几个月的数量
        :param kol_id_range: KOL ID范围
        :return: 导出文件路径
        """
        current_date = datetime.now()

        # 计算起始月份
        start_date = current_date - timedelta(days=30 * months_count)
        start_year = start_date.year
        start_month = start_date.month

        end_year = current_date.year
        end_month = current_date.month

        logger.info(f"导出最近 {months_count} 个月的数据")
        return self.export_kol_revenue_data(start_year, start_month, end_year, end_month, kol_id_range)


def main():
    """主函数 - 可配置的数据导出"""
    exporter = DouYinDataExporter()

    try:
        # 配置选项 - 可以根据需要修改这些参数

        # 选项1: 导出指定时间范围的数据
        # filepath = exporter.export_kol_revenue_data(
        #     start_year=2025,
        #     start_month=1,
        #     end_year=2025,
        #     end_month=7,
        #     kol_id_range=None  # None表示全部KOL，或者使用(100, 516)限制范围
        # )

        # 选项2: 导出最近6个月的数据
        filepath = exporter.export_recent_months(
            months_count=6,
            kol_id_range=None
        )

        print(f"\n✅ 导出成功!")
        print(f"📁 文件位置: {filepath}")
        print(f"📊 请查看Excel文件中的三个工作表:")
        print(f"   - KOL营收数据: 详细的每个KOL每月数据")
        print(f"   - 数据汇总: 整体统计信息")
        print(f"   - 月度汇总: 按月份的汇总数据")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        print(f"\n❌ 导出失败: {str(e)}")


if __name__ == "__main__":
    main()