import pandas as pd
from datetime import datetime, timedelta
from core.database_text_fangpian import session
from models.models_tibao import DouYinKolRealization, DouYinKolNote, DouyinBianxian
from loguru import logger
import os
from typing import Dict, List, Optional

"""
获取抖音博主的当前月份数据
优化版本：只获取当前月份，并筛选出视频数量为0的记录
"""


class DouYinDataExporter:
    def __init__(self, output_dir: str = "data"):
        """
        初始化导出器
        :param output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def get_current_month_range(self) -> Dict[str, str]:
        """
        获取当前月份的开始和结束日期
        :return: 包含开始和结束日期的字典
        """
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month-1
        
        # 生成当前月的开始和结束日期
        start_date = f"{current_year}-{current_month:02d}-01"
        
        if current_month == 12:
            end_date = f"{current_year + 1}-01-01"
        else:
            end_date = f"{current_year}-{current_month + 1:02d}-01"
        
        return {
            'year': current_year,
            'month': current_month,
            'start_date': start_date,
            'end_date': end_date,
            'month_name': f"{current_year}年{current_month}月"
        }
    
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
    
    def check_zero_video_kols(self, kol_id_range: Optional[tuple] = None) -> List[Dict]:
        """
        检查当前月份视频数量为0的KOL
        :param kol_id_range: KOL ID范围，格式为(min_id, max_id)，None表示全部
        :return: 视频数量为0的KOL列表
        """
        try:
            # 获取当前月份的时间范围
            current_month = self.get_current_month_range()
            logger.info(f"检查 {current_month['month_name']} 的视频数据")
            
            # 获取KOL数据
            query = session.query(DouYinKolRealization)
            if kol_id_range:
                min_id, max_id = kol_id_range
                query = query.filter(DouYinKolRealization.id >= min_id, DouYinKolRealization.id < max_id)
            
            kols = query.all()
            logger.info(f"共找到 {len(kols)} 个KOL记录")
            
            zero_video_kols = []
            total_checked = 0
            
            for i, kol in enumerate(kols, 1):
                try:
                    total_checked += 1
                    
                    # 获取当前月份的视频数量
                    video_count = self.get_monthly_video_count(
                        kol.douyin_user_id, 
                        current_month['start_date'], 
                        current_month['end_date']
                    )
                    
                    # 如果视频数量为0，添加到列表中
                    if video_count == 0:
                        session.query(DouyinBianxian).filter(DouyinBianxian.kol_name == kol.douyin_user_id).update({
                            DouyinBianxian.status: 0
                        })
                        session.commit()
                    
                except Exception as e:
                    logger.error(f"处理KOL {kol.douyin_user_id} 数据时出错: {str(e)}")
                    continue
            
            # 打印统计信息
            print(f"\n📊 检查结果统计:")
            print(f"   总检查KOL数量: {total_checked}")
            print(f"   视频数量为0的KOL数量: {len(zero_video_kols)}")
            print(f"   检查月份: {current_month['month_name']}")
            
            return zero_video_kols
            
        except Exception as e:
            logger.error(f"检查视频数量为0的KOL时发生错误: {str(e)}")
            raise
        finally:
            # 确保关闭数据库会话
            session.close()

def main():
    """主函数 - 检查当前月份视频数量为0的KOL"""
    exporter = DouYinDataExporter()
    
    try:
        # 检查视频数量为0的KOL
        exporter.check_zero_video_kols(
            kol_id_range=None  # None表示全部KOL，或者使用(100, 516)限制范围
        )

        print(f"\n✅ 检查完成!")
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        print(f"\n❌ 检查失败: {str(e)}")


if __name__ == "__main__":
    main()