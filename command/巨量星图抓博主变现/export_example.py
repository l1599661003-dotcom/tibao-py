#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
抖音KOL数据导出示例
演示如何使用不同的导出配置
"""

from tongbu_douyin import DouYinDataExporter
from loguru import logger
import sys

def example_1_recent_months():
    """示例1: 导出最近几个月的数据"""
    print("=" * 60)
    print("示例1: 导出最近6个月的数据")
    print("=" * 60)
    
    exporter = DouYinDataExporter(output_dir="exports")
    
    try:
        filepath = exporter.export_recent_months(months_count=6)
        print(f"✅ 导出成功: {filepath}")
    except Exception as e:
        print(f"❌ 导出失败: {e}")

def example_2_specific_range():
    """示例2: 导出指定时间范围的数据"""
    print("\n" + "=" * 60)
    print("示例2: 导出2025年3月到7月的数据")
    print("=" * 60)
    
    exporter = DouYinDataExporter(output_dir="exports")
    
    try:
        filepath = exporter.export_kol_revenue_data(
            start_year=2025,
            start_month=3,
            end_year=2025,
            end_month=7
        )
        print(f"✅ 导出成功: {filepath}")
    except Exception as e:
        print(f"❌ 导出失败: {e}")

def example_3_limited_kols():
    """示例3: 导出指定ID范围的KOL数据"""
    print("\n" + "=" * 60)
    print("示例3: 导出ID 100-516 的KOL数据（2025年1-7月）")
    print("=" * 60)
    
    exporter = DouYinDataExporter(output_dir="exports")
    
    try:
        filepath = exporter.export_kol_revenue_data(
            start_year=2025,
            start_month=1,
            end_year=2025,
            end_month=7,
            kol_id_range=(100, 516)
        )
        print(f"✅ 导出成功: {filepath}")
    except Exception as e:
        print(f"❌ 导出失败: {e}")

def example_4_single_month():
    """示例4: 导出单个月的数据"""
    print("\n" + "=" * 60)
    print("示例4: 导出2025年6月的数据")
    print("=" * 60)
    
    exporter = DouYinDataExporter(output_dir="exports")
    
    try:
        filepath = exporter.export_kol_revenue_data(
            start_year=2025,
            start_month=6,
            end_year=2025,
            end_month=6
        )
        print(f"✅ 导出成功: {filepath}")
    except Exception as e:
        print(f"❌ 导出失败: {e}")

def show_menu():
    """显示菜单"""
    print("\n" + "=" * 60)
    print("抖音KOL数据导出工具")
    print("=" * 60)
    print("请选择导出方式:")
    print("1. 导出最近6个月的数据")
    print("2. 导出2025年3月到7月的数据")
    print("3. 导出指定ID范围的KOL数据")
    print("4. 导出单个月的数据")
    print("5. 自定义配置")
    print("0. 退出")
    print("=" * 60)

def custom_export():
    """自定义导出配置"""
    print("\n自定义导出配置:")
    
    try:
        start_year = int(input("请输入起始年份 (例如: 2025): "))
        start_month = int(input("请输入起始月份 (1-12): "))
        end_year = int(input("请输入结束年份 (例如: 2025): "))
        end_month = int(input("请输入结束月份 (1-12): "))
        
        use_range = input("是否限制KOL ID范围? (y/n): ").lower().strip()
        kol_id_range = None
        
        if use_range == 'y':
            min_id = int(input("请输入最小ID: "))
            max_id = int(input("请输入最大ID: "))
            kol_id_range = (min_id, max_id)
        
        output_dir = input("请输入输出目录 (默认: exports): ").strip()
        if not output_dir:
            output_dir = "exports"
        
        print(f"\n开始导出: {start_year}年{start_month}月 到 {end_year}年{end_month}月")
        if kol_id_range:
            print(f"KOL ID范围: {kol_id_range[0]} - {kol_id_range[1]}")
        
        exporter = DouYinDataExporter(output_dir=output_dir)
        filepath = exporter.export_kol_revenue_data(
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            kol_id_range=kol_id_range
        )
        
        print(f"✅ 导出成功: {filepath}")
        
    except ValueError:
        print("❌ 输入格式错误，请输入正确的数字")
    except Exception as e:
        print(f"❌ 导出失败: {e}")

def main():
    """主菜单"""
    while True:
        show_menu()
        
        try:
            choice = input("请输入选项 (0-5): ").strip()
            
            if choice == '0':
                print("👋 再见!")
                sys.exit(0)
            elif choice == '1':
                example_1_recent_months()
            elif choice == '2':
                example_2_specific_range()
            elif choice == '3':
                example_3_limited_kols()
            elif choice == '4':
                example_4_single_month()
            elif choice == '5':
                custom_export()
            else:
                print("❌ 无效选项，请重新选择")
                
        except KeyboardInterrupt:
            print("\n👋 用户取消操作，再见!")
            sys.exit(0)
        except Exception as e:
            print(f"❌ 发生错误: {e}")
        
        input("\n按回车键继续...")

if __name__ == "__main__":
    main()
 