from models.models_tibao import DouyinSearchList, DouYinKolNote, DouYinKolRealization
from core.database_text_tibao_2 import session
import pandas as pd
import json
from datetime import datetime, timedelta
from sqlalchemy import func, and_, text

def export_douyin_kol_data():
    """导出抖音KOL数据到Excel"""
    try:
        print("开始导出抖音KOL数据...")
        
        # 使用SQLAlchemy ORM查询基础数据
        query = session.query(
            DouyinSearchList.star_id,
            DouYinKolRealization.douyin_nickname,
            DouYinKolRealization.douyin_link,
            DouYinKolRealization.price_info,
            DouYinKolRealization.follower_count,
            DouYinKolRealization.author_base_info,
            DouYinKolRealization.self_intro
        ).outerjoin(
            DouYinKolRealization, 
            DouyinSearchList.star_id == DouYinKolRealization.douyin_user_id
        ).filter(
            DouyinSearchList.status == 0,
            DouyinSearchList.category is not None,
        ).order_by(DouyinSearchList.id)
        
        rows = query.all()
        
        print(f"查询到 {len(rows)} 条KOL数据")
        
        # 准备Excel数据
        excel_data = []
        
        for row in rows:
            star_id = row[0]
            douyin_nickname = row[1] or ''
            douyin_link = row[2] or ''
            price_info_json = row[3] or '[]'
            follower_count = row[4] or 0
            author_base_info_json = row[5] or '{}'
            self_intro = row[6] or ''
            
            # 解析价格信息
            price_1_20 = ''
            price_21_60 = ''
            price_60_plus = ''
            price_short_direct = ''

            try:
                price_info = json.loads(price_info_json) if price_info_json else []
                if isinstance(price_info, list) and len(price_info) > 0:
                    # 取前三个价格信息
                    for i, price_item in enumerate(price_info[:4]):
                        if isinstance(price_item, dict):
                            price_value = price_item.get('price', 0)
                            desc = price_item.get('desc', '')
                            
                            if i == 0:
                                price_1_20 = f"{price_value}"
                            elif i == 1:
                                price_21_60 = f"{price_value}"
                            elif i == 2:
                                price_60_plus = f"{price_value}"
                            elif i == 3:
                                price_short_direct = f"{price_value}"
            except (json.JSONDecodeError, TypeError) as e:
                print(f"解析价格信息失败 (star_id: {star_id}): {str(e)}")
            
            # 统计90天商单数
            business_orders_90d = 0
            try:
                # 计算90天前的日期
                ninety_days_ago = datetime.now() - timedelta(days=90)
                
                # 使用SQLAlchemy ORM查询
                business_count = session.query(DouYinKolNote).filter(
                    DouYinKolNote.douyin_user_id == star_id,
                    DouYinKolNote.duration_min == 1,
                    DouYinKolNote.douyin_item_date >= ninety_days_ago.strftime('%Y-%m-%d')
                ).count()
                
                business_orders_90d = business_count or 0
                
            except Exception as e:
                print(f"统计90天商单数失败 (star_id: {star_id}): {str(e)}")
            
            # 计算90天GMV (21s-60s)
            gmv_90d = 0
            try:
                if price_21_60:
                    # 从价格字符串中提取数字
                    import re
                    price_match = re.search(r'(\d+)', price_21_60)
                    if price_match:
                        price_value = int(price_match.group(1))
                        gmv_90d = business_orders_90d * price_value
            except Exception as e:
                print(f"计算GMV失败 (star_id: {star_id}): {str(e)}")
            
            # 提取MCN信息
            mcn_name = ''
            city = ''
            gender = ''
            tags_str = ''
            tags_relation = ''
            try:
                author_info = json.loads(author_base_info_json) if author_base_info_json else {}
                mcn_name = author_info.get('mcn_name', '') or ''
                city = author_info.get('city', '') or ''
                gender = author_info.get('gender', '') or ''
                tags = author_info.get('content_theme_labels', '') or ''
                tags_relation = author_info.get('tags_relation', '') or ''
                for tag in tags:
                    tags_str += tag + '、'
                if gender == 1:
                    gender = '男'
                elif gender == 2:
                    gender = '女'
            except (json.JSONDecodeError, TypeError):
                pass
            
            # 添加到Excel数据
            excel_data.append({
                '抖音昵称': douyin_nickname,
                '达人类型': tags_relation,
                '内容主题': tags_str,
                '星图主页链接': douyin_link,
                '星图ID': star_id,
                '粉丝（万）': follower_count,
                '1-20s视频报价': price_1_20,
                '21-60s视频报价': price_21_60,
                '60s+视频报价': price_60_plus,
                '短直种草平台裸价': price_short_direct,
                '性别': gender,
                '所在地区': city,
                '博主机构': mcn_name,
            })
        
        # 创建DataFrame并导出Excel
        df = pd.DataFrame(excel_data)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'抖音KOL数据报表_{timestamp}.xlsx'
        
        # 导出Excel
        df.to_excel(filename, index=False, engine='openpyxl')
        
        print(f"✅ Excel文件已生成: {filename}")
        print(f"📊 共导出 {len(excel_data)} 条数据")
        
        # 显示统计信息
        print("\n📈 数据统计:")
        print(f"总KOL数量: {len(excel_data)}")
        print(f"有粉丝数的KOL: {len([x for x in excel_data if x['粉丝数'] > 0])}")
        print(f"有90天商单的KOL: {len([x for x in excel_data if x['90天商单数'] > 0])}")
        print(f"有MCN信息的KOL: {len([x for x in excel_data if x['MCN']])}")
        print(f"有微信号的KOL: {len([x for x in excel_data if x['微信号']])}")
        
        return True
        
    except Exception as e:
        print(f"❌ 导出失败: {str(e)}")
        import traceback
        print(f"错误详情: {traceback.format_exc()}")
        return False
    finally:
        session.close()

if __name__ == "__main__":
    export_douyin_kol_data()
