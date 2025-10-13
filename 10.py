from models.models_tibao import DouyinSearchList, DouYinKolNote, DouYinKolRealization
from core.localhost_fp_project import session
import pandas as pd
import json
from datetime import datetime, timedelta
from sqlalchemy import func, and_, text

def export_douyin_kol_data():
    """导出抖音KOL数据到Excel"""
    try:
        print("开始导出抖音KOL数据...")
        
        # 分页处理 - 避免内存问题
        BATCH_SIZE = 5000
        print(f"使用分页处理，每批 {BATCH_SIZE} 条数据...")
        
        # 先获取总数量
        count_query = "SELECT COUNT(*) FROM douyin_search_list"
        total_count = session.execute(text(count_query)).scalar()
        print(f"总共 {total_count} 条数据，将分 {(total_count + BATCH_SIZE - 1) // BATCH_SIZE} 批处理")
        
        # 准备Excel数据
        excel_data = []
        processed_count = 0
        
        # 计算90天前的日期
        ninety_days_ago = datetime.now() - timedelta(days=90)
        
        # 分页处理
        for offset in range(0, total_count, BATCH_SIZE):
            print(f"正在处理第 {offset//BATCH_SIZE + 1} 批数据 (偏移量: {offset})...")
            
            # 批量获取当前页数据
            query = f"""
            SELECT 
                ds.id,
                ds.star_id,
                ds.attribute_datas
            FROM douyin_search_list ds
            ORDER BY ds.id
            LIMIT {BATCH_SIZE} OFFSET {offset}
            """
            
            result = session.execute(text(query))
            douyin_search_rows = result.fetchall()
            
            if not douyin_search_rows:
                break
                
            # 批量获取当前页的star_id
            star_ids = [row[1] for row in douyin_search_rows if row[1]]
            
            if not star_ids:
                continue
            
            # 批量查询90天商单数
            business_orders_query = """
            SELECT 
                douyin_user_id,
                COUNT(*) as order_count
            FROM douyin_kol_note 
            WHERE douyin_user_id IN :star_ids
            AND duration_min = 1 
            AND douyin_item_date >= :ninety_days_ago
            GROUP BY douyin_user_id
            """
            
            business_result = session.execute(text(business_orders_query), {
                'star_ids': tuple(star_ids),
                'ninety_days_ago': ninety_days_ago.strftime('%Y-%m-%d')
            })
            
            # 创建商单数字典
            business_orders_dict = {row[0]: row[1] for row in business_result.fetchall()}
            
            # 批量查询DouYinKolRealization数据
            realization_query = """
            SELECT 
                douyin_user_id,
                author_base_info,
                self_intro
            FROM douyin_kol_realization 
            WHERE douyin_user_id IN :star_ids
            """
            
            realization_result = session.execute(text(realization_query), {
                'star_ids': tuple(star_ids)
            })
            
            # 创建realization数据字典
            realization_dict = {}
            for row in realization_result.fetchall():
                realization_dict[row[0]] = {
                    'author_base_info': row[1] or '{}',
                    'self_intro': row[2] or ''
                }
            
            # 处理当前批次的数据
            for row in douyin_search_rows:
                star_id = row[1]
                attribute_datas_json = row[2] or '{}'
                
                # 从attribute_datas中解析数据
                douyin_nickname = ''
                follower_count = 0
                price_1_20 = ''
                price_21_60 = ''
                price_60_plus = ''
                
                try:
                    attribute_data = json.loads(attribute_datas_json) if attribute_datas_json else {}
                    douyin_nickname = attribute_data.get('nick_name', '') or ''
                    follower_count = attribute_data.get('follower', 0) or 0
                    price_1_20 = attribute_data.get('price_1_20', '') or ''
                    price_21_60 = attribute_data.get('price_20_60', '') or ''
                    price_60_plus = attribute_data.get('price_60', '') or ''
                except (json.JSONDecodeError, TypeError):
                    pass
                
                # 构建星图链接
                douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{star_id}" if star_id else ''
                
                # 从字典中获取90天商单数
                business_orders_90d = business_orders_dict.get(star_id, '') if business_orders_dict.get(star_id) else ''
                
                # 计算90天GMV (21s-60s)
                gmv_90d = ''
                try:
                    if business_orders_90d and price_21_60:
                        import re
                        price_match = re.search(r'(\d+)', str(price_21_60))
                        if price_match:
                            price_value = int(price_match.group(1))
                            gmv_90d = business_orders_90d * price_value
                except Exception:
                    pass
                
                # 从字典中获取MCN和微信号信息
                mcn_name = ''
                wechat_id = ''
                
                if star_id in realization_dict:
                    realization_data = realization_dict[star_id]
                    wechat_id = realization_data['self_intro']
                    
                    try:
                        author_info = json.loads(realization_data['author_base_info'])
                        mcn_name = author_info.get('mcn_name', '') or ''
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # 添加到Excel数据
                excel_data.append({
                    '昵称': douyin_nickname,
                    '星图ID': star_id,
                    '星图链接': douyin_link,
                    '1-20s视频报价': price_1_20,
                    '21-60s视频报价': price_21_60,
                    '60s+视频报价': price_60_plus,
                    '粉丝数': follower_count,
                    '90天商单数': business_orders_90d,
                    '90天GMV(21s-60s)': gmv_90d,
                    'MCN': mcn_name,
                    '微信号': wechat_id
                })
                
                # 进度显示
                processed_count += 1
                if processed_count % 1000 == 0:
                    print(f"已处理 {processed_count}/{total_count} 条数据...")
        
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
        
        # 统计有粉丝数的KOL（处理字符串和数字类型）
        fans_count = 0
        for x in excel_data:
            fans = x['粉丝数']
            if fans:
                try:
                    if int(fans) > 0:
                        fans_count += 1
                except (ValueError, TypeError):
                    pass
        print(f"有粉丝数的KOL: {fans_count}")
        
        print(f"有90天商单的KOL: {len([x for x in excel_data if x['90天商单数']])}")
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
