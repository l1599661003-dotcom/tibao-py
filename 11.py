from models.models_tibao import DouyinSearchList
from core.localhost_fp_project import session
import pandas as pd
import json
from datetime import datetime

def export_douyin_kol_data():
    """导出抖音KOL数据到Excel"""
    try:
        print("开始导出抖音KOL数据...")
        
        # 使用SQLAlchemy ORM查询DouyinSearchList表数据
        query = session.query(DouyinSearchList).filter(
            DouyinSearchList.import_status == 0,
            DouyinSearchList.category.isnot(None)
        ).order_by(DouyinSearchList.id)
        
        rows = query.all()
        
        print(f"查询到 {len(rows)} 条KOL数据")
        
        # 按category分组数据
        category_data = {}
        
        for row in rows:
            category = row.category or '未知分类'
            star_id = row.star_id
            
            # 解析attribute_datas
            douyin_nickname = ''
            tags_relation = ''
            content_theme_labels = ''
            follower_count = 0
            price_1_20 = ''
            price_20_60 = ''
            price_60 = ''
            gender = ''
            city = ''

            try:
                attribute_datas = json.loads(row.attribute_datas) if row.attribute_datas else {}
                douyin_nickname = attribute_datas.get('nick_name', '') or ''
                tags_relation = attribute_datas.get('tags_relation', '') or ''
                follower_count = attribute_datas.get('follower', 0) or 0
                price_1_20 = attribute_datas.get('price_1_20', '') or ''
                price_20_60 = attribute_datas.get('price_20_60', '') or ''
                price_60 = attribute_datas.get('price_60', '') or ''
                gender = attribute_datas.get('gender', '') or ''
                city = attribute_datas.get('city', '') or ''
                
                # 处理性别显示
                if gender == '1':
                    gender = '男'
                elif gender == '2':
                    gender = '女'
                else:
                    gender = ''
                    
            except (json.JSONDecodeError, TypeError) as e:
                print(f"解析attribute_datas失败 (star_id: {star_id}): {str(e)}")
            
            # 解析content_theme_labels_180d
            content_theme_labels_str = ''
            try:
                content_theme_labels = attribute_datas.get('content_theme_labels_180d', '')
                if content_theme_labels:
                    content_theme_labels_list = json.loads(content_theme_labels) if isinstance(content_theme_labels, str) else content_theme_labels
                    if isinstance(content_theme_labels_list, list):
                        content_theme_labels_str = '、'.join(content_theme_labels_list)
            except (json.JSONDecodeError, TypeError):
                pass
            
            # 解析task_infos获取短直种草平台裸价
            price_short_direct = ''
            try:
                task_infos = json.loads(row.task_infos) if row.task_infos else []
                if isinstance(task_infos, list) and len(task_infos) > 0:
                    # task_infos是一个数组，取第一个元素的price_infos
                    first_task = task_infos[0]
                    price_infos = first_task.get('price_infos', [])
                    if isinstance(price_infos, list):
                        for price_info in price_infos:
                            if isinstance(price_info, dict) and price_info.get('video_type') == 150:
                                price_short_direct = price_info.get('price', '') or ''
                                break
            except (json.JSONDecodeError, TypeError):
                pass
            
            # 构建星图主页链接
            douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{star_id}"
            
            # 添加到对应category的数据中
            if category not in category_data:
                category_data[category] = []
            
            category_data[category].append({
                '抖音昵称': douyin_nickname,
                '达人类型': tags_relation,
                '内容主题': content_theme_labels_str,
                '星图主页链接': douyin_link,
                '星图ID': star_id,
                '粉丝（万）': follower_count,
                '1-20s视频报价': price_1_20,
                '21-60s视频报价': price_20_60,
                '60s+视频报价': price_60,
                '短直种草平台裸价': price_short_direct,
                '性别': gender,
                '所在地区': city,
            })
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'抖音KOL数据报表_{timestamp}.xlsx'
        
        # 使用ExcelWriter创建多sheet的Excel文件
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            total_records = 0
            for category, data_list in category_data.items():
                if data_list:  # 只处理有数据的category
                    # 创建DataFrame
                    df = pd.DataFrame(data_list)
                    
                    # 清理sheet名称（Excel sheet名称不能包含特殊字符）
                    sheet_name = str(category).replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace('[', '_').replace(']', '_').replace(':', '_')
                    # 限制sheet名称长度（Excel限制31个字符）
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # 写入到对应的sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    total_records += len(data_list)
                    print(f"📊 {category}: {len(data_list)} 条数据")
        
        print(f"✅ Excel文件已生成: {filename}")
        print(f"📊 共导出 {total_records} 条数据，分为 {len([k for k, v in category_data.items() if v])} 个分类")
        
        # 更新import_status字段为1
        print("🔄 更新import_status字段...")
        try:
            # 获取所有导出的star_id
            exported_star_ids = []
            for data_list in category_data.values():
                for item in data_list:
                    exported_star_ids.append(item['星图ID'])
            
            if exported_star_ids:
                # 批量更新import_status为1
                update_count = session.query(DouyinSearchList).filter(
                    DouyinSearchList.star_id.in_(exported_star_ids)
                ).update({DouyinSearchList.import_status: 1}, synchronize_session=False)
                
                session.commit()
                print(f"✅ 已更新 {update_count} 条记录的import_status为1")
            else:
                print("⚠️ 没有需要更新的记录")
                
        except Exception as e:
            print(f"❌ 更新import_status失败: {str(e)}")
            session.rollback()
        
        # 显示统计信息
        print("\n📈 数据统计:")
        print(f"总KOL数量: {total_records}")
        
        # 统计各分类的数据
        for category, data_list in category_data.items():
            if data_list:
                category_count = len(data_list)
                print(f"{category}: {category_count} 条数据")
        
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
