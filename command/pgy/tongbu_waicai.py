from core.database_text_tibao_2 import session
from models.models_tibao import KolProfileDataWaicai, ZWaicaiZong
from sqlalchemy import and_

def sync_waicai_data():
    """
    同步外采数据到博主档案表
    批量更新，提高性能
    """
    try:
        # 获取所有外采数据
        waicai_data = session.query(ZWaicaiZong).all()
        print(f"获取到 {len(waicai_data)} 条外采数据")
        
        # 定义需要同步的字段映射
        field_mapping = {
            # 'dandelion_link': 'dandelion_link',
            # 'xhs_link': 'xhs_link',
            # 'nickname': 'nickname',
            # 'xhs_id': 'xhs_id',
            # 'fans_count': 'fans_count',
            # 'tags': 'tags',
            # 'graphic_price': 'graphic_price',
            # 'video_price': 'video_price',
            # 'like_collect_count': 'like_collect_count',
            # 'region': 'region',
            # 'last_note_update_time': 'last_note_update_time',
            # 'video_note_ratio': 'video_note_ratio',
            # 'female_fans_ratio': 'female_fans_ratio',
            # 'active_fans_ratio': 'active_fans_ratio',
            # 'age_lt18_ratio': 'age_lt18_ratio',
            # 'age_18_24_ratio': 'age_18_24_ratio',
            # 'age_25_34_ratio': 'age_25_34_ratio',
            # 'age_35_44_ratio': 'age_35_44_ratio',
            # 'age_gt44_ratio': 'age_gt44_ratio',
            # 'cooperated_brands': 'cooperated_brands',
            'fangpian_wechat': 'fangpian_wechat',
            'return_points': 'return_points',
            'wechat': 'wechat',
            'return_points_person': 'return_points_person',
        }
        
        updated_count = 0
        created_count = 0
        
        for waicai in waicai_data:
            if not waicai.blogger_id:
                continue
                
            # 查找对应的博主档案
            waicai_info = session.query(KolProfileDataWaicai).filter(
                KolProfileDataWaicai.blogger_id == waicai.blogger_id
            ).first()
            
            if waicai_info:
                # 更新现有记录 - 只更新空值或None的字段
                update_fields = {}
                for source_field, target_field in field_mapping.items():
                    source_value = getattr(waicai, source_field, None)
                    target_value = getattr(waicai_info, target_field, None)
                    
                    # 如果目标字段为空或None，且源字段有值，则更新
                    if (target_value is None or target_value == '' or target_value == 0) and source_value is not None:
                        update_fields[target_field] = source_value
                
                if update_fields:
                    # 批量更新字段
                    for field, value in update_fields.items():
                        setattr(waicai_info, field, value)
                    updated_count += 1
            else:
                # 创建新记录
                new_profile = KolProfileDataWaicai()
                new_profile.blogger_id = waicai.blogger_id
                
                # 设置所有字段值
                for source_field, target_field in field_mapping.items():
                    source_value = getattr(waicai, source_field, None)
                    if source_value is not None:
                        setattr(new_profile, target_field, source_value)
                
                session.add(new_profile)
                created_count += 1
        
        # 提交事务
        session.commit()
        print(f"同步完成！更新了 {updated_count} 条记录，创建了 {created_count} 条新记录")
        
    except Exception as e:
        session.rollback()
        print(f"同步过程中发生错误: {e}")
        raise
    finally:
        session.close()

def sync_waicai_data_batch():
    """
    批量同步外采数据到博主档案表 - 使用批量更新提高性能
    """
    try:
        # 获取所有外采数据
        waicai_data = session.query(ZWaicaiZong).all()
        print(f"获取到 {len(waicai_data)} 条外采数据")
        
        # 获取所有博主ID
        blogger_ids = [waicai.blogger_id for waicai in waicai_data if waicai.blogger_id]
        
        # 批量查询现有的博主档案
        existing_profiles = session.query(KolProfileDataWaicai).filter(
            KolProfileDataWaicai.blogger_id.in_(blogger_ids)
        ).all()
        
        # 创建ID到档案的映射
        profile_map = {profile.blogger_id: profile for profile in existing_profiles}
        
        updated_count = 0
        created_count = 0
        
        for waicai in waicai_data:
            if not waicai.blogger_id:
                continue
            
            if waicai.blogger_id in profile_map:
                # 更新现有记录
                profile = profile_map[waicai.blogger_id]
                updated = False
                
                # 只有 dandelion_link 为 null 的记录才进行更新
            # if profile.dandelion_link is None:
                # 检查并更新各个字段
                # if not profile.dandelion_link and waicai.dandelion_link:
                #     profile.dandelion_link = waicai.dandelion_link
                #     updated = True
                # if not profile.xhs_link and waicai.xhs_link:
                #     profile.xhs_link = waicai.xhs_link
                #     updated = True
                # if not profile.nickname and waicai.nickname:
                #     profile.nickname = waicai.nickname
                #     updated = True
                # if not profile.xhs_id and waicai.xhs_id:
                #     profile.xhs_id = waicai.xhs_id
                #     updated = True
                # if not profile.fans_count and waicai.fans_count:
                #     profile.fans_count = waicai.fans_count
                #     updated = True
                # if not profile.tags and waicai.tags:
                #     profile.tags = waicai.tags
                #     updated = True
                # if not profile.graphic_price and waicai.graphic_price:
                #     profile.graphic_price = waicai.graphic_price
                #     updated = True
                # if not profile.video_price and waicai.video_price:
                #     profile.video_price = waicai.video_price
                #     updated = True
                # if not profile.like_collect_count and waicai.like_collect_count:
                #     profile.like_collect_count = waicai.like_collect_count
                #     updated = True
                # if not profile.region and waicai.region:
                #     profile.region = waicai.region
                #     updated = True
                # if not profile.last_note_update_time and waicai.last_note_update_time:
                #     profile.last_note_update_time = waicai.last_note_update_time
                #     updated = True
                # if not profile.video_note_ratio and waicai.video_note_ratio:
                #     profile.video_note_ratio = waicai.video_note_ratio
                #     updated = True
                # if not profile.female_fans_ratio and waicai.female_fans_ratio:
                #     profile.female_fans_ratio = waicai.female_fans_ratio
                #     updated = True
                # if not profile.active_fans_ratio and waicai.active_fans_ratio:
                #     profile.active_fans_ratio = waicai.active_fans_ratio
                #     updated = True
                # if not profile.age_lt18_ratio and waicai.age_lt18_ratio:
                #     profile.age_lt18_ratio = waicai.age_lt18_ratio
                #     updated = True
                # if not profile.age_18_24_ratio and waicai.age_18_24_ratio:
                #     profile.age_18_24_ratio = waicai.age_18_24_ratio
                #     updated = True
                # if not profile.age_25_34_ratio and waicai.age_25_34_ratio:
                #     profile.age_25_34_ratio = waicai.age_25_34_ratio
                #     updated = True
                # if not profile.age_35_44_ratio and waicai.age_35_44_ratio:
                #     profile.age_35_44_ratio = waicai.age_35_44_ratio
                #     updated = True
                # if not profile.age_gt44_ratio and waicai.age_gt44_ratio:
                #     profile.age_gt44_ratio = waicai.age_gt44_ratio
                #     updated = True
                # if not profile.cooperated_brands and waicai.cooperated_brands:
                #     profile.cooperated_brands = waicai.cooperated_brands
                #     updated = True
                # if not profile.fangpian_wechat and waicai.fangpian_wechat:
                #     profile.fangpian_wechat = waicai.fangpian_wechat
                #     updated = True
                if not profile.return_points and waicai.return_points:
                    profile.return_points = waicai.return_points
                    updated = True
                if not profile.wechat and waicai.wechat:
                    profile.wechat = waicai.wechat
                    updated = True
                if not profile.return_points_person and waicai.return_points_person:
                    profile.return_points_person = waicai.return_points_person
                    updated = True

                if updated:
                    updated_count += 1
            # else:
            #     # 创建新记录
            #     new_profile = KolProfileDataWaicai()
            #     new_profile.blogger_id = waicai.blogger_id
            #
            #     # 设置所有字段值
            #     if waicai.dandelion_link:
            #         new_profile.dandelion_link = waicai.dandelion_link
            #     if waicai.xhs_link:
            #         new_profile.xhs_link = waicai.xhs_link
            #     if waicai.nickname:
            #         new_profile.nickname = waicai.nickname
            #     if waicai.xhs_id:
            #         new_profile.xhs_id = waicai.xhs_id
            #     if waicai.fans_count:
            #         new_profile.fans_count = waicai.fans_count
            #     if waicai.tags:
            #         new_profile.tags = waicai.tags
            #     if waicai.graphic_price:
            #         new_profile.graphic_price = waicai.graphic_price
            #     if waicai.video_price:
            #         new_profile.video_price = waicai.video_price
            #     if waicai.like_collect_count:
            #         new_profile.like_collect_count = waicai.like_collect_count
            #     if waicai.region:
            #         new_profile.region = waicai.region
            #     if waicai.last_note_update_time:
            #         new_profile.last_note_update_time = waicai.last_note_update_time
            #     if waicai.video_note_ratio:
            #         new_profile.video_note_ratio = waicai.video_note_ratio
            #     if waicai.female_fans_ratio:
            #         new_profile.female_fans_ratio = waicai.female_fans_ratio
            #     if waicai.active_fans_ratio:
            #         new_profile.active_fans_ratio = waicai.active_fans_ratio
            #     if waicai.age_lt18_ratio:
            #         new_profile.age_lt18_ratio = waicai.age_lt18_ratio
            #     if waicai.age_18_24_ratio:
            #         new_profile.age_18_24_ratio = waicai.age_18_24_ratio
            #     if waicai.age_25_34_ratio:
            #         new_profile.age_25_34_ratio = waicai.age_25_34_ratio
            #     if waicai.age_35_44_ratio:
            #         new_profile.age_35_44_ratio = waicai.age_35_44_ratio
            #     if waicai.age_gt44_ratio:
            #         new_profile.age_gt44_ratio = waicai.age_gt44_ratio
            #     if waicai.cooperated_brands:
            #         new_profile.cooperated_brands = waicai.cooperated_brands
            #     if waicai.fangpian_wechat:
            #         new_profile.fangpian_wechat = waicai.fangpian_wechat
            #     if waicai.return_points:
            #         new_profile.return_points = waicai.return_points
            #     if waicai.wechat:
            #         new_profile.wechat = waicai.wechat
            #
            #     session.add(new_profile)
            #     created_count += 1
        
        # 提交事务
        session.commit()
        print(f"批量同步完成！更新了 {updated_count} 条记录，创建了 {created_count} 条新记录")
        
    except Exception as e:
        session.rollback()
        print(f"批量同步过程中发生错误: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    # 使用优化的批量同步方法
    sync_waicai_data_batch()







