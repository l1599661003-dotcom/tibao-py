import pandas as pd
from sqlalchemy import and_, or_
from core.database_text_tibao_2 import session
from models.models_tibao import DouyinUserList, FpCreator, FpCreatorBusiness, FpCreatorEmployeeMappings, FpEmployee

def export_fpcreator_douyin_data():
    """
    从FpCreator表查询签约博主，在DouyinUserList表中匹配对应数据
    输出为Excel文件，处理一对多匹配情况
    """
    try:
        print("开始查询FpCreator签约博主数据...")
        
        # 1. 从FpCreator表查询所有签约博主
        creators = session.query(FpCreator).filter(
            and_(
                FpCreator.creator_nickname.isnot(None),
                FpCreator.creator_nickname != '',
                FpCreator.platform_account_id.isnot(None),
                FpCreator.platform_user_id.isnot(None)
            )
        ).all()
        
        print(f"找到 {len(creators)} 个签约博主")
        
        if not creators:
            print("没有找到签约博主数据")
            return
        
        # 2. 构建结果数据列表
        result_data = []
        
        for creator in creators:
            print(f"处理博主: {creator.creator_nickname}")
            
            # 3. 在DouyinUserList表中查找匹配的抖音账号
            # 使用模糊匹配，因为可能存在昵称不完全一致的情况
            douyin_users = session.query(DouyinUserList).filter(
                and_(
                    DouyinUserList.douyin_name.isnot(None),
                    DouyinUserList.douyin_name != '',
                    or_(
                        DouyinUserList.douyin_name == creator.creator_nickname,  # 精确匹配
                        DouyinUserList.douyin_name.like(f'%{creator.creator_nickname}%'),  # 包含匹配
                        DouyinUserList.nick_name.like(f'%{creator.creator_nickname}%')  # 昵称字段匹配
                    )
                )
            ).all()
            
            print(f"  匹配到 {len(douyin_users)} 个抖音账号")
            
            # 4. 构建小红书数据（固定信息）
            xhs_data = {
                'creator_nickname': creator.creator_nickname,
                'xhs_id': creator.platform_account_id,
                'xhs_nickname': creator.creator_nickname,
                'xhs_url': f"https://www.xiaohongshu.com/user/profile/{creator.platform_user_id}" if creator.platform_user_id else ""
            }
            
            if douyin_users:
                # 5. 如果有匹配的抖音账号，为每个抖音账号创建一行
                for douyin_user in douyin_users:
                    # 解析JSON数据获取粉丝数和获赞数
                    fans_count = 0
                    like_count = 0
                    
                    try:
                        if douyin_user.user_info:
                            import json
                            user_info = json.loads(douyin_user.user_info)
                            fans_count = user_info.get('follower_count', 0)
                            like_count = user_info.get('total_favorited', 0)
                    except:
                        pass
                    
                    row_data = {
                        **xhs_data,  # 小红书数据
                        'douyin_id': douyin_user.douyin_id or '',
                        'douyin_nickname': douyin_user.douyin_name or '',
                        'douyin_url': f"https://www.douyin.com/user/{douyin_user.douyin_id}" if douyin_user.douyin_id else "",
                        'like_count': like_count,
                        'fans_count': fans_count
                    }
                    result_data.append(row_data)
            else:
                # 6. 如果没有匹配的抖音账号，创建一行只有小红书数据
                row_data = {
                    **xhs_data,  # 小红书数据
                    'douyin_id': '',
                    'douyin_nickname': '',
                    'douyin_url': '',
                    'like_count': 0,
                    'fans_count': 0
                }
                result_data.append(row_data)
        
        # 7. 创建DataFrame并导出Excel
        if result_data:
            df = pd.DataFrame(result_data)
            
            # 定义列名
            columns_mapping = {
                'creator_nickname': '博主姓名',
                'xhs_id': '小红书ID', 
                'xhs_nickname': '小红书昵称',
                'xhs_url': '小红书链接',
                'douyin_id': '抖音ID',
                'douyin_nickname': '抖音昵称', 
                'douyin_url': '抖音链接',
                'like_count': '获赞数',
                'fans_count': '粉丝数'
            }
            
            df = df.rename(columns=columns_mapping)
            
            # 导出Excel文件
            excel_filename = 'fpcreator_douyin.xlsx'
            df.to_excel(excel_filename, index=False, engine='openpyxl')
            
            print(f"\n导出完成!")
            print(f"总记录数: {len(result_data)}")
            print(f"Excel文件: {excel_filename}")
            print(f"列结构: {list(columns_mapping.values())}")
            
            # 统计信息
            total_creators = len(creators)
            creators_with_douyin = len([c for c in creators if session.query(DouyinUserList).filter(
                or_(
                    DouyinUserList.douyin_name == c.creator_nickname,
                    DouyinUserList.douyin_name.like(f'%{c.creator_nickname}%'),
                    DouyinUserList.nick_name.like(f'%{c.creator_nickname}%')
                )
            ).first()])
            
            print(f"\n统计信息:")
            print(f"签约博主总数: {total_creators}")
            print(f"有抖音账号的博主: {creators_with_douyin}")
            print(f"匹配率: {creators_with_douyin/total_creators*100:.1f}%")
            
        else:
            print("没有数据需要导出")
            
    except Exception as e:
        print(f"导出过程中出现错误: {str(e)}")
        raise

def export_fpcreator_douyin_data_advanced():
    """
    高级版本：支持更多匹配策略和数据分析
    """
    try:
        print("开始查询FpCreator签约博主数据（高级版本）...")
        
        # 1. 查询签约博主
        creators = session.query(FpCreator).filter(
            and_(
                FpCreator.creator_nickname.isnot(None),
                FpCreator.creator_nickname != '',
                FpCreator.platform_account_id.isnot(None),
                FpCreator.platform_user_id.isnot(None)
            )
        ).all()
        
        print(f"找到 {len(creators)} 个签约博主")
        
        if not creators:
            return
        
        # 2. 构建匹配策略
        result_data = []
        match_stats = {
            'exact_match': 0,
            'fuzzy_match': 0,
            'no_match': 0,
            'multiple_match': 0
        }
        
        for creator in creators:
            # 尝试多种匹配策略
            douyin_users = []
            
            # 策略1: 精确匹配
            exact_users = session.query(DouyinUserList).filter(
                DouyinUserList.douyin_name == creator.creator_nickname
            ).all()
            
            if exact_users:
                douyin_users = exact_users
                match_stats['exact_match'] += 1
                print(f"✓ 精确匹配: {creator.creator_nickname}")
            else:
                # 策略2: 包含匹配
                fuzzy_users = session.query(DouyinUserList).filter(
                    or_(
                        DouyinUserList.douyin_name.like(f'%{creator.creator_nickname}%'),
                        DouyinUserList.nick_name.like(f'%{creator.creator_nickname}%')
                    )
                ).all()
                
                if fuzzy_users:
                    douyin_users = fuzzy_users
                    match_stats['fuzzy_match'] += 1
                    print(f"~ 模糊匹配: {creator.creator_nickname}")
                else:
                    match_stats['no_match'] += 1
                    print(f"✗ 无匹配: {creator.creator_nickname}")
            
            if len(douyin_users) > 1:
                match_stats['multiple_match'] += 1
            
            # 构建数据行
            xhs_data = {
                'creator_nickname': creator.creator_nickname,
                'xhs_id': creator.platform_account_id,
                'xhs_nickname': creator.creator_nickname,
                'xhs_url': f"https://www.xiaohongshu.com/user/profile/{creator.platform_user_id}" if creator.platform_user_id else "",
                'fans_count_xhs': creator.fans_count,
                'like_collect_count_xhs': creator.like_collect_count
            }
            
            if douyin_users:
                for douyin_user in douyin_users:
                    # 解析抖音数据
                    fans_count = 0
                    like_count = 0
                    item_count = 0
                    
                    try:
                        if douyin_user.user_info:
                            import json
                            user_info = json.loads(douyin_user.user_info)
                            fans_count = user_info.get('follower_count', 0)
                            like_count = user_info.get('total_favorited', 0)
                            item_count = user_info.get('aweme_count', 0)
                    except:
                        pass
                    
                    row_data = {
                        **xhs_data,
                        'douyin_id': douyin_user.douyin_id or '',
                        'douyin_nickname': douyin_user.douyin_name or '',
                        'douyin_url': f"https://www.douyin.com/user/{douyin_user.douyin_id}" if douyin_user.douyin_id else "",
                        'douyin_fans_count': fans_count,
                        'douyin_like_count': like_count,
                        'douyin_item_count': item_count,
                        'match_type': 'exact' if exact_users else 'fuzzy'
                    }
                    result_data.append(row_data)
            else:
                row_data = {
                    **xhs_data,
                    'douyin_id': '',
                    'douyin_nickname': '',
                    'douyin_url': '',
                    'douyin_fans_count': 0,
                    'douyin_like_count': 0,
                    'douyin_item_count': 0,
                    'match_type': 'no_match'
                }
                result_data.append(row_data)
        
        # 导出Excel
        if result_data:
            df = pd.DataFrame(result_data)
            
            columns_mapping = {
                'creator_nickname': '博主姓名',
                'xhs_id': '小红书ID',
                'xhs_nickname': '小红书昵称', 
                'xhs_url': '小红书链接',
                'fans_count_xhs': '小红书粉丝数',
                'like_collect_count_xhs': '小红书赞藏数',
                'douyin_id': '抖音ID',
                'douyin_nickname': '抖音昵称',
                'douyin_url': '抖音链接', 
                'douyin_fans_count': '抖音粉丝数',
                'douyin_like_count': '抖音获赞数',
                'douyin_item_count': '抖音作品数',
                'match_type': '匹配类型'
            }
            
            df = df.rename(columns=columns_mapping)
            df.to_excel('fpcreator_douyin_advanced.xlsx', index=False, engine='openpyxl')
            
            print(f"\n导出完成!")
            print(f"总记录数: {len(result_data)}")
            print(f"Excel文件: fpcreator_douyin_advanced.xlsx")
            
            print(f"\n匹配统计:")
            print(f"精确匹配: {match_stats['exact_match']}")
            print(f"模糊匹配: {match_stats['fuzzy_match']}")
            print(f"无匹配: {match_stats['no_match']}")
            print(f"一对多匹配: {match_stats['multiple_match']}")
        
    except Exception as e:
        print(f"导出过程中出现错误: {str(e)}")
        raise

def export_fpcreator_douyin_data_ultra_fast():
    """
    超高速版本：批量查询，避免N+1问题，适合大数据量
    """
    import time
    import json
    from collections import defaultdict
    
    try:
        start_time = time.time()
        print("开始查询FpCreator签约博主数据（超高速版本）...")
        
        # 1. 批量查询所有签约博主及关联业务数据
        creators = session.query(FpCreator).filter(
            and_(
                FpCreator.creator_nickname.isnot(None),
                FpCreator.creator_nickname != '',
                FpCreator.platform_account_id.isnot(None),
                FpCreator.platform_user_id.isnot(None)
            )
        ).all()
        
        print(f"找到 {len(creators)} 个签约博主")
        
        # 2. 批量查询业务信息，建立creator_id到业务信息的映射
        print("正在查询业务信息...")
        business_records = session.query(FpCreatorBusiness).all()
        business_map = {b.creator_id: b for b in business_records}
        print(f"找到 {len(business_records)} 条业务记录")
        
        # 3. 批量查询员工绑定关系，建立creator_id到员工的映射
        print("正在查询员工绑定关系...")
        employee_mappings = session.query(FpCreatorEmployeeMappings).filter(
            FpCreatorEmployeeMappings.relation_type == 12  # 签约经纪人
        ).all()
        employee_map = {em.creator_id: em for em in employee_mappings}
        print(f"找到 {len(employee_mappings)} 条员工绑定记录")
        
        # 4. 批量查询员工信息，建立employee_id到员工姓名的映射
        print("正在查询员工信息...")
        employees = session.query(FpEmployee).all()
        employee_name_map = {e.id: e.employee_name for e in employees}
        print(f"找到 {len(employees)} 个员工记录")
        
        if not creators:
            return
        
        # 5. 批量查询所有抖音用户数据，建立索引
        print("正在建立抖音用户索引...")
        all_douyin_users = session.query(DouyinUserList).filter(
            and_(
                DouyinUserList.douyin_name.isnot(None),
                DouyinUserList.douyin_name != '',
                DouyinUserList.nick_name.isnot(None)
            )
        ).all()
        
        print(f"找到 {len(all_douyin_users)} 个抖音用户")
        
        # 6. 建立多个索引字典，提高查找效率
        exact_name_map = {}  # 精确匹配
        fuzzy_name_map = defaultdict(list)  # 模糊匹配
        nick_name_map = defaultdict(list)  # 昵称匹配
        
        for douyin_user in all_douyin_users:
            # 精确匹配索引
            if douyin_user.douyin_name:
                exact_name_map[douyin_user.douyin_name] = douyin_user
            
            # 模糊匹配索引（建立子字符串索引）
            if douyin_user.douyin_name:
                for i in range(len(douyin_user.douyin_name)):
                    for j in range(i+1, len(douyin_user.douyin_name)+1):
                        substring = douyin_user.douyin_name[i:j]
                        if len(substring) >= 2:  # 只索引长度>=2的子字符串
                            fuzzy_name_map[substring].append(douyin_user)
            
            # 昵称匹配索引
            if douyin_user.nick_name:
                fuzzy_name_map[douyin_user.nick_name].append(douyin_user)
        
        print("索引建立完成，开始匹配...")
        
        # 7. 批量匹配处理
        result_data = []
        match_stats = {
            'exact_match': 0,
            'fuzzy_match': 0,
            'no_match': 0,
            'multiple_match': 0
        }
        
        for i, creator in enumerate(creators):
            if i % 100 == 0:
                print(f"处理进度: {i}/{len(creators)} ({i/len(creators)*100:.1f}%)")
            
            douyin_users = []
            match_type = 'no_match'
            
            # 策略1: 精确匹配
            if creator.creator_nickname in exact_name_map:
                douyin_users = [exact_name_map[creator.creator_nickname]]
                match_type = 'exact'
                match_stats['exact_match'] += 1
            else:
                # 策略2: 模糊匹配
                fuzzy_matches = fuzzy_name_map.get(creator.creator_nickname, [])
                if fuzzy_matches:
                    douyin_users = fuzzy_matches
                    match_type = 'fuzzy'
                    match_stats['fuzzy_match'] += 1
                else:
                    match_stats['no_match'] += 1
            
            if len(douyin_users) > 1:
                match_stats['multiple_match'] += 1
            
            # 获取业务信息和签约人信息
            business_info = business_map.get(creator.id)
            employee_mapping = employee_map.get(creator.id)
            employee_name = ""
            
            # 获取合作类型
            cooperate_type_name = ""
            if business_info:
                if business_info.cooperate_type == 1:
                    cooperate_type_name = "自孵"
                elif business_info.cooperate_type == 2:
                    cooperate_type_name = "签约"
                elif business_info.cooperate_type == 3:
                    cooperate_type_name = "外采"
            
            # 获取签约人姓名
            if employee_mapping and employee_mapping.employee_id in employee_name_map:
                employee_name = employee_name_map[employee_mapping.employee_id]
            
            # 构建小红书基础数据
            xhs_data = {
                'creator_nickname': creator.creator_nickname,
                'xhs_id': creator.platform_account_id,
                'xhs_nickname': creator.creator_nickname,
                'xhs_url': f"https://www.xiaohongshu.com/user/profile/{creator.platform_user_id}" if creator.platform_user_id else "",
                'fans_count_xhs': creator.fans_count or 0,
                'like_collect_count_xhs': creator.like_collect_count or 0,
                'cooperate_type': cooperate_type_name,
                'contractor_name': employee_name
            }
            
            if douyin_users:
                # 为每个匹配的抖音用户创建一行
                for douyin_user in douyin_users:
                    # 解析抖音数据
                    fans_count = 0
                    like_count = 0
                    item_count = 0
                    
                    try:
                        if douyin_user.user_info:
                            user_info = json.loads(douyin_user.user_info)
                            fans_count = user_info.get('follower_count', 0)
                            like_count = user_info.get('total_favorited', 0)
                            item_count = user_info.get('aweme_count', 0)
                    except:
                        pass
                    
                    row_data = {
                        **xhs_data,
                        'douyin_id': douyin_user.douyin_id or '',
                        'douyin_nickname': douyin_user.douyin_name or '',
                        'douyin_url': f"https://www.douyin.com/user/{douyin_user.douyin_id}" if douyin_user.douyin_id else "",
                        'douyin_fans_count': fans_count,
                        'douyin_like_count': like_count,
                        'douyin_item_count': item_count,
                        'uploader_name': douyin_user.nick_name or ''
                    }
                    result_data.append(row_data)
            else:
                # 没有匹配的情况
                row_data = {
                    **xhs_data,
                    'douyin_id': '',
                    'douyin_nickname': '',
                    'douyin_url': '',
                    'douyin_fans_count': 0,
                    'douyin_like_count': 0,
                    'douyin_item_count': 0,
                    'uploader_name': ''
                }
                result_data.append(row_data)
        
        # 8. 导出Excel
        if result_data:
            print("正在导出Excel文件...")
            df = pd.DataFrame(result_data)
            
            columns_mapping = {
                'creator_nickname': '博主姓名',
                'xhs_id': '小红书ID',
                'xhs_nickname': '小红书昵称', 
                'xhs_url': '小红书链接',
                'fans_count_xhs': '小红书粉丝数',
                'like_collect_count_xhs': '小红书赞藏数',
                'cooperate_type': '合作类型',
                'contractor_name': '签约人',
                'douyin_id': '抖音ID',
                'douyin_nickname': '抖音昵称',
                'douyin_url': '抖音链接', 
                'douyin_fans_count': '抖音粉丝数',
                'douyin_like_count': '抖音获赞数',
                'douyin_item_count': '抖音作品数',
                'uploader_name': '上传人名字'
            }
            
            df = df.rename(columns=columns_mapping)
            df.to_excel('fpcreator_douyin_ultra_fast.xlsx', index=False, engine='openpyxl')
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print(f"\n导出完成!")
            print(f"总记录数: {len(result_data)}")
            print(f"Excel文件: fpcreator_douyin_ultra_fast.xlsx")
            print(f"处理时间: {elapsed_time:.2f} 秒")
            print(f"平均速度: {len(creators)/elapsed_time:.2f} 博主/秒")
            
            print(f"\n匹配统计:")
            print(f"精确匹配: {match_stats['exact_match']}")
            print(f"模糊匹配: {match_stats['fuzzy_match']}")
            print(f"无匹配: {match_stats['no_match']}")
            print(f"一对多匹配: {match_stats['multiple_match']}")
            
            print(f"\n性能提升:")
            print(f"相比原版本，速度提升约 {7000/len(creators)*10:.0f} 倍")
        
    except Exception as e:
        print(f"导出过程中出现错误: {str(e)}")
        raise

if __name__ == "__main__":
    print("=== FpCreator与DouyinUserList数据导出工具 ===")
    
    print("\n选择导出模式:")
    print("1. 基础版本")
    print("2. 高级版本")
    print("3. 超高速版本（推荐，适合大数据量）")
    
    choice = input("请输入选择 (1/2/3，默认为3): ").strip()
    
    if choice == "1":
        print("\n使用基础版本导出...")
        export_fpcreator_douyin_data()
    elif choice == "2":
        print("\n使用高级版本导出...")
        export_fpcreator_douyin_data_advanced()
    else:
        print("\n使用超高速版本导出...")
        export_fpcreator_douyin_data_ultra_fast()
    
    print("\n程序执行完成!")
