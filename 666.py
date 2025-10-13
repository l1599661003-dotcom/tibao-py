import time
from sqlalchemy import and_, text
from core.database_text_tibao_2 import session
from models.models_tibao import DouyinUserList, FpCreator

def sync_douyin_name(batch_size=1000):
    """
    高效同步抖音用户的小红书信息
    使用批量查询和批量更新，避免N+1查询问题
    """
    try:
        print("开始同步小红书信息字段...")
        print(f"批量大小: {batch_size} 条/批")
        start_time = time.time()
        
        # 获取所有需要更新的记录 - 修正查询条件
        users = session.query(DouyinUserList).filter(
            and_(
                DouyinUserList.douyin_name.isnot(None),
                DouyinUserList.douyin_name != ''
            )
        ).all()
        
        print(f"找到 {len(users)} 条需要处理的记录")
        
        if not users:
            print("没有需要处理的记录")
            return
        
        # 批量获取所有creator数据，建立昵称映射
        print("正在建立昵称映射...")
        creators = session.query(FpCreator).filter(
            and_(
                FpCreator.creator_nickname.isnot(None),
                FpCreator.creator_nickname != '',
                FpCreator.platform_account_id.isnot(None),
                FpCreator.platform_user_id.isnot(None)
            )
        ).all()
        
        # 建立昵称到creator的映射字典，提高查找效率
        nickname_map = {}
        for creator in creators:
            if creator.creator_nickname:
                nickname_map[creator.creator_nickname] = creator
        
        print(f"建立了 {len(nickname_map)} 个昵称映射")
        
        updated_count = 0
        error_count = 0
        processed_count = 0
        batch_users = []
        
        for user in users:
            try:
                # 使用字典查找，时间复杂度O(1)
                creator = nickname_map.get(user.douyin_name)
                
                if creator:
                    # 批量收集需要更新的用户
                    user.xhs_id = creator.platform_account_id
                    user.xhs_nickname = creator.creator_nickname
                    user.xhs_url = f"https://www.xiaohongshu.com/user/profile/{creator.platform_user_id}"
                    batch_users.append(user)
                    updated_count += 1

                processed_count += 1
                
                # 每处理batch_size条记录就提交一次
                if processed_count % batch_size == 0:
                    if batch_users:
                        session.commit()
                        print(f"已处理 {processed_count}/{len(users)} 条记录，更新了 {len(batch_users)} 条，提交第 {processed_count//batch_size} 批")
                        batch_users = []
                    else:
                        print(f"已处理 {processed_count}/{len(users)} 条记录，第 {processed_count//batch_size} 批无更新")
                
            except Exception as e:
                error_count += 1
                print(f"用户ID {user.id}: 处理失败 - {str(e)}")
        
        # 提交剩余的更改
        if batch_users:
            session.commit()
            print(f"提交最后一批，共更新 {len(batch_users)} 条记录")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n同步完成!")
        print(f"总处理记录: {len(users)}")
        print(f"成功更新: {updated_count}")
        print(f"处理失败: {error_count}")
        print(f"处理时间: {elapsed_time:.2f} 秒")
        print(f"平均速度: {len(users)/elapsed_time:.2f} 条/秒")
        
    except Exception as e:
        session.rollback()
        print(f"同步过程中出现错误: {str(e)}")
        raise

def sync_douyin_name_optimized_v2(batch_size=1000):
    """
    更高效的版本：使用原生SQL批量更新
    """
    try:
        print("开始同步小红书信息字段（优化版本2）...")
        print(f"批量大小: {batch_size} 条/批")
        start_time = time.time()
        
        # 使用原生SQL进行批量更新，避免Python循环
        # 使用COLLATE解决字符集排序规则不匹配问题
        update_sql = """
        UPDATE douyin_user_list dul
        INNER JOIN fp_creator fc ON dul.douyin_name COLLATE utf8mb4_unicode_ci = fc.creator_nickname COLLATE utf8mb4_unicode_ci
        SET 
            dul.xhs_id = fc.platform_account_id,
            dul.xhs_nickname = fc.creator_nickname,
            dul.xhs_url = CONCAT('https://www.xiaohongshu.com/user/profile/', fc.platform_user_id)
        WHERE 
            dul.douyin_name IS NOT NULL 
            AND dul.douyin_name != ''
            AND fc.creator_nickname IS NOT NULL
            AND fc.platform_account_id IS NOT NULL
            AND fc.platform_user_id IS NOT NULL
        """
        
        result = session.execute(text(update_sql))
        updated_count = result.rowcount
        session.commit()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n同步完成!")
        print(f"成功更新: {updated_count} 条记录")
        print(f"处理时间: {elapsed_time:.2f} 秒")
        print(f"平均速度: {updated_count/elapsed_time:.2f} 条/秒")
        
    except Exception as e:
        session.rollback()
        print(f"同步过程中出现错误: {str(e)}")
        raise

if __name__ == "__main__":
    print("=== 抖音用户数据同步工具 ===")
    
    # 根据数据量选择合适的批量大小
    batch_size = 5000  # 推荐1000条/批，平衡内存和性能
    
    print(f"使用批量大小: {batch_size} 条/批")
    print("提示: 如果数据库压力大，可以减小batch_size；如果数据库性能好，可以增大batch_size")
    
    # 选择执行方式
    print("\n选择执行方式:")
    print("1. 版本1: Python批量处理（适合复杂逻辑）")
    print("2. 版本2: 原生SQL批量更新（最高效）")
    
    choice = input("请输入选择 (1/2，默认为2): ").strip()
    
    if choice == "1":
        print("\n使用版本1: Python批量处理")
        sync_douyin_name(batch_size)
    else:
        print("\n使用版本2: 原生SQL批量更新（推荐）")
        sync_douyin_name_optimized_v2(batch_size)
    
    print("\n程序执行完成!")