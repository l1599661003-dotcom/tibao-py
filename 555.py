import json
from core.database_text_tibao_2 import session
from models.models_tibao import DouyinUserList

def sync_douyin_name(batch_size=1000):
    """同步douyin_name字段，从user_info JSON中提取nickname
    
    Args:
        batch_size (int): 每批处理的记录数量，默认100条
    """
    
    try:
        print("开始同步douyin_name字段...")
        print(f"批量大小: {batch_size} 条/批")
        
        # 获取所有需要更新的记录
        users = session.query(DouyinUserList).filter(
            DouyinUserList.user_info.isnot(None),
            DouyinUserList.user_info != '',
            DouyinUserList.douyin_name.is_(None)
        ).all()
        # users = session.query(DouyinUserList).filter(
        #     DouyinUserList.user_info.isnot(None),
        #     DouyinUserList.user_info != '',
        #     DouyinUserList.douyin_id.is_(None)
        # ).all()
        
        print(f"找到 {len(users)} 条需要处理的记录")
        
        updated_count = 0
        error_count = 0
        processed_count = 0
        
        for user in users:
            try:
                # 解析user_info JSON
                user_info = json.loads(user.user_info)
                
                # 提取nickname
                nickname = user_info.get('nickname')

                if nickname and user.douyin_name != nickname:
                    # 更新douyin_name字段
                    user.douyin_name = nickname
                    updated_count += 1
                    print(f"更新用户ID {user.id}: {nickname}")

                # short_id = user_info.get('short_id')
                #
                # if short_id and user.douyin_id != short_id:
                #     # 更新douyin_name字段
                #     user.douyin_id = short_id
                #     updated_count += 1
                #     print(f"更新用户ID {user.id}: {short_id}")
                
                processed_count += 1
                
                # 每处理batch_size条记录就提交一次
                if processed_count % batch_size == 0:
                    session.commit()
                    print(f"已处理 {processed_count}/{len(users)} 条记录，提交第 {processed_count//batch_size} 批")
                
            except json.JSONDecodeError:
                error_count += 1
                print(f"用户ID {user.id}: JSON解析失败")
            except Exception as e:
                error_count += 1
                print(f"用户ID {user.id}: 处理失败 - {str(e)}")
        
        # 提交剩余的更改
        if processed_count % batch_size != 0:
            session.commit()
            print(f"提交最后一批，共处理 {processed_count} 条记录")
        
        print(f"\n同步完成!")
        print(f"总处理记录: {len(users)}")
        print(f"成功更新: {updated_count}")
        print(f"处理失败: {error_count}")
        print(f"分批提交: {batch_size} 条/批")
        
    except Exception as e:
        session.rollback()
        print(f"同步过程中出现错误: {str(e)}")

if __name__ == "__main__":
    print("=== 抖音用户数据同步工具 ===")

    # 2. 同步数据
    print("\n1. 开始同步数据...")
    
    # 根据数据量选择合适的批量大小
    # 小数据量(<1000条): 50条/批
    # 中等数据量(1000-10000条): 100条/批  
    # 大数据量(>10000条): 200条/批
    batch_size = 5000  # 默认100条/批，可以根据实际情况调整
    
    print(f"使用批量大小: {batch_size} 条/批")
    print("提示: 如果数据库压力大，可以减小batch_size；如果数据库性能好，可以增大batch_size")
    
    sync_douyin_name(batch_size)
    
    print("\n程序执行完成!")
