import time

from core.database_haoge_fangpian_local import session
from models.models_tianji import FpEmployee, FpEmployeeDashboard, FpDepartmentEmployeeMapping
from loguru import logger


def get_employee_dashboard():
    """
    获取部门员工数据并为每个员工创建查看组内所有员工看板的权限配置
    
    逻辑说明：
    - 获取部门编码为008012开头的所有员工
    - 为每个员工创建能够查看组内所有员工看板的配置
    - user_id: 查看者员工ID
    - employee_id: 被查看的员工ID
    - 每个员工都能看到组内所有员工的看板
    """
    try:
        # 获取部门编码为008012开头的员工ID列表
        employee_ids = session.query(FpDepartmentEmployeeMapping.employee_id).filter(
            FpDepartmentEmployeeMapping.department_level_code.like('008012%')
        ).all()
        
        logger.info(f"找到 {len(employee_ids)} 个员工ID")
        
        # 提取ID值
        employee_id_list = [row[0] for row in employee_ids]
        
        # 查询员工信息
        employees = session.query(FpEmployee).filter(
            FpEmployee.id.in_(employee_id_list),
            FpEmployee.status == 1
        ).all()
        
        success_count = 0
        duplicate_count = 0
        
        # 为每个员工创建仪表盘配置
        # 让每个员工都能看到组内所有员工的看板
        for viewer_employee in employees:  # 查看者员工
            try:
                for target_employee in employees:  # 被查看的员工
                    # 检查是否已存在相同的仪表盘配置
                    existing_dashboard = session.query(FpEmployeeDashboard).filter(
                        FpEmployeeDashboard.user_id == viewer_employee.id,  # 查看者
                        FpEmployeeDashboard.employee_id == target_employee.id,  # 被查看的员工
                        FpEmployeeDashboard.dashboard_type_id == 12,
                        FpEmployeeDashboard.status == 1
                    ).first()

                    if existing_dashboard:
                        logger.info(f"员工 {viewer_employee.employee_name} 查看 {target_employee.employee_name} 的仪表盘配置已存在，跳过")
                        duplicate_count += 1
                        continue

                    # 创建新的仪表盘配置
                    dashboard = FpEmployeeDashboard(
                        user_id=target_employee.id,  # 查看者ID
                        dashboard_type_id=12,
                        dept_id=0,
                        employee_id=target_employee.id,  # 被查看的员工ID
                        is_default=1,
                        delete_time=0,
                        create_time=int(time.time()),
                        update_time=int(time.time()),
                        create_user=3,
                        update_user=3,
                        other_params='',
                        list_order=1000,
                        status=1,
                        remark=viewer_employee.employee_name,
                    )
                
                    session.add(dashboard)
                    success_count += 1
                    logger.info(f"员工 {viewer_employee.employee_name} 可以查看 {target_employee.employee_name} 的仪表盘配置")
                
            except Exception as e:
                logger.error(f"为员工 {viewer_employee.employee_name} 创建仪表盘配置时出错: {str(e)}")
                session.rollback()
                continue
        
        # 批量提交所有更改
        try:
            session.commit()
            logger.info(f"批量提交成功: 新增 {success_count} 个，重复 {duplicate_count} 个")
        except Exception as e:
            logger.error(f"提交数据库更改时出错: {str(e)}")
            session.rollback()
            raise
            
    except Exception as e:
        logger.error(f"获取员工仪表盘数据时出错: {str(e)}")
        session.rollback()
        raise
    finally:
        # 确保关闭数据库会话
        session.close()


if __name__ == '__main__':
    get_employee_dashboard()








