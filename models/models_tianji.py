from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FpEmployee(Base):
    __tablename__ = 'fp_employee'

    id = Column(Integer, primary_key=True)
    status = Column(Integer)
    employee_name = Column(String(256))
    delete_time = Column(Integer)
    update_time = Column(Integer)
    create_user = Column(Integer)
    create_time = Column(Integer)
    update_user = Column(Integer)


class FpDepartmentEmployeeMapping(Base):
    __tablename__ = 'fp_department_employee_mappings'

    id = Column(Integer, primary_key=True)
    dept_id = Column(String(256))
    employee_id = Column(Integer)
    department_level_code = Column(String(300))
    delete_time = Column(Integer)
    update_time = Column(Integer)
    create_user = Column(Integer)
    create_time = Column(Integer)
    update_user = Column(Integer)


class FpEmployeeDashboard(Base):
    __tablename__ = 'fp_employee_dashboard'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    employee_id = Column(Integer)
    dashboard_type_id = Column(Integer)
    dept_id = Column(Integer)
    other_params = Column(String(300))
    is_default = Column(Integer)
    list_order = Column(Integer)
    status = Column(Integer)
    remark = Column(String(300))
    delete_time = Column(Integer)
    update_time = Column(Integer)
    create_user = Column(Integer)
    create_time = Column(Integer)
    update_user = Column(Integer)

class FpAdminGroupAccess(Base):
    __tablename__ = 'fp_admin_access'

    id = Column(Integer, primary_key=True)
    uid = Column(Integer)
    old_group_id = Column(Integer)
    group_id = Column(Integer)
    role_primary = Column(Integer)
    delete_time = Column(Integer)
    update_time = Column(Integer)
    create_user = Column(Integer)
    create_time = Column(Integer)
    update_user = Column(Integer)

