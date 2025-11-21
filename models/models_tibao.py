from ast import Index
from datetime import datetime
import time

from sqlalchemy import Column, Integer, String, TIMESTAMP, DateTime, Numeric, Text, Boolean, Float, BigInteger, \
    ForeignKey, DECIMAL, func, text, Double
from sqlalchemy.dialects.mssql import JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Setting(Base):
    __tablename__ = 'settings'

    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    key = Column(String(256))
    value = Column(String(256))
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

class XiaohongshuMonth(Base):
    __tablename__ = 'xiaohongshu_month'

    id = Column(Integer, primary_key=True)
    xiaohongshu_url = Column(String(255))
    data = Column(String(255))
    status = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class ChinaProvince(Base):
    __tablename__ = 'china_province'

    id = Column(Integer, primary_key=True)
    province_name = Column(String(25))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class ChinaCity(Base):
    __tablename__ = 'china_city'

    id = Column(Integer, primary_key=True)
    city_name = Column(String(25))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class KolMediaAccount(Base):
    __tablename__ = 'kol_media_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nickname = Column(String(255), nullable=False, comment='博主昵称')
    xhs_url = Column(Text, comment='小红书链接')
    followers = Column(Integer, comment='粉丝数量')
    like_count = Column(Integer, comment='赞藏数量')
    phone = Column(String(255), comment='电话')
    wechat = Column(String(255), comment='微信')
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    sign_type = Column(String(255), comment='签约类型，自孵化或签约')
    is_enable = Column(Integer, default=1, nullable=False, comment='签约状态，0：解约，1：签约，2：可回签')
    revenue_percent = Column(Numeric(8, 2), default=0.80)
    profit_type = Column(String(20))
    fixed_price = Column(Numeric(8, 2), nullable=False, default=0.00, comment='一口价')
    fixed_price_1_1 = Column(Numeric(8, 2), default=0.00)
    fixed_price_1_2 = Column(Numeric(8, 2), default=0.00)
    fixed_price_1_3 = Column(Numeric(8, 2), default=0.00)
    fixed_price_2_4 = Column(Numeric(8, 2), default=0.00)
    fixed_price_2_5 = Column(Numeric(8, 2), default=0.00)
    fixed_price_2_6 = Column(Numeric(8, 2), default=0.00)
    referrer = Column(String(255), comment='推荐人，手动输入')
    referrer_shangwu = Column(Integer, default=0)
    account_level = Column(String(200), comment='账号等级')
    username = Column(String(255), comment='姓名')
    id_card = Column(String(255), comment='身份证')
    bank_card = Column(String(255), comment='银行卡号')
    tags = Column(String(255), comment='账号标签')
    talent_id = Column(String(100), comment='达人ID')
    privileged_time = Column(String(200), comment='授权时间')
    child_age = Column(Numeric(3, 1), comment='母婴孩子年龄')
    child_sex = Column(String(200), comment='母婴孩子性别：1=男，2=女')
    pet_supplies = Column(String(255), comment='宠物品种')
    pet_age = Column(String(200), comment='宠物年龄')
    fixed_price_proportion = Column(Numeric(8, 5), nullable=False, default=0.00000, comment='一口价分成比例')
    address = Column(String(255), comment='小红书地址（省市区类型）')
    groups = Column(Integer, comment='分组')
    refund_ratio = Column(Numeric(3, 2), comment='返点比例')
    refund_ratio_fei = Column(Numeric(5, 2), comment='非年框返点比例')

    # 数据指标
    daily_video_exposure_median = Column(Numeric(10, 2), comment='日常视频曝光中位数')
    daily_video_reading_median = Column(Numeric(10, 2), comment='日常视频阅读中位数')
    daily_video_interaction_median = Column(Numeric(10, 2), comment='日常视频互动中位数')
    daily_video_interaction_rate = Column(Numeric(10, 4), comment='日常视频互动率')
    
    # 用户画像数据
    age_less_than_18 = Column(Numeric(5, 2), nullable=False, default=0.00, comment='年龄<18')
    age_18_to_24 = Column(Numeric(5, 2), nullable=False, default=0.00, comment='年龄18_24')
    age_25_to_34 = Column(Numeric(5, 2), nullable=False, default=0.00, comment='年龄25_34')
    age_35_to_44 = Column(Numeric(5, 2), nullable=False, default=0.00, comment='年龄35_44')
    age_greater_than_44 = Column(Numeric(5, 2), nullable=False, default=0.00, comment='年龄>44')
    male_fan_percentage = Column(Numeric(5, 2), nullable=False, default=0.00, comment='男粉丝占比')
    female_fan_percentage = Column(Numeric(5, 2), nullable=False, default=0.00, comment='女粉丝占比')

    # 兴趣和地域数据
    interest_top1 = Column(String(255), comment='用户兴趣top1')
    interest_top2 = Column(String(255), comment='用户兴趣top2')
    interest_top3 = Column(String(255), comment='用户兴趣top3')
    interest_top4 = Column(String(255), comment='用户兴趣top4')
    interest_top5 = Column(String(255), comment='用户兴趣top5')
    province_top1 = Column(String(255), comment='省份top1')
    province_top2 = Column(String(255), comment='省份top2')
    province_top3 = Column(String(255), comment='省份top3')
    city_top1 = Column(String(255), comment='城市top1')
    city_top2 = Column(String(255), comment='城市top2')
    city_top3 = Column(String(255), comment='城市top3')
    device_top1 = Column(String(255), comment='设备top1')
    device_top2 = Column(String(255), comment='设备top2')
    device_top3 = Column(String(255), comment='设备top3')

    # 笔记和合作数据
    notes_published = Column(Integer, nullable=False, default=0, comment='发布笔记数量')
    content_categories = Column(String(200), comment='内容类目及占比')
    cooperated_industries = Column(String(255), comment='合作行业')
    
    # 平台相关数据
    dandelion_platform_link = Column(String(255), comment='蒲公英平台链接')
    dandelion_platform_id = Column(String(255), comment='蒲公英平台ID')
    graphic_price = Column(Numeric(10, 2), comment='图文一口价')
    video_price = Column(Numeric(10, 2), comment='视频一口价')
    
    # 其他业务数据
    shipping_address = Column(String(200), comment='所在地区')
    blogger_rebate_ratio = Column(Numeric(5, 2), nullable=False, default=0.20, comment='博主返点比例')
    dw_id = Column(String(255), comment='多维表格id')
    record_id = Column(String(255), comment='多维表格record')
    
    # 系统统计数据
    system_total_orders = Column(Integer, nullable=False, default=0, comment='系统总商单量')
    system_orders_30_days = Column(Integer, nullable=False, default=0, comment='系统30天商单量')
    system_orders_90_days = Column(Integer, nullable=False, default=0, comment='系统90天商单量')
    pgy_total_orders = Column(Integer, nullable=False, default=0, comment='蒲公英总商单量')
    pgy_orders_30_days = Column(Integer, nullable=False, default=0, comment='蒲公英30天商单量')
    pgy_orders_90_days = Column(Integer, nullable=False, default=0, comment='蒲公英90天商单量')
    
    # 状态标记
    is_update = Column(Integer, nullable=False, default=1, comment='当天是否更新')
    currentLevel = Column(Integer, default=2, comment='当前等级')
    is_water_account = Column(Integer, default=0, comment='是否是水号 0否 1是')
    is_added_to_training_group = Column(Integer, nullable=False, default=0, comment='是否拉培训群')
    is_added_to_operation_group = Column(Integer, nullable=False, default=0, comment='是否拉运营群')

    # 阅读数据
    read_median = Column(Integer, nullable=False, default=0, comment='阅读中位数')
    interaction_median = Column(Integer, nullable=False, default=0, comment='互动中位数')
    coop_pic_text_exposure_median = Column(Integer, nullable=False, default=0, comment='互动中位数')
    # imp_median = Column(Integer, comment='图文+视频曝光中位数')

    # 添加缺失的字段
    picture_read_cost = Column(DECIMAL(10, 2), nullable=True, default=0, comment='图文阅读单价')
    video_read_cost = Column(DECIMAL(10, 2), nullable=True, default=0, comment='视频阅读单价')
    gender = Column(String(10), nullable=True, comment='性别')
    reading_followers_percentage = Column(DECIMAL(10, 4), nullable=True, default=0, comment='阅读粉丝占比')
    engaged_followers_percentage = Column(DECIMAL(10, 4), nullable=True, default=0, comment='互动粉丝占比')
    reading_followers_benchmark_exceed = Column(DECIMAL(10, 4), nullable=True, default=0, comment='阅读粉丝基准超出')
    engaged_followers_benchmark_exceed = Column(DECIMAL(10, 4), nullable=True, default=0, comment='互动粉丝基准超出')
    brand_name = Column(Text, nullable=True, comment='品牌名称')
    active_followers_percentage = Column(DECIMAL(10, 4), nullable=True, default=0, comment='活跃粉丝占比')
    active_followers_benchmark_exceed = Column(DECIMAL(10, 4), nullable=True, default=0, comment='活跃粉丝基准超出')
    notesign = Column(String(255), nullable=True, comment='博主所在机构')
    last_update_time = Column(DateTime, nullable=True, comment='最后更新时间')

    # 日常图文+视频相关字段
    daily_pic_video_exposure_median = Column(Integer, nullable=False, default=0, comment='日常图文+视频曝光中位数')
    daily_pic_video_reading_median = Column(Integer, nullable=False, default=0, comment='日常图文+视频阅读中位数')
    daily_pic_video_interaction_median = Column(Integer, nullable=False, default=0, comment='日常图文+视频互动中位数')
    daily_pic_video_interaction_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文+视频互动率')
    daily_pic_video_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文+视频百赞笔记比例')
    daily_pic_video_hundred_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文+视频千赞笔记比例')
    daily_pic_video_completion_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文+视频完播率')
    daily_pic_video_three_sec_reading_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文+视频图文3秒阅读率')

    # 合作图文+视频相关字段
    cooperation_pic_video_exposure_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频曝光中位数')
    cooperation_pic_video_reading_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频阅读中位数')
    cooperation_pic_video_interaction_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频互动中位数')
    cooperation_pic_video_interaction_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频互动率')
    cooperation_pic_video_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频百赞笔记比例')
    cooperation_pic_video_hundred_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频千赞笔记比例')
    cooperation_pic_video_completion_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频完播率')
    cooperation_pic_video_three_sec_reading_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频图文3秒阅读率')

    cooperation_pic_video_zr_exposure_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频曝光中位数')
    cooperation_pic_video_zr_reading_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频阅读中位数')
    cooperation_pic_video_zr_interaction_median = Column(Integer, nullable=False, default=0, comment='合作图文+视频互动中位数')
    cooperation_pic_video_zr_interaction_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频互动率')
    cooperation_pic_video_zr_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频百赞笔记比例')
    cooperation_pic_video_zr_hundred_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频千赞笔记比例')
    cooperation_pic_video_zr_completion_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频完播率')
    cooperation_pic_video_zr_three_sec_reading_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='合作图文+视频图文3秒阅读率')

    # 日常图文相关字段
    daily_pic_text_exposure_median = Column(Integer, nullable=False, default=0, comment='日常图文曝光中位数')
    daily_pic_text_reading_median = Column(Integer, nullable=False, default=0, comment='日常图文阅读中位数')
    daily_pic_text_interaction_median = Column(Integer, nullable=False, default=0, comment='日常图文互动中位数')
    daily_pic_text_interaction_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文互动率')
    daily_pic_text_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文百赞笔记比例')
    daily_pic_text_hundred_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文千赞笔记比例')
    daily_pic_text_completion_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文完播率')
    daily_pic_text_three_sec_reading_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常图文3秒阅读率')
    daily_pic_text_notenumber = Column(Integer, nullable=False, default=0, comment='日常图文笔记数')

    # CPE、CPM、CPC、CPR相关字段
    daily_pic_text_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文CPE')
    daily_pic_text_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文CPM')
    daily_pic_text_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文CPC')
    daily_pic_text_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文CPR')

    # 合作图文相关字段
    coop_pic_text_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPE')
    coop_pic_text_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPM')
    coop_pic_text_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPC')
    coop_pic_text_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPR')

    # 合作图文视频相关字段
    cooperation_pic_video_video_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPE')
    cooperation_pic_video_video_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPC')
    cooperation_pic_video_video_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPM')
    cooperation_pic_video_video_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPR')
    cooperation_pic_video_text_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPE')
    cooperation_pic_video_text_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPC')
    cooperation_pic_video_text_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPM')
    cooperation_pic_video_text_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPR')

    cooperation_pic_video_zr_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPE')
    cooperation_pic_video_zr_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPC')
    cooperation_pic_video_zr_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPM')
    cooperation_pic_video_zr_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作图文CPR')

    # 日常视频相关字段
    daily_video_hundred_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常视频百赞笔记比例')
    daily_video_thousand_likes_note_ratio = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常视频千赞笔记比例')
    daily_video_three_sec_reading_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常视频3秒阅读率')
    daily_video_notenumber = Column(Integer, nullable=False, default=0, comment='日常视频笔记数')
    daily_video_completion_rate = Column(DECIMAL(10, 4), nullable=False, default=0, comment='日常视频完播率')

    # 日常图文视频文本相关字段
    daily_pic_video_text_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频文本CPE')
    daily_pic_video_text_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频文本CPC')
    daily_pic_video_text_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频文本CPM')
    daily_pic_video_text_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频文本CPR')

    # 日常视频成本指标
    daily_video_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常视频CPE')
    daily_video_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常视频CPC')
    daily_video_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常视频CPM')
    daily_video_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常视频CPR')

    # 合作视频成本指标
    coop_video_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPE')
    coop_video_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPC')
    coop_video_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPM')
    coop_video_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='合作视频CPR')

    # 日常图文视频成本指标
    daily_pic_video_video_cpe = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频CPE')
    daily_pic_video_video_cpc = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频CPC')
    daily_pic_video_video_cpm = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频CPM')
    daily_pic_video_video_cpr = Column(DECIMAL(10, 2), nullable=False, default=0, comment='日常图文视频CPR')

class AdminUserGroup(Base):
    """管理员用户组表"""
    __tablename__ = 'admin_user_groups'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False, comment='分组名')
    p_id = Column(Integer, nullable=False, default=0, comment='父级分类')
    proportion = Column(Numeric(5, 2), nullable=False, default=-1.00, comment='分成比例')
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    level = Column(Integer, nullable=True, comment='用户组层级')

class KolBusinessMediaAccount(Base):
    __tablename__ = 'kol_business_media_accounts'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    media_account_id = Column(Integer, nullable=False, unique=True, comment='小红书账号')
    business_id = Column(Integer, nullable=True, comment='商务')
    director_id = Column(Integer, nullable=True, comment='编导')
    lead_director_id = Column(Integer, nullable=True, comment='主编/签约人')
    created_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True)
    editor = Column(Integer, nullable=True, comment='剪辑')
    talent_id = Column(Integer, nullable=True, comment='达人')
    deputy_director_id = Column(Integer, nullable=True)
    is_valid_business = Column(Integer, nullable=False, default=0, comment='是否离职：0-非离职，1-离职')

class AdminUser(Base):
    """管理员用户表"""
    __tablename__ = 'admin_users'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, comment='用户名')
    username = Column(String(255), nullable=False, comment='用户名')
    phone = Column(String(255), nullable=False, comment='手机号')
    avatar = Column(String(255), nullable=False, comment='头像')
    # email = Column(String(255), nullable=False, unique=True, comment='邮箱')
    password = Column(String(255), nullable=False, comment='密码')
    group = Column(Integer, nullable=True, comment='用户组')
    pm_group = Column(Integer, nullable=True, comment='PM组')
    remember_token = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    # status = Column(Integer, default=1, comment='状态 1:启用 0:禁用')

class BloggerSigningHistory(Base):
    """博主签约历史记录表"""
    __tablename__ = 'blogger_signing_and_cancellation_history'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    creator_id = Column(BigInteger, nullable=False, comment='博主ID')
    operator_id = Column(BigInteger, nullable=True, comment='操作人ID')
    action_time = Column(DateTime, nullable=False, default=datetime.now, comment='操作时间')
    status = Column(Integer, nullable=False, comment='状态 1:签约 0:解约')
    group = Column(Integer, nullable=True, comment='组别')
    created_at = Column(TIMESTAMP, nullable=True)
    updated_at = Column(TIMESTAMP, nullable=True)

class KolOrder(Base):
    __tablename__ = 'kol_orders'
    
    # 主键
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # 基本信息
    record_id_no = Column(String(255), comment='飞书记录ID')
    no = Column(String(255), unique=True, nullable=False)
    sub_id = Column(Integer, nullable=False, default=0)
    admin_user_id = Column(Integer, nullable=False)
    director_id = Column(Integer, nullable=False)
    media_account_id = Column(Integer, nullable=False)
    
    # 订单信息
    brand = Column(String(255))
    category = Column(String(255), nullable=False)
    type = Column(String(255), nullable=False, comment='合作形式')
    price = Column(Integer, nullable=False)
    rebate_price = Column(DECIMAL(10, 2), nullable=False, default=0.00)
    final_price = Column(DECIMAL(10, 2), nullable=False, default=0.00)
    source = Column(String(255))
    stage = Column(Integer, nullable=False, default=0)
    
    # 时间相关
    closed_at = Column(DateTime)
    published_at = Column(DateTime)
    canceled_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # 取消原因和快递信息
    cancel_reason = Column(String(255))
    express_no = Column(String(255), comment='快递单号')
    
    # 创建者信息
    shangwu_creator = Column(Integer)
    director_creator = Column(Integer)
    talent_creator = Column(Integer, nullable=False, default=0)
    editor_creator = Column(Integer, nullable=False, default=0)
    
    # 联系信息
    wechat = Column(String(255))
    location = Column(String(255))
    company = Column(String(255))
    contacts = Column(String(255))
    contact_nickname = Column(String(255))
    
    # 发布相关
    pre_schedule = Column(DateTime)
    likes = Column(Integer, nullable=False, default=0)
    is_valid = Column(Integer, nullable=False, default=0)
    image = Column(String(255))
    publish_talent = Column(Integer, default=0)
    publish_editor = Column(Integer, default=0)
    publish_director = Column(Integer)
    publish_expired_time = Column(DateTime)
    published_data_time = Column(DateTime)
    
    # 财务相关
    input = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='投流金额')
    cost = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='制作成本')
    profit = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='利润')
    is_rebate = Column(Integer, nullable=False, default=0)
    is_finalized = Column(Integer, nullable=False, default=0)
    
    # 主管相关
    lead_director_creator = Column(Integer, nullable=False, default=0)
    lead_director_id = Column(Integer, nullable=False, default=0)
    company_id = Column(Integer)
    
    # 返点和溢价
    rebate_rate = Column(DECIMAL(20, 4), nullable=False, default=0.0000)
    rebate_rate_new = Column(DECIMAL(5, 2), comment='年框返点比例')
    premium_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000, comment='溢价')
    
    # 文章和平台
    article_url = Column(String(1024))
    platform_id = Column(Integer, nullable=False, default=1)
    
    # 利润相关
    company_profit = Column(DECIMAL(20, 4), nullable=False, default=0.0000)
    contracted_profit = Column(DECIMAL(20, 4), nullable=False, default=0.0000)
    brand_id = Column(Integer, nullable=False, default=0)
    
    # 其他状态
    valid_time = Column(DateTime)
    deputy_director_id = Column(Integer)
    is_timeout = Column(Integer, nullable=False, default=0)
    remark = Column(String(255))
    lock_profit_fields = Column(Boolean, nullable=False, default=False)
    
    # 小红书相关
    red_book_order = Column(String(100), comment='小红书订单号')
    
    # 商务助理相关
    business_assistant_create = Column(Integer, comment='商务助理')
    business_assistant_jiekuan = Column(Integer, comment='商务助理')
    
    # 金额相关
    total_price = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='累计金额')
    
    # 年框相关
    year_frame_type_id = Column(Integer, nullable=False, default=0, comment='年框类型主键id字段')
    year_frame_type = Column(Integer, nullable=False, default=0, comment='年框类型字段')
    
    # 商务提成相关
    shangwu_rate = Column(DECIMAL(10, 2), comment='商务提成比例')
    shangwu_commission_rate = Column(DECIMAL(10, 2), comment='商务提成发放比例')
    shangwu_due_commission = Column(DECIMAL(10, 2), comment='商务应发提成')
    shangwu_paid_commission = Column(DECIMAL(10, 2), comment='商务实发提成')
    
    # 备注相关
    cw_remark = Column(Text, comment='财务备注')
    rebate_amount = Column(Text, comment='实际返点金额')
    
    # 分组相关
    xiaohongshu_account_group = Column(String(255), comment='小红书账号分组')
    completed_business_group = Column(String(255), comment='成单商务分组')
    settlement_business_group = Column(String(255), comment='结款商务分组')
    
    # 是否打包相关
    dabao = Column(String(255), nullable=False, default='非打包', comment='是否打包')
    is_first = Column(String(255), nullable=False, default='否', comment='是否是首单')
    packaging_amount = Column(DECIMAL(8, 2), nullable=False, default=0.00, comment='打包累计金额')
    
    # 新增字段
    is_zero_price_declaration = Column(Boolean, nullable=False, default=False, comment='是否是0元报备 1:是 0：否')
    
    # 补发相关
    assistant_reissue_commission = Column(DECIMAL(10, 2), comment='表助理补发提成金额')
    settlement_assistant_reissue_commission = Column(DECIMAL(10, 2), comment='结款助理补发提成金额')
    reissue_amount = Column(DECIMAL(10, 2), comment='补发金额')
    
    # 其他
    notesign = Column(String(255), comment='博主所在机构')
    record_id = Column(String(50), nullable=False, default='', comment='飞书记录ID')
    gross_profit_margin = Column(DECIMAL(8, 2), nullable=False, default=0.00, comment='毛利润率')
    ticheng_updates = Column(Integer, nullable=False, default=0, comment='是否已更新商务提成 1-是 2-否')
    relation_id = Column(String(255), comment='关联ID')
    history_rebate_rate = Column(DECIMAL(8, 2), comment='历史返点比例')

    def __repr__(self):
        return f"<KolOrder(id={self.id}, no='{self.no}')>"

class TrainingBloggerDetails(Base):
    __tablename__ = 'training_blogger_details'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = Column(String(244), nullable=True)  # 小红书ID
    nickname = Column(String(255), nullable=True)  # 昵称
    blogger_dandelion_id = Column(String(100), nullable=False)  # 荷公英ID
    followers_count = Column(Integer, nullable=False, default=0)  # 粉丝量
    organization_name = Column(String(255), nullable=True)  # 机构名称
    graphic_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文报价
    video_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频报价
    current_note_count = Column(Integer, nullable=False, default=0)  # 当前笔记总数
    graphic_orders_count = Column(Integer, nullable=False, default=0)  # 图文商单数量
    video_orders_count = Column(Integer, nullable=False, default=0)  # 视频商单数量
    tags = Column(String(255), nullable=True)  # 标签
    graphic_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文总营收
    video_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频总营收
    total_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 总营收
    page = Column(Integer, nullable=True)  # 页数
    month = Column(String(7), nullable=True)  # 月份（格式如2024-06）
    created_at = Column(TIMESTAMP, nullable=False)  # 创建时间
    updated_at = Column(TIMESTAMP, nullable=False)  # 更新时间
    is_updated = Column(Integer, nullable=False, default=0)  # 是否更新
    intro = Column(Text, nullable=True)  # 简介
    type = Column(Integer, nullable=False, default=0)  # 1=图文2000-5000, 2=图文5001+, 3=视频2000-5000, 4=视频5001+
    status = Column(Integer, nullable=False, default=0)  # 0=未更新 1=已更新

class TrainingBloggers(Base):
    __tablename__ = 'training_bloggers'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = Column(String(244), nullable=True)  # 小红书ID
    nickname = Column(String(255), nullable=True)  # 昵称
    blogger_dandelion_id = Column(String(100), nullable=False)  # 荷公英ID
    followers_count = Column(Integer, nullable=False, default=0)  # 粉丝量
    organization_name = Column(String(255), nullable=True)  # 机构名称
    graphic_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文报价
    video_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频报价
    current_note_count = Column(Integer, nullable=False, default=0)  # 当前笔记总数
    graphic_orders_count = Column(Integer, nullable=False, default=0)  # 图文商单数量
    video_orders_count = Column(Integer, nullable=False, default=0)  # 视频商单数量
    tags = Column(String(255), nullable=True)  # 标签
    graphic_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文总营收
    video_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频总营收
    total_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 总营收
    page = Column(Integer, nullable=True)  # 页数
    month = Column(String(7), nullable=True)  # 月份（格式如2024-06）
    created_at = Column(TIMESTAMP, nullable=False)  # 创建时间
    updated_at = Column(TIMESTAMP, nullable=False)  # 更新时间
    is_updated = Column(Integer, nullable=False, default=0)  # 是否更新
    intro = Column(Text, nullable=True)  # 简介
    type = Column(Integer, nullable=False, default=0)  # 1=图文2000-5000, 2=图文5001+, 3=视频2000-5000, 4=视频5001+
    status = Column(Integer, nullable=False, default=0)  # 0=未更新 1=已更新

class SpiderQianguaHotNote(Base):
    __tablename__ = 'fp_spider_qiangua_hot_note'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    kol_name = Column(String(128), comment='博主名称')
    note_title = Column(String(128), comment='笔记标题')
    xiaohongshu_user_id = Column(String(32), comment='博主user_id')
    xiaohongshu_note_id = Column(String(32), comment='笔记小红书id')
    xsec_token = Column(String(256), comment='笔记token')
    kol_img = Column(String(128), comment='博主头像')
    kol_image_id = Column(String(128), comment='博主头像id')
    kol_type = Column(String(128), comment='博主类型')
    note_like = Column(BigInteger, comment='点赞')
    note_collect = Column(BigInteger, comment='收藏')
    note_issue_time = Column(String(128), comment='笔记发布时间')
    note_comment = Column(BigInteger, comment='评论')
    note_share = Column(BigInteger, comment='分享')
    note_read = Column(BigInteger, comment='预估阅读')
    note_interact = Column(BigInteger, comment='互动量')
    note_classify = Column(String(128), comment='笔记分类')
    note_image = Column(String(255), comment='笔记封面')
    note_image_id = Column(String(255), comment='笔记封面id')
    note_tags = Column(String(255), comment='笔记标签')
    note_tag_classify = Column(String(128), comment='笔记标签分类')
    hot_note_24h = Column(String(128), comment='排名时间（近24小时）')
    note_type = Column(Integer, comment='笔记类型')
    hot_date = Column(Integer, comment='日期0点时间戳')
    status = Column(Integer, comment='抓取状态')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')
    video_url = Column(String(255), comment='视频URL')
    note_video_text = Column(Text, comment='视频文本内容')

class TrainingBloggerDetailsPeizhi(Base):
    __tablename__ = 'training_blogger_details_peizhi'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255))
    password = Column(String(255))
    month = Column(String(255))
    start_id = Column(Integer)
    end_id = Column(Integer)
    video_price = Column(Integer)
    graphic_price = Column(Integer)
    created_time = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class KolFangpianPrice(Base):
    __tablename__ = 'kol_fangpian_price'

    id = Column(Integer, primary_key=True, autoincrement=True)
    kol_id = Column(String(255), nullable=True, comment='KOL唯一ID')
    redId = Column(String(255), nullable=True, comment='小红书ID')
    kol_name = Column(String(255), nullable=True, comment='KOL名称')
    picture_price = Column(Integer, nullable=True, comment='图文报价')
    video_price = Column(Integer, nullable=True, comment='视频报价')
    currentLevel = Column(Integer, nullable=True, comment='当前等级')
    created_time = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    kol_fans = Column(String(255), nullable=True, comment='粉丝数量')
    kol_gender = Column(String(10), nullable=True, comment='性别')
    kol_location = Column(String(255), nullable=True, comment='地区')
    xhs_link = Column(String(255), nullable=True, comment='小红书链接')
    pgy_link = Column(String(255), nullable=True, comment='蒲公英链接')
    kol_tag = Column(String(255), nullable=True, comment='KOL标签')
    kol_like = Column(String(255), nullable=True, comment='点赞数')
    mcn_name = Column(String(255), nullable=True, comment='点赞数')

class KolMediaAccountsConfig(Base):
    __tablename__ = 'kol_media_accounts_config'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255))
    password = Column(String(255))
    pageSize = Column(Integer)
    client_id = Column(Integer)
    name = Column(String(255))
    remark = Column(String(255))
    created_time = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class TrainingBloggerDetailsSpider(Base):
    __tablename__ = 'training_blogger_details_spider'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255))
    password = Column(String(255))
    start_id = Column(Integer)
    end_id = Column(Integer)
    spider_id = Column(Integer)
    total = Column(Integer)
    graphic_price = Column(Integer)
    created_time = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    remark = Column(String(255))

class KolProfileDataWaicaiPeizhiPeizhi(Base):
    __tablename__ = 'kol_profile_data_waicai_peizhi'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255))
    password = Column(String(255))
    start_id = Column(Integer)
    end_id = Column(Integer)
    created_time = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class DouYinAuthorInfo(Base):
    __tablename__ = 'douyin_author_info'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(32), unique=True, nullable=False, comment='抖音用户ID')
    user_name = Column(String(255), comment='用户名称')
    user_head_logo = Column(String(512), comment='用户头像')
    user_gender = Column(String(10), comment='用户性别')
    user_location = Column(String(255), comment='用户地址')
    user_introduction = Column(Text, comment='用户简介')
    fans_count = Column(BigInteger, comment='粉丝数')
    like_count = Column(BigInteger, comment='获赞数')
    item_count = Column(Integer, comment='作品数')
    first_tag_name = Column(String(255), comment='一级标签')
    second_tag_name = Column(String(255), comment='二级标签')
    user_aweme_url = Column(String(512), comment='抖音主页链接')
    profile_url = Column(String(512), comment='巨量算数主页链接')
    create_time = Column(DateTime, default=datetime.now, comment='创建时间')
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class DouYinVideoInfo(Base):
    __tablename__ = 'douyin_video_info'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(32), nullable=False, comment='抖音用户ID')
    picture = Column(String(512), comment='视频封面')
    video_text = Column(Text, comment='视频文案')
    like_cnt = Column(BigInteger, comment='点赞数')
    coment_cnt = Column(BigInteger, comment='评论数')
    share_cnt = Column(BigInteger, comment='分享数')
    follow_cnt = Column(BigInteger, comment='关注数')
    video_url = Column(String(512), comment='视频链接')
    rank = Column(Integer, comment='排名')
    item_id = Column(String(32), unique=True, comment='视频ID')
    create_time = Column(DateTime, comment='视频发布时间')
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

class DouYinKolRealization2(Base):
    """抖音KOL变现数据表"""
    __tablename__ = 'douyin_kol_realization2'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键ID')
    douyin_user_id = Column(String(255), nullable=True, comment='抖音用户ID')
    douyin_nickname = Column(String(255), nullable=True, comment='抖音用户昵称')
    create_time = Column(Integer, nullable=True, comment='创建时间')
    update_time = Column(Integer, nullable=True, comment='更新时间')
    douyin_link = Column(Text, nullable=True, comment='抖音链接')
    
    # 新增字段 - 作者显示检查API数据 (check_author_display)
    follower_count = Column(Integer, nullable=True, comment='粉丝数量')
    link_count = Column(Integer, nullable=True, comment='链接数量')
    videos_count = Column(Integer, nullable=True, comment='视频数量')
    
    # 新增字段 - 作者链接结构API数据 (author_link_struct)
    link_struct = Column(Text, nullable=True, comment='链接结构JSON数据')
    
    # 新增字段 - 作者平台渠道信息API数据 (get_author_platform_channel_info_v2)
    self_intro = Column(Text, nullable=True, comment='自我介绍')
    
    # 新增字段 - 作者商业传播信息API数据 (get_author_commerce_spread_info)
    commerce_info = Column(Text, nullable=True, comment='商业传播信息JSON数据')
    
    # 新增字段 - 作者基本信息API数据 (get_author_base_info)
    author_base_info = Column(Text, nullable=True, comment='作者基本信息JSON数据')
    
    # 新增字段 - 营销信息API数据 (get_author_marketing_info)
    industry_tags = Column(Text, nullable=True, comment='行业标签JSON数据')
    price_info = Column(Text, nullable=True, comment='价格信息JSON数据')
    commerce_seed_info = Column(Text, nullable=True, comment='价格信息JSON数据')
    spread_info = Column(Text, nullable=True, comment='价格信息JSON数据')


class DouYinKolRealization(Base):
    """抖音KOL变现数据表"""
    __tablename__ = 'douyin_kol_realization'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键ID')
    douyin_user_id = Column(String(255), nullable=True, comment='抖音用户ID')
    douyin_nickname = Column(String(255), nullable=True, comment='抖音用户昵称')
    avg_a3_incr_cnt = Column(String(255), nullable=True, comment='抖音用户昵称')
    create_time = Column(Integer, nullable=True, comment='创建时间')
    update_time = Column(Integer, nullable=True, comment='更新时间')
    douyin_link = Column(Text, nullable=True, comment='抖音链接')

    # 新增字段 - 作者显示检查API数据 (check_author_display)
    follower_count = Column(Integer, nullable=True, comment='粉丝数量')
    link_count = Column(Integer, nullable=True, comment='链接数量')
    videos_count = Column(Integer, nullable=True, comment='视频数量')

    # 新增字段 - 作者链接结构API数据 (author_link_struct)
    link_struct = Column(Text, nullable=True, comment='链接结构JSON数据')

    # 新增字段 - 作者平台渠道信息API数据 (get_author_platform_channel_info_v2)
    self_intro = Column(Text, nullable=True, comment='自我介绍')

    # 新增字段 - 作者商业传播信息API数据 (get_author_commerce_spread_info)
    commerce_info = Column(Text, nullable=True, comment='商业传播信息JSON数据')

    # 新增字段 - 作者基本信息API数据 (get_author_base_info)
    author_base_info = Column(Text, nullable=True, comment='作者基本信息JSON数据')

    # 新增字段 - 营销信息API数据 (get_author_marketing_info)
    industry_tags = Column(Text, nullable=True, comment='行业标签JSON数据')
    price_info = Column(Text, nullable=True, comment='价格信息JSON数据')
    spread_info = Column(Text, nullable=True, comment='价格信息JSON数据')
    audience_distribution = Column(Text, nullable=True, comment='价格信息JSON数据')

class DouYinKolNote(Base):
    """抖音KOL笔记数据表"""
    __tablename__ = 'douyin_kol_note'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键ID')
    douyin_user_id = Column(String(255), nullable=True, comment='抖音用户ID')
    douyin_item_id = Column(String(255), nullable=True, unique=True, comment='抖音作品ID')
    douyin_item_date = Column(String(255), nullable=True, comment='作品发布日期')
    douyin_item_title = Column(String(255), nullable=True, comment='作品标题')
    video_like = Column(Integer, nullable=True, comment='点赞数')
    video_play = Column(Integer, nullable=True, comment='播放数')
    video_share = Column(Integer, nullable=True, comment='分享数')
    video_comment = Column(Integer, nullable=True, comment='评论数')
    create_time = Column(Integer, nullable=True, comment='创建时间')
    update_time = Column(Integer, nullable=True, comment='更新时间')
    
    # 新增字段 - latest_item_info 和 latest_star_item_info 数据
    core_user_id = Column(String(255), nullable=True, comment='核心用户ID')
    create_timestamp = Column(BigInteger, nullable=True, comment='创建时间戳')
    duration = Column(Integer, nullable=True, comment='视频时长(秒)')
    duration_min = Column(Integer, nullable=True, comment='视频时长(分钟)')
    head_image_uri = Column(Text, nullable=True, comment='头图URI')
    is_hot = Column(Boolean, nullable=True, default=False, comment='是否热门')
    is_playlet = Column(Integer, nullable=True, default=0, comment='是否短剧')
    item_animated_cover = Column(Text, nullable=True, comment='动态封面')
    item_cover = Column(Text, nullable=True, comment='封面图')
    media_type = Column(String(10), nullable=True, comment='媒体类型')
    original_status = Column(Integer, nullable=True, comment='原创状态')
    status = Column(Integer, nullable=True, default=1, comment='状态')
    title = Column(Text, nullable=True, comment='标题')
    url = Column(Text, nullable=True, comment='视频URL')
    video_id = Column(String(255), nullable=True, comment='视频ID')

class FpOutBloggerInfo(Base):
    """小红书博主信息表 - 外采数据"""
    __tablename__ = 'fp_out_blogger_info'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    userId = Column(String(32), nullable=True)
    valid = Column(Integer, nullable=True)
    headPhoto = Column(String(128), nullable=True)
    name = Column(String(32), nullable=True)
    redId = Column(String(32), nullable=True)
    location = Column(String(16), nullable=True)
    homePageDisplay = Column(Integer, nullable=True)
    travelAreaList = Column(String(255), nullable=True)
    type = Column(String(255), nullable=True)
    personalTags = Column(String(255), nullable=True)
    fansCount = Column(Integer, nullable=True)
    likeCollectCountInfo = Column(Integer, nullable=True)
    businessNoteCount = Column(Integer, nullable=True)
    totalNoteCount = Column(Integer, nullable=True)
    recommend = Column(String(64), nullable=True)
    picturePrice = Column(Integer, nullable=True)
    videoPrice = Column(Integer, nullable=True)
    lowerPrice = Column(Integer, nullable=True)
    userType = Column(Integer, nullable=True)
    showPrice = Column(Integer, nullable=True)
    pictureState = Column(Integer, nullable=True)
    videoState = Column(Integer, nullable=True)
    isCollect = Column(Boolean, nullable=True)
    cooperateState = Column(Boolean, nullable=True)
    pictureInCart = Column(Boolean, nullable=True)
    videoInCart = Column(Boolean, nullable=True)
    note = Column(String(64), nullable=True)
    live = Column(String(32), nullable=True)
    cps = Column(String(32), nullable=True)
    noteSign = Column(Text, nullable=True)
    liveSign = Column(Text, nullable=True)
    goodRecommendPermission = Column(String(16), nullable=True)
    cpc = Column(String(16), nullable=True)
    pictureCpcBasePrice = Column(Integer, nullable=True)
    pictureCpcPerPrice = Column(Integer, nullable=True)
    pictureCpcEstimateNum = Column(Integer, nullable=True)
    videoCpcState = Column(Boolean, nullable=True)
    videoCpcBasePrice = Column(Integer, nullable=True)
    videoCpcPerPrice = Column(Integer, nullable=True)
    videoCpcEstimateNum = Column(Integer, nullable=True)
    pictureCpcInCart = Column(Boolean, nullable=True)
    videoCpcInCart = Column(Boolean, nullable=True)
    contentTags = Column(Text, nullable=True)
    featureTags = Column(Text, nullable=True)
    industryTag = Column(String(32), nullable=True)
    gender = Column(String(4), nullable=True)
    picPriceRemind = Column(String(16), nullable=True)
    videoPriceRemind = Column(String(16), nullable=True)
    currentLevel = Column(Integer, nullable=True)
    nextLevel = Column(Integer, nullable=True)
    priceState = Column(Integer, nullable=True)
    resemblance = Column(Boolean, nullable=True)
    noteList = Column(String(255), nullable=True)
    tradeType = Column(String(256), nullable=True)
    clickMidNum = Column(Integer, nullable=True)
    clickMidNumMcn = Column(Integer, nullable=True)
    interMidNum = Column(Integer, nullable=True)
    fansNum = Column(Integer, nullable=True)
    matchNoteNumber = Column(Integer, nullable=True)
    authorityList = Column(String(255), nullable=True)
    processingAuthorities = Column(String(255), nullable=True)
    pictureShowState = Column(Boolean, nullable=True)
    videoShowState = Column(Boolean, nullable=True)
    classifyCode = Column(String(32), nullable=True)
    predictiveExposure = Column(String(255), nullable=True)
    efficiencyValidUser = Column(String(255), nullable=True)
    pictureReadCost = Column(String(255), nullable=True)
    videoReadCost = Column(String(255), nullable=True)
    pictureClickMidNum = Column(Integer, nullable=True)
    pictureInterMidNum = Column(Integer, nullable=True)
    videoClickMidNum = Column(Integer, nullable=True)
    videoFinishRate = Column(String(255), nullable=True)
    videoInterMidNum = Column(Integer, nullable=True)
    fans30GrowthRate = Column(String(255), nullable=True)
    fans30GrowthNum = Column(Integer, nullable=True)
    nextPicturePrice = Column(String(255), nullable=True)
    nextVideoPrice = Column(String(255), nullable=True)
    fansRiseNum = Column(String(255), nullable=True)
    fansEngageNum = Column(String(255), nullable=True)
    overflowNum = Column(String(255), nullable=True)
    newHighQuality = Column(Integer, nullable=True)
    isIndustryRecommend = Column(String(255), nullable=True)
    picturePriceGtZero = Column(String(255), nullable=True)
    videoPriceGtZero = Column(String(255), nullable=True)
    lowActive = Column(String(255), nullable=True)
    fansActiveIn28dLv = Column(String(255), nullable=True)
    fansEngageNum30dLv = Column(String(255), nullable=True)
    hundredLikePercent30 = Column(String(255), nullable=True)
    thousandLikePercent30 = Column(String(255), nullable=True)
    pictureHundredLikePercent30 = Column(String(255), nullable=True)
    pictureThousandLikePercent30 = Column(String(255), nullable=True)
    videoHundredLikePercent30 = Column(String(255), nullable=True)
    videoThousandLikePercent30 = Column(String(255), nullable=True)
    cooperType = Column(Integer, nullable=True)
    buyerType = Column(Integer, nullable=True)
    promiseImpNum = Column(String(255), nullable=True)
    kolType = Column(Integer, nullable=True)
    showPromiseTag = Column(Integer, nullable=True)
    activityList = Column(String(255), nullable=True)
    controlState = Column(Integer, nullable=True)
    clothingIndustryPrice = Column(Text, nullable=True)
    fbState = Column(Integer, nullable=True)
    forecastReadUvLower = Column(Integer, nullable=True)
    forecastReadUvUpper = Column(Integer, nullable=True)
    forecastGroupCoverRateLower = Column(Integer, nullable=True)
    forecastGroupCoverRateUpper = Column(Integer, nullable=True)
    intentionInfo = Column(String(255), nullable=True)
    seedAndHarcest = Column(String(255), nullable=True)
    liveImg = Column(String(255), nullable=True)
    liveId = Column(String(255), nullable=True)
    liveGMV = Column(String(255), nullable=True)
    isStar = Column(String(255), nullable=True)
    top2CategoryList = Column(String(255), nullable=True)
    hasBuyerAuth = Column(String(255), nullable=True)
    sellerRealIncomeAmt90d = Column(Integer, nullable=True)
    estimatePictureCpm = Column(Integer, nullable=True)
    estimatePictureCpmCompare = Column(Integer, nullable=True)
    estimateVideoCpm = Column(Integer, nullable=True)
    estimateVideoCpmCompare = Column(Integer, nullable=True)
    estimatePictureEngageCost = Column(Integer, nullable=True)
    estimatePictureEngageCostCompare = Column(Integer, nullable=True)
    estimateVideoEngageCost = Column(Integer, nullable=True)
    estimateVideoEngageCostCompare = Column(Integer, nullable=True)
    inviteReply48hNumRatio = Column(Integer, nullable=True)
    recommendReason = Column(String(255), nullable=True)
    kolHeadLabel = Column(Integer, nullable=True)
    accumCoopImpMedinNum30d = Column(Integer, nullable=True)
    estimateCpuv30d = Column(Integer, nullable=True)
    accumPicCommonImpMedinNum30d = Column(Integer, nullable=True)
    accumVideoCommonImpMedinNum30d = Column(Integer, nullable=True)
    accumCommonImpMedinNum30d = Column(Integer, nullable=True)
    marketTarget = Column(String(255), nullable=True)
    readMidCoop30 = Column(String(255), nullable=True)
    interMidCoop30 = Column(String(255), nullable=True)
    mEngagementNum = Column(Integer, nullable=True)
    mEngagementNumMcn = Column(Integer, nullable=True)
    mCpuvNum30d = Column(Integer, nullable=True)
    user_desc = Column(String(255), nullable=True)
    source_type = Column(String(255), nullable=True)
    type = Column(Integer, default=1)

class FpForeignSummaryStatement(Base):
    """外采总表刊例库"""
    __tablename__ = 'fp_foreign_summary_statement'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增主键')
    is_quality = Column(Boolean, nullable=True, comment='是否优质，0/1')
    assigned_person = Column(String(64), nullable=True, comment='分配人员')
    pgy_link = Column(String(255), nullable=True, comment='蒲公英链接')
    kol_id = Column(String(64), nullable=True, comment='博主id')
    xiaohongshu_link = Column(String(255), nullable=True, comment='小红书链接')
    kol_nickname = Column(String(128), nullable=True, comment='达人昵称')
    kol_persona = Column(String(128), nullable=True, comment='博主人设')
    note_last_publish_time = Column(Integer, nullable=True, comment='笔记最后发布时间')
    kol_wechat = Column(String(64), nullable=True, comment='微信号')
    rebate_level = Column(String(32), nullable=True, comment='返点等级')
    content_type = Column(Text, nullable=True, comment='内容类型')
    appearance_env = Column(String(128), nullable=True, comment='出镜环境')
    content_form = Column(String(64), nullable=True, comment='内容形式')
    ip_address = Column(String(64), nullable=True, comment='IP地址（只写国外）')
    school = Column(String(128), nullable=True, comment='学校')
    home_furnishing = Column(String(128), nullable=True, comment='（可嵌入）家居')
    child_age = Column(String(64), nullable=True, comment='孩子岁数（母婴）')
    tags = Column(String(255), nullable=True, comment='标签')
    graphic_price = Column(DECIMAL(12, 2), nullable=True, comment='图文价格')
    video_price = Column(DECIMAL(12, 2), nullable=True, comment='视频价格')
    graphic_order_count = Column(Integer, nullable=True, comment='图文商单量')
    video_order_count = Column(Integer, nullable=True, comment='视频商单量')
    graphic_revenue = Column(DECIMAL(14, 2), nullable=True, comment='图文营收')
    monthly_revenue = Column(DECIMAL(14, 2), nullable=True, comment='月总营收')
    content_category1 = Column(String(64), nullable=True, comment='内容类目1')
    read_source_discovery_rate = Column(DECIMAL(5, 2), nullable=True, comment='阅读量来源的发现页占比(%)')
    exposure_source_discovery_rate = Column(DECIMAL(5, 2), nullable=True, comment='曝光量来源的发现页占比(%)')
    content_category2 = Column(String(64), nullable=True, comment='内容类目2')
    content_ratio1 = Column(DECIMAL(5, 2), nullable=True, comment='内容占比1(%)')
    content_ratio2 = Column(DECIMAL(5, 2), nullable=True, comment='内容占比2(%)')
    kol_phone = Column(String(64), nullable=True, comment='博主手机号')
    fangpian_wechat = Column(String(64), nullable=True, comment='方片微信号')
    parent_record = Column(String(64), nullable=True, comment='父记录')
    xiaohongshu_id = Column(String(64), nullable=True, comment='小红书ID')
    kol_fans_count = Column(BigInteger, nullable=True, comment='粉丝数')
    kol_likes_count = Column(BigInteger, nullable=True, comment='赞藏数')
    region = Column(String(64), nullable=True, comment='所在地区')
    brands_cooperated = Column(Text, nullable=True, comment='已合作品牌')
    cooperation_date = Column(DateTime, nullable=True, comment='合作日期')
    daily_exposure_median = Column(Integer, nullable=True, comment='日常图文+视频曝光中位数')
    daily_read_median = Column(Integer, nullable=True, comment='日常图文+视频阅读中位数')
    daily_interaction_median = Column(Integer, nullable=True, comment='日常图文+视频互动中位数')
    daily_interaction_rate = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频互动率')
    daily_thousand_like_note_ratio = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频千赞笔记比例')
    daily_completion_rate = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频完播率')
    daily_3sec_read_rate = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频3秒阅读率')
    daily_graphic_cpe = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-图文CPE')
    daily_graphic_cpm = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-图文CPM')
    daily_graphic_cpc = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-图文CPC')
    daily_graphic_cpr = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-图文CPR')
    daily_video_cpm = Column(DECIMAL(20, 10), nullable=True, comment='日常图文+视频-视频CPM')
    daily_video_cpc = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-视频CPC')
    daily_video_cpr = Column(DECIMAL(10, 4), nullable=True, comment='日常图文+视频-视频CPR')
    cooperation_exposure_median = Column(Integer, nullable=True, comment='合作图文+视频曝光中位数')
    cooperation_read_median = Column(Integer, nullable=True, comment='合作图文+视频阅读中位数')
    cooperation_interaction_median = Column(Integer, nullable=True, comment='合作图文+视频互动中位数')
    cooperation_interaction_rate = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频互动率')
    cooperation_thousand_like_note_ratio = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频千赞笔记比例')
    cooperation_completion_rate = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频完播率')
    cooperation_3sec_read_rate = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频3秒阅读率')
    cooperation_graphic_cpe = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-图文CPE')
    cooperation_graphic_cpm = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-图文CPM')
    cooperation_graphic_cpc = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-图文CPC')
    cooperation_graphic_cpr = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-图文CPR')
    cooperation_video_cpe = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-视频CPE')
    cooperation_video_cpm = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-视频CPM')
    cooperation_video_cpc = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-视频CPC')
    cooperation_video_cpr = Column(DECIMAL(10, 4), nullable=True, comment='合作图文+视频-视频CPR')
    daily_90d_exposure_median = Column(Integer, nullable=True, comment='90天日常视频+图文曝光中位数')
    daily_90d_read_median = Column(Integer, nullable=True, comment='90天日常视频+图文阅读中位数')
    daily_90d_interaction_median = Column(Integer, nullable=True, comment='90天日常视频+图文互动中位数')
    daily_90d_interaction_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文互动率')
    daily_90d_thousand_like_note_ratio = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文千赞笔记比例')
    daily_90d_completion_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文完播率')
    daily_90d_3sec_read_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文3秒阅读率')
    daily_90d_graphic_cpe = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-图文CPE')
    daily_90d_graphic_cpm = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-图文CPM')
    daily_90d_graphic_cpc = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-图文CPC')
    daily_90d_graphic_cpr = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-图文CPR')
    cooperation_90d_exposure_median = Column(Integer, nullable=True, comment='90天合作图文+视频曝光中位数')
    cooperation_90d_read_median = Column(Integer, nullable=True, comment='90天合作图文+视频阅读中位数')
    cooperation_90d_interaction_median = Column(Integer, nullable=True, comment='90天合作图文+视频互动中位数')
    cooperation_90d_interaction_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频互动率')
    cooperation_90d_thousand_like_note_ratio = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频千赞笔记比例')
    cooperation_90d_completion_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频完播率')
    cooperation_90d_3sec_read_rate = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频3秒阅读率')
    cooperation_90d_graphic_cpe = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-图文CPE')
    cooperation_90d_graphic_cpm = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-图文CPM')
    cooperation_90d_graphic_cpc = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-图文CPC')
    cooperation_90d_graphic_cpr = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-图文CPR')
    cooperation_90d_video_cpe = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-视频CPE')
    cooperation_90d_video_cpm = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-视频CPM')
    cooperation_90d_video_cpc = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-视频CPC')
    cooperation_90d_video_cpr = Column(DECIMAL(10, 4), nullable=True, comment='90天合作图文+视频-视频CPR')
    daily_90d_video_cpe = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-视频CPE')
    daily_90d_video_cpm = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-视频CPM')
    daily_90d_video_cpc = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-视频CPC')
    daily_90d_video_cpr = Column(DECIMAL(10, 4), nullable=True, comment='90天日常视频+图文-视频CPR')
    age_under_18 = Column(Integer, nullable=True, comment='年龄小于18')
    age_18_24 = Column(Integer, nullable=True, comment='年龄18-24')
    age_25_34 = Column(Integer, nullable=True, comment='年龄25-34')
    age_35_44 = Column(Integer, nullable=True, comment='年龄35-44')
    age_over_44 = Column(Integer, nullable=True, comment='年龄大于44')
    female_fan_ratio = Column(DECIMAL(5, 2), nullable=True, comment='女粉丝占比(%)')
    user_interest_top1 = Column(String(64), nullable=True, comment='用户兴趣top1')
    user_interest_top2 = Column(String(64), nullable=True, comment='用户兴趣top2')
    user_interest_top3 = Column(String(64), nullable=True, comment='用户兴趣top3')
    user_interest_top4 = Column(String(64), nullable=True, comment='用户兴趣top4')
    user_interest_top5 = Column(String(64), nullable=True, comment='用户兴趣top5')
    province_top1 = Column(String(64), nullable=True, comment='省份top1')
    province_top2 = Column(String(64), nullable=True, comment='省份top2')
    province_top3 = Column(String(64), nullable=True, comment='省份top3')
    city_top1 = Column(String(64), nullable=True, comment='城市top1')
    city_top2 = Column(String(64), nullable=True, comment='城市top2')
    city_top3 = Column(String(64), nullable=True, comment='城市top3')
    device_top1 = Column(String(64), nullable=True, comment='设备top1')
    device_top2 = Column(String(64), nullable=True, comment='设备top2')
    device_top3 = Column(String(64), nullable=True, comment='设备top3')
    note_publish_count = Column(Integer, nullable=True, comment='发布笔记数量')
    fan_increment = Column(Integer, nullable=True, comment='粉丝增量')
    fan_change_rate = Column(DECIMAL(10, 2), nullable=True, comment='粉丝量变化幅度')
    active_fan_ratio = Column(DECIMAL(10, 2), nullable=True, comment='活跃粉丝占比')
    sign_time = Column(DateTime, nullable=True, comment='签约时间')
    unsign_time = Column(DateTime, nullable=True, comment='解约时间')
    resign_time = Column(DateTime, nullable=True, comment='可回签时间')
    update_time = Column(DateTime, nullable=True, comment='修改时间')
    create_time = Column(DateTime, nullable=True, comment='创建时间')
    vertical_tags = Column(String(255), nullable=True, comment='垂类细分标签')
    system_order_total = Column(Integer, nullable=True, comment='系统订单总数')
    account_level = Column(String(64), nullable=True, comment='账号等级')
    offline_store_visit = Column(Boolean, nullable=True, comment='线下探店')
    est_read_price_graphic = Column(DECIMAL(10, 2), nullable=True, comment='预估阅读单价（图文）')
    est_read_price_video = Column(DECIMAL(10, 2), nullable=True, comment='预估阅读单价（视频）')
    order_count_30d = Column(Integer, nullable=True, comment='30天内系统订单数')
    order_count_90d = Column(Integer, nullable=True, comment='90天内系统订单数')
    pgy_order_total = Column(Integer, nullable=True, comment='蒲公英商单总数')
    pgy_order_count_30d = Column(Integer, nullable=True, comment='30天蒲公英商单数')
    pgy_order_count_90d = Column(Integer, nullable=True, comment='90天蒲公英商单数')
    business_tags_xg = Column(String(255), nullable=True, comment='修哥组商务标签')
    child1_gender = Column(String(10), nullable=True, comment='孩子1性别')
    child1_age = Column(String(10), nullable=True, comment='孩子1年龄')
    child2_gender = Column(String(10), nullable=True, comment='孩子2性别')
    child2_age = Column(String(10), nullable=True, comment='孩子2年龄')
    child3_gender = Column(String(10), nullable=True, comment='孩子3性别')
    child3_age = Column(String(10), nullable=True, comment='孩子3年龄')
    business_tags_my = Column(String(255), nullable=True, comment='沐远组商务标签')
    can_install_ac = Column(Boolean, nullable=True, comment='能否接空调挂机')
    can_install_ac_outdoor = Column(Boolean, nullable=True, comment='能否挂空调外机')
    has_fridge_scene = Column(Boolean, nullable=True, comment='是否有冰箱的全嵌入场景')
    fridge_scene_size = Column(String(64), nullable=True, comment='冰箱的全嵌入场景尺寸')
    has_washer_scene = Column(Boolean, nullable=True, comment='是否洗衣机全嵌入场景')
    washer_scene_size = Column(String(64), nullable=True, comment='洗衣机全嵌入场景尺寸')
    has_dishwasher_scene = Column(Boolean, nullable=True, comment='是否有洗碗机全嵌入场景')
    dishwasher_scene_size = Column(String(64), nullable=True, comment='洗碗机全嵌入场景尺寸')
    has_oven_scene = Column(Boolean, nullable=True, comment='是否有蒸烤箱的全嵌入场景')
    oven_scene_size = Column(String(64), nullable=True, comment='蒸烤箱的全嵌入场景尺寸')
    can_connect_power_water = Column(Boolean, nullable=True, comment='能否通电通水')
    can_drill_holes = Column(Boolean, nullable=True, comment='能否接受打孔')
    can_install_water_purifier = Column(Boolean, nullable=True, comment='接受净饮机上墙')
    can_install_tv = Column(Boolean, nullable=True, comment='接受电视上墙')
    gender = Column(String(10), nullable=True, comment='性别')
    is_new_kol = Column(Boolean, nullable=True, comment='新晋博主')
    kol_type = Column(String(64), nullable=True, comment='博主类型')
    order_natural_read_median = Column(Integer, nullable=True, comment='商单自然流阅读中位数')
    order_natural_interaction_median = Column(Integer, nullable=True, comment='商单自然流互动中位数')
    training_category = Column(String(64), nullable=True, comment='培训所属分类')
    cooperation_form = Column(String(64), nullable=True, comment='合作形式')
    kol_sign_type = Column(String(64), nullable=True, comment='博主签约类型')
    kol_sign_time = Column(DateTime, nullable=True, comment='博主签约时间')
    has_wechat = Column(Boolean, nullable=True, comment='是否有微信')
    total_interaction_median = Column(Integer, nullable=True, comment='汇总互动中位数')
    total_cpe = Column(DECIMAL(10, 2), nullable=True, comment='汇总CPE')
    children_summary = Column(String(255), nullable=True, comment='孩子汇总')
    total_90d_cpe = Column(DECIMAL(10, 2), nullable=True, comment='90天汇总CPE')
    fans_count_w = Column(DECIMAL(10, 2), nullable=True, comment='(w)粉丝数')
    likes_count_w = Column(DECIMAL(10, 2), nullable=True, comment='(w)赞藏')
    device_top1_ratio = Column(DECIMAL(5, 2), nullable=True, comment='设备top1百分比')
    device_top2_ratio = Column(DECIMAL(5, 2), nullable=True, comment='设备top2百分比')
    device_top3_ratio = Column(DECIMAL(5, 2), nullable=True, comment='设备top3百分比')
    nationality = Column(String(64), nullable=True, comment='国籍')
    tier1_city_ratio = Column(DECIMAL(5, 2), nullable=True, comment='一线城市占比')
    device_top1_name = Column(String(64), nullable=True, comment='设备top1名称')
    device_top2_name = Column(String(64), nullable=True, comment='设备top2名称')
    device_top3_name = Column(String(64), nullable=True, comment='设备top3名称')
    vertical_label = Column(String(255), nullable=True, comment='垂类标签')
    kol_level = Column(String(64), nullable=True, comment='博主分级')
    daily_like_median = Column(Integer, nullable=True, comment='日常图文+视频点赞中位数')
    daily_collect_median = Column(Integer, nullable=True, comment='日常图文+视频收藏中位数')
    daily_comment_median = Column(Integer, nullable=True, comment='日常图文+视频评论中位数')
    daily_share_median = Column(Integer, nullable=True, comment='日常图文+视频分享中位数')
    daily_follow_median = Column(Integer, nullable=True, comment='日常图文+视频关注中位数')
    cooperation_like_median = Column(Integer, nullable=True, comment='合作图文+视频点赞中位数')
    cooperation_collect_median = Column(Integer, nullable=True, comment='合作图文+视频收藏中位数')
    cooperation_comment_median = Column(Integer, nullable=True, comment='合作图文+视频评论中位数')
    cooperation_share_median = Column(Integer, nullable=True, comment='合作图文+视频分享中位数')
    cooperation_follow_median = Column(Integer, nullable=True, comment='合作图文+视频关注中位数')
    daily_90d_like_median = Column(Integer, nullable=True, comment='90天日常视频+视频点赞中位数')
    daily_90d_collect_median = Column(Integer, nullable=True, comment='90天日常视频+视频收藏中位数')
    daily_90d_comment_median = Column(Integer, nullable=True, comment='90天日常视频+视频评论中位数')
    daily_90d_share_median = Column(Integer, nullable=True, comment='90天日常视频+视频分享中位数')
    daily_90d_follow_median = Column(Integer, nullable=True, comment='90天日常视频+视频关注中位数')
    cooperation_90d_like_median = Column(Integer, nullable=True, comment='90天合作图文+视频点赞中位数')
    cooperation_90d_collect_median = Column(Integer, nullable=True, comment='90天合作图文+视频收藏中位数')
    cooperation_90d_comment_median = Column(Integer, nullable=True, comment='90天合作图文+视频评论中位数')
    cooperation_90d_share_median = Column(Integer, nullable=True, comment='90天合作图文+视频分享中位数')
    cooperation_90d_follow_median = Column(Integer, nullable=True, comment='90天合作图文+视频关注中位数')
    daily_read_better_than_peers = Column(Boolean, nullable=True, comment='日常阅读中位数优于同类博主')
    daily_interaction_better_than_peers = Column(Boolean, nullable=True, comment='日常互动中位数优于同类博主')
    cooperation_read_better_than_peers = Column(Boolean, nullable=True, comment='合作阅读中位数优于同类博主')
    cooperation_interaction_better_than_peers = Column(Boolean, nullable=True, comment='合作互动中位数优于同类博主')
    note_total_count = Column(Integer, nullable=True, comment='笔记总数')
    graphic_note_ratio = Column(DECIMAL(5, 2), nullable=True, comment='图文笔记占比(%)')
    video_note_ratio = Column(DECIMAL(5, 2), nullable=True, comment='视频笔记占比(%)')

class FpOutBloggerFansHistory(Base):
    """小红书博主粉丝历史数据表"""
    __tablename__ = 'fp_out_blogger_fans_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    num = Column(Integer, nullable=True)
    dateKey = Column(String(255), nullable=True)
    user_id = Column(String(255), nullable=True)
    date_type = Column(Integer, nullable=True)
    increase_type = Column(Integer, nullable=True)
    updated_at = Column(String(255), nullable=True)

class FpOutBloggerFansSummary(Base):
    """小红书博主信息表 - 外采数据"""
    __tablename__ = 'fp_out_blogger_fans_summary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fansNum = Column(Integer)  # 粉丝总数
    fansIncreaseNum = Column(Integer)  # 粉丝增加数量
    fansGrowthRate = Column(Double)  # 粉丝增长率
    fansGrowthBeyondRate = Column(Double)  # 粉丝增长超越率
    activeFansL28 = Column(Integer)  # 最近28天活跃粉丝数
    activeFansRate = Column(Double)  # 活跃粉丝比例
    activeFansBeyondRate = Column(Double)  # 活跃粉丝超越率
    engageFansRate = Column(Double)  # 互动粉丝比例
    engageFansL30 = Column(Integer)  # 最近30天互动粉丝数
    engageFansBeyondRate = Column(Double)  # 互动粉丝超越率
    readFansIn30 = Column(Integer)  # 最近30天阅读粉丝数
    readFansRate = Column(Double)  # 阅读粉丝比例
    readFansBeyondRate = Column(Double)  # 阅读粉丝超越率
    payFansUserRate30d = Column(Double)  # 近30天付费粉丝比例
    payFansUserNum30d = Column(Integer)  # 近30天付费粉丝数
    user_id = Column(String(255), index=True)  # 用户ID

class FpOutBloggerFansProfile(Base):
    """小红书博主信息表 - 外采数据"""
    __tablename__ = 'fp_out_blogger_fans_profile'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(64))
    ages = Column(Text)
    gender = Column(Text)
    interests = Column(Text)
    provinces = Column(Text)
    cities = Column(Text)
    dateKey = Column(String(64))
    devices = Column(Text)

class DouyinBianxian(Base):
    """小红书博主信息表 - 外采数据"""
    __tablename__ = 'z_douyin_bianxian'

    id = Column(Integer, primary_key=True, autoincrement=True)
    kol_name = Column(String(255))
    douyin_link = Column(Text)
    status = Column(Integer)
class FpOutBloggerNoteDetail(Base):
    """小红书博主笔记详情数据表"""
    __tablename__ = 'fp_out_blogger_note_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    readNum = Column(Integer, nullable=True)
    likeNum = Column(Integer, nullable=True)
    collectNum = Column(Integer, nullable=True)
    isAdvertise = Column(String(255), nullable=True)
    isVideo = Column(String(255), nullable=True)
    noteId = Column(String(255), nullable=True)
    imgUrl = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    brandName = Column(String(255), nullable=True)
    date = Column(String(255), nullable=True)
    user_id = Column(String(255), nullable=True)
    advertise_switch = Column(Integer, nullable=True)
    order_type = Column(Integer, nullable=True)
    note_type = Column(Integer, nullable=True)

class FpOutBloggerCostEffective(Base):
    """小红书博主成本效益数据表"""
    __tablename__ = 'fp_out_blogger_cost_effective'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(255), nullable=True)
    date = Column(String(255), nullable=True)
    estimateVideoCpm = Column(DECIMAL(10, 2), nullable=True, comment='预估视频CPM')
    pictureReadCost = Column(DECIMAL(2, 0), nullable=True, comment='图文阅读成本')
    videoReadCost = Column(DECIMAL(2, 0), nullable=True, comment='视频阅读成本')

class FpPgyInvitationsMessage(Base):
    """蒲公英邀请消息表"""
    __tablename__ = 'fp_pgy_invitations_message'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    status = Column(Integer)
    platform_user_id = Column(String(255))
    platform_message_id = Column(String(255))
    platform_kol_id = Column(String(255))
    platform_nickname = Column(String(255))
    platform_content = Column(Text)
    created_at = Column(DateTime)

class FpPgyInvitationsInfo(Base):
    """蒲公英邀请信息详情表"""
    __tablename__ = 'fp_pgy_invitations_info'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    invitation_details = Column(Text, nullable=False, comment='邀约详情')
    company_remarks = Column(Text, nullable=True, comment='公司备注')
    recorded_at = Column(TIMESTAMP, nullable=True)
    blogger_name = Column(String(255), nullable=True, comment='博主名称')
    blogger_link = Column(String(255), nullable=True, comment='博主链接')
    blogger_intent = Column(String(255), nullable=True, comment='博主意向')
    brand_name = Column(String(255), nullable=True, comment='品牌名')
    cooperation_type = Column(String(255), nullable=True, comment='合作类型')
    product_name = Column(String(255), nullable=True, comment='产品名称')
    expected_publish_period_start = Column(DateTime, nullable=True, comment='期望发布开始时间')
    expected_publish_period_end = Column(DateTime, nullable=True, comment='期望发布结束时间')
    cooperation_content = Column(Text, nullable=True, comment='合作内容介绍')
    contact_information = Column(String(255), nullable=True, comment='联系方式')
    invitation_initiation_time = Column(DateTime, nullable=True, comment='邀约发起时间')
    organization = Column(String(255), nullable=True, comment='机构')
    clue_type = Column(String(255), nullable=True, comment='线索类型')
    platform_user_id = Column(String(255), nullable=True, comment='蒲公英id')
    account_source = Column(String(255), nullable=True, comment='账号来源')
    message_id = Column(String(255), nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    is_handle = Column(Integer, default=2, comment='已处理：1，未处理：2')

class FpOutBloggerCooperatorV2(Base):
    """小红书博主合作者V2数据表"""
    __tablename__ = 'fp_out_blogger_cooperator_v2'

    id = Column(Integer, primary_key=True, autoincrement=True)
    accumCommonImpMedinNum30d = Column(Integer, nullable=True, comment='30天日常曝光中位数')
    accumCoopImpMedinNum30d = Column(Integer, nullable=True, comment='30天合作曝光中位数')
    accumPicCommonImpMedinNum30d = Column(Integer, nullable=True, comment='30天图文日常曝光中位数')
    accumVideoCommonImpMedinNum30d = Column(Integer, nullable=True, comment='30天视频日常曝光中位数')
    businessNoteCount = Column(Integer, nullable=True, comment='商业笔记数量')
    buyerType = Column(Integer, nullable=True, comment='买家类型')
    classifyCode = Column(Integer, nullable=True, comment='分类代码')
    clickMidNum = Column(Integer, nullable=True, comment='点击中位数')
    clickMidNumMcn = Column(Integer, nullable=True, comment='MCN点击中位数')
    cooperType = Column(Integer, nullable=True, comment='合作类型')
    cooperateState = Column(Integer, nullable=True, comment='合作状态')
    currentLevel = Column(Integer, nullable=True, comment='当前等级')
    efficiencyValidUser = Column(Integer, nullable=True, comment='有效用户数')
    estimateCpuv30d = Column(DECIMAL(10, 2), nullable=True, comment='预估30天CPUV')
    estimatePictureCpm = Column(DECIMAL(10, 2), nullable=True, comment='预估图文CPM')
    estimatePictureCpmCompare = Column(DECIMAL(10, 2), nullable=True, comment='预估图文CPM对比')
    estimatePictureEngageCost = Column(DECIMAL(10, 2), nullable=True, comment='预估图文互动成本')
    estimatePictureEngageCostCompare = Column(DECIMAL(10, 2), nullable=True, comment='预估图文互动成本对比')
    estimateVideoCpm = Column(DECIMAL(10, 2), nullable=True, comment='预估视频CPM')
    estimateVideoCpmCompare = Column(DECIMAL(10, 2), nullable=True, comment='预估视频CPM对比')
    estimateVideoEngageCost = Column(DECIMAL(10, 2), nullable=True, comment='预估视频互动成本')
    estimateVideoEngageCostCompare = Column(DECIMAL(10, 2), nullable=True, comment='预估视频互动成本对比')
    fans30GrowthNum = Column(Integer, nullable=True, comment='30天粉丝增长数')
    fans30GrowthRate = Column(DECIMAL(10, 2), nullable=True, comment='30天粉丝增长率')
    fansActiveIn28dLv = Column(DECIMAL(10, 2), nullable=True, comment='28天粉丝活跃度')
    fansEngageNum30dLv = Column(DECIMAL(10, 2), nullable=True, comment='30天粉丝互动率')
    fansNum = Column(Integer, nullable=True, comment='粉丝数')
    featureTags = Column(String(500), nullable=True, comment='特征标签')
    gender = Column(String(10), nullable=True, comment='性别')
    hasBuyerAuth = Column(Boolean, nullable=True, comment='是否有买家权限')
    headPhoto = Column(String(500), nullable=True, comment='头像URL')
    homePageDisplay = Column(Integer, nullable=True, comment='主页展示')
    hundredLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天百赞率')
    interMidCoop30 = Column(Integer, nullable=True, comment='30天合作互动中位数')
    interMidNum = Column(Integer, nullable=True, comment='互动中位数')
    inviteReply48hNumRatio = Column(DECIMAL(10, 2), nullable=True, comment='48小时回复率')
    isCollect = Column(Boolean, nullable=True, comment='是否收藏')
    isIndustryRecommend = Column(Boolean, nullable=True, comment='是否行业推荐')
    kolHeadLabel = Column(Integer, nullable=True, comment='KOL头部标签')
    kolType = Column(Integer, nullable=True, comment='KOL类型')
    location = Column(String(100), nullable=True, comment='地理位置')
    lowActive = Column(Boolean, nullable=True, comment='是否低活跃')
    lowerPrice = Column(Integer, nullable=True, comment='最低价格')
    mEngagementNum = Column(Integer, nullable=True, comment='互动数')
    mEngagementNumMcn = Column(Integer, nullable=True, comment='MCN互动数')
    matchNoteNumber = Column(Integer, nullable=True, comment='匹配笔记数')
    name = Column(String(255), nullable=True, comment='博主昵称')
    personalTags = Column(String(500), nullable=True, comment='个人标签')
    pictureClickMidNum = Column(Integer, nullable=True, comment='图文点击中位数')
    pictureCpcBasePrice = Column(String(50), nullable=True, comment='图文CPC基础价格')
    pictureHundredLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天图文百赞率')
    pictureInterMidNum = Column(Integer, nullable=True, comment='图文互动中位数')
    picturePrice = Column(Integer, nullable=True, comment='图文价格')
    picturePriceGtZero = Column(Boolean, nullable=True, comment='图文价格是否大于0')
    pictureReadCost = Column(String(50), nullable=True, comment='图文阅读成本')
    pictureState = Column(Integer, nullable=True, comment='图文状态')
    pictureThousandLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天图文千赞率')
    priceState = Column(Integer, nullable=True, comment='价格状态')
    readMidCoop30 = Column(Integer, nullable=True, comment='30天合作阅读中位数')
    redId = Column(String(50), nullable=True, comment='红人ID')
    sellerRealIncomeAmt90d = Column(Integer, nullable=True, comment='90天卖家实际收入')
    showPrice = Column(Boolean, nullable=True, comment='是否显示价格')
    showPromiseTag = Column(Integer, nullable=True, comment='是否显示承诺标签')
    thousandLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天千赞率')
    tradeType = Column(String(500), nullable=True, comment='交易类型')
    userId = Column(String(50), nullable=True, index=True, comment='用户ID')
    userType = Column(Integer, nullable=True, comment='用户类型')
    valid = Column(Integer, nullable=True, comment='是否有效')
    videoClickMidNum = Column(Integer, nullable=True, comment='视频点击中位数')
    videoCpcBasePrice = Column(String(50), nullable=True, comment='视频CPC基础价格')
    videoFinishRate = Column(DECIMAL(10, 2), nullable=True, comment='视频完播率')
    videoHundredLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天视频百赞率')
    videoInterMidNum = Column(Integer, nullable=True, comment='视频互动中位数')
    videoPrice = Column(Integer, nullable=True, comment='视频价格')
    videoPriceGtZero = Column(Boolean, nullable=True, comment='视频价格是否大于0')
    videoReadCost = Column(String(50), nullable=True, comment='视频阅读成本')
    videoState = Column(Integer, nullable=True, comment='视频状态')
    videoThousandLikePercent30 = Column(DECIMAL(10, 2), nullable=True, comment='30天视频千赞率')
    created_at = Column(DateTime, server_default=func.now(), nullable=True, comment='创建时间')
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=True, comment='更新时间')
    type = Column(String(255), nullable=True)
    status = Column(Boolean, server_default=text("0"), nullable=True, comment='同步状态，0：未开始，1：进行中，2：已完成')
    task_id = Column(BigInteger, nullable=True, comment='任务ID')

class Sheet(Base):
    """小红书博主信息表 - 外采数据"""
    __tablename__ = 'sheet1'

    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(String(32), nullable=True)
    name = Column(String(32), nullable=True)
    redId = Column(String(32), nullable=True)
    location = Column(String(16), nullable=True)
    gender = Column(String(4), nullable=True)
    contentTags = Column(Text, nullable=True)
    likeCollectCountInfo = Column(String(255), nullable=True)
    user_desc = Column(String(255), nullable=True)
    fansCount = Column(Integer, nullable=True)
    picturePrice = Column(Integer, nullable=True)
    videoPrice = Column(Integer, nullable=True)
    noteSign = Column(String(255), nullable=True, comment='博主所在机构')


class FpOutBloggerNoteRate(Base):
    """小红书博主笔记数据统计表"""
    __tablename__ = 'fp_out_blogger_note_rate'

    id = Column(Integer, primary_key=True, autoincrement=True)
    noteNumber = Column(Integer, nullable=True, comment='笔记数量')
    videoNoteNumber = Column(Integer, nullable=True, comment='视频笔记数量')
    hundredLikePercent = Column(Float, nullable=True, comment='百赞比例')
    thousandLikePercent = Column(Float, nullable=True, comment='千赞比例')
    noteType = Column(Integer, nullable=True, comment='笔记类型')
    tradeNames = Column(Text, nullable=True, comment='行业名称')
    impMedian = Column(Integer, nullable=True, comment='曝光中位数')
    impMedianBeyondRate = Column(Float, nullable=True, comment='曝光中位数超出率')
    readMedian = Column(Integer, nullable=True, comment='阅读中位数')
    readMedianBeyondRate = Column(Float, nullable=True, comment='阅读中位数超出率')
    interactionMedian = Column(Integer, nullable=True, comment='互动中位数')
    interactionRate = Column(Float, nullable=True, comment='互动率')
    interactionBeyondRate = Column(Float, nullable=True, comment='互动超出率')
    likeMedian = Column(Integer, nullable=True, comment='点赞中位数')
    collectMedian = Column(Integer, nullable=True, comment='收藏中位数')
    commentMedian = Column(Integer, nullable=True, comment='评论中位数')
    shareMedian = Column(Integer, nullable=True, comment='分享中位数')
    videoFullViewRate = Column(Float, nullable=True, comment='视频完播率')
    videoFullViewBeyondRate = Column(Float, nullable=True, comment='视频完播超出率')
    picture3sViewRate = Column(Float, nullable=True, comment='图文3秒观看率')
    notes = Column(Text, nullable=True, comment='笔记详情')
    pagePercentVo = Column(Text, nullable=True, comment='页面百分比')
    longTermCommonNoteVo = Column(Text, nullable=True, comment='长期日常笔记')
    longTermCooperateNoteVo = Column(Text, nullable=True, comment='长期合作笔记')
    mEngagementNum = Column(Integer, nullable=True, comment='互动数')
    mFollowCnt = Column(Integer, nullable=True, comment='关注数')
    user_id = Column(String(255), nullable=True, comment='用户ID')
    note_type = Column(Integer, nullable=True, comment='笔记类型')
    date_type = Column(Integer, nullable=True, comment='日期类型')
    advertise_switch = Column(Integer, nullable=True, comment='广告开关')
    business = Column(Integer, nullable=True, comment='业务类型')

class FpOutBloggerDataSummary(Base):
    """小红书博主数据汇总表 - 外采数据"""
    __tablename__ = 'fp_out_blogger_data_summary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    _id = Column(String(255))
    mValidRawReadFeedNum = Column(String(255))
    mEngagementNumCompare = Column(String(255))
    picReadCost = Column(String(255))
    noteType = Column(String(255))
    readMedianBeyondRate = Column(String(255))
    responseRate = Column(String(255))
    videoReadBeyondRate = Column(String(255))
    kolAdvantageHover = Column(String(255))
    estimateVideoEngageCostCompare = Column(String(255))
    estimateVideoEngageCost = Column(String(255))
    estimatePictureCpuvCompare = Column(String(255))
    noteNumber = Column(String(255))
    interactionMedian = Column(String(255))
    pictureReadBeyondRate = Column(String(255))
    kolAdvantage = Column(String(255))
    estimatePictureCpmCompare = Column(String(255))
    estimatePictureCpm = Column(String(255))
    estimateVideoCpmCompare = Column(String(255))
    picReadCostCompare = Column(String(255))
    isActive = Column(String(255))
    videoReadCost = Column(String(255))
    fans30GrowthRate = Column(String(255))
    mAccumImpCompare = Column(String(255))
    mValidRawReadFeedCompare = Column(String(255))
    estimatePictureCpuv = Column(String(255))
    pictureCase = Column(String(255))
    estimatePictureEngageCostCompare = Column(String(255))
    dateKey = Column(String(255))
    pictureReadCost = Column(String(255))
    fans30GrowthBeyondRate = Column(String(255))
    mCpuvNum = Column(String(255))
    mCpuvNumCompare = Column(String(255))
    estimateVideoCpuvCompare = Column(String(255))
    interactionBeyondRate = Column(String(255))
    easyConnect = Column(String(255))
    mAccumImpNum = Column(String(255))
    estimateVideoCpm = Column(String(255))
    estimatePictureEngageCost = Column(String(255))
    mEngagementNum = Column(String(255))
    tradeNames = Column(String(255))
    readMedian = Column(String(255))
    activeDayInLast7 = Column(String(255))
    inviteNum = Column(String(255))
    mEngagementNumOld = Column(String(255))
    videoReadCostV2 = Column(String(255))
    videoReadCostCompare = Column(String(255))
    estimateVideoCpuv = Column(String(255))
    videoCase = Column(String(255))
    creator_id = Column(String(255))
    platform_user_id = Column(String(255))
    create_time = Column(String(255))
    sync_status = Column(String(255))
    source_type = Column(String(255))
    task_id = Column(String(255))
    client_task_log_id = Column(String(255))
    client_id = Column(String(255))
    created_at = Column(String(255))
    updated_at = Column(String(255))
    type = Column(Integer, default=1)
    
class KolProfileDataWaicai(Base):
    __tablename__ = 'kol_profile_data_waicai'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    blogger_id = Column(String(64), comment='博主ID')
    dandelion_link = Column(Text, comment='蒲公英链接')
    xhs_link = Column(Text, comment='小红书链接')
    nickname = Column(String(100), comment='达人昵称')
    xhs_id = Column(String(100), comment='小红书ID')
    fans_count = Column(Integer, comment='粉丝量')
    tags = Column(Text, comment='标签')
    graphic_price = Column(BigInteger, comment='图文价格')
    video_price = Column(BigInteger, comment='视频价格')
    like_collect_count = Column(Integer, comment='赞藏')
    region = Column(String(100), comment='所在地区')
    agency = Column(String(100), comment='所属机构')
    last_note_update_time = Column(DateTime, comment='最新笔记更新时间')
    video_note_ratio = Column(DECIMAL(5, 2), comment='视频笔记占比 (%)')
    note_like_median = Column(Integer, comment='笔记点赞中位数')
    reply_rate_48h = Column(DECIMAL(5, 2), comment='邀约48小时回复率 (%)')
    cooperate_industry = Column(Text, comment='合作行业')
    est_graphic_cpm = Column(DECIMAL(10, 2), comment='图文预估CPM')
    est_video_cpm = Column(DECIMAL(10, 2), comment='视频预估CPM')
    est_graphic_cpc = Column(DECIMAL(10, 4), comment='图文预估CPC')
    est_video_cpc = Column(DECIMAL(10, 4), comment='视频预估CPC')
    est_graphic_cpe = Column(DECIMAL(10, 4), comment='图文预估CPE')
    est_video_cpe = Column(DECIMAL(10, 4), comment='视频预估CPE')
    daily_exposure_median = Column(Integer, comment='日常曝光中位数')
    daily_read_median = Column(Integer, comment='日常阅读中位数')
    daily_engage_median = Column(Integer, comment='日常互动中位数')
    coop_exposure_median = Column(Integer, comment='合作曝光中位数')
    coop_read_median = Column(Integer, comment='合作阅读中位数')
    coop_engage_median = Column(Integer, comment='合作互动中位数')
    daily_cpm = Column(DECIMAL(10, 2), comment='日常图文+视频CPM')
    coop_cpm = Column(DECIMAL(10, 2), comment='合作图文+视频CPM')
    daily_cpc = Column(DECIMAL(10, 4), comment='日常图文+视频CPC')
    coop_cpc = Column(DECIMAL(10, 4), comment='合作图文+视频CPC')
    daily_cpe = Column(DECIMAL(10, 4), comment='日常图文+视频CPE')
    coop_cpe = Column(DECIMAL(10, 4), comment='合作图文+视频CPE')
    female_fans_ratio = Column(DECIMAL(5, 2), comment='女性粉丝占比 (%)')
    active_fans_ratio = Column(DECIMAL(5, 2), comment='活跃粉丝占比 (%)')
    age_lt18_ratio = Column(DECIMAL(5, 2), comment='年龄<18 (%)')
    age_18_24_ratio = Column(DECIMAL(5, 2), comment='年龄18-24 (%)')
    age_25_34_ratio = Column(DECIMAL(5, 2), comment='年龄25-34 (%)')
    age_35_44_ratio = Column(DECIMAL(5, 2), comment='年龄35-44 (%)')
    age_gt44_ratio = Column(DECIMAL(5, 2), comment='年龄>44 (%)')
    region_distribution = Column(Text, comment='地域分布')
    user_interest = Column(Text, comment='用户兴趣')
    device_type = Column(String(100), comment='设备苹果华为')
    cooperated_brands = Column(Text, comment='博主已合作品牌')
    cooperated_brands_with_date = Column(Text, comment='博主已合作品牌及合作日期')
    dandelion_status = Column(String(50), comment='蒲公英状态')
    kol_persona = Column(String(100), comment='博主人设')
    hundred_like_ratio = Column(DECIMAL(5, 2), comment='百赞比例')
    thousand_like_ratio = Column(DECIMAL(5, 2), comment='千赞比例')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    status = Column(Integer, default=0, comment='是否优质 1优 0不优质')
    spider_status = Column(Integer, default=0)
    signer_employee = Column(String(255), comment='博主人设')
    fangpian_wechat = Column(String(255), comment='方片微信')
    return_points = Column(String(255), comment='方片微信')
    wechat = Column(String(255), comment='方片微信')
    return_points_person = Column(String(255), comment='方片微信')
    kol_intro = Column(Text)

class ZWaicaiZong(Base):
    __tablename__ = 'z_waicai_zong'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    dandelion_link = Column(String(255), comment='蒲公英链接')
    blogger_id = Column(String(255), comment='博主ID')
    xhs_link = Column(String(255), comment='小红书链接')
    nickname = Column(String(255), comment='达人昵称')
    kol_persona = Column(String(255), comment='博主人设')
    last_note_update_time = Column(String(255), comment='最新笔记更新时间')
    wechat = Column(String(255), comment='微信')
    return_points = Column(String(255), comment='返点')
    tags = Column(String(255), comment='标签')
    graphic_price = Column(String(255), comment='图文价格')
    video_price = Column(String(255), comment='视频价格')
    fangpian_wechat = Column(String(255), comment='方片微信')
    xhs_id = Column(String(255), comment='小红书ID')
    fans_count = Column(String(255), comment='粉丝量')
    like_collect_count = Column(String(255), comment='赞藏')
    region = Column(String(255), comment='所在地区')
    cooperated_brands = Column(String(255), comment='博主已合作品牌')
    age_lt18_ratio = Column(String(255), comment='年龄<18 (%)')
    age_18_24_ratio = Column(String(255), comment='年龄18-24 (%)')
    age_25_34_ratio = Column(String(255), comment='年龄25-34 (%)')
    age_35_44_ratio = Column(String(255), comment='年龄35-44 (%)')
    age_gt44_ratio = Column(String(255), comment='年龄>44 (%)')
    female_fans_ratio = Column(String(255), comment='女性粉丝占比 (%)')
    active_fans_ratio = Column(String(255), comment='活跃粉丝占比 (%)')
    video_note_ratio = Column(String(255), comment='视频笔记占比 (%)')
    return_points_person = Column(String(255), comment='方片微信')

class DouyinUserList(Base):
    __tablename__ = 'douyin_user_list'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    uid = Column(String(255), nullable=True, comment='用户UID')
    nick_name = Column(String(255), nullable=True, comment='用户UID')
    douyin_name = Column(String(255), nullable=True, comment='用户UID')
    douyin_id = Column(String(255), nullable=True, comment='用户UID')
    xhs_id = Column(String(255), nullable=True, comment='用户UID')
    xhs_nickname = Column(String(255), nullable=True, comment='小红书博主名')
    xhs_url = Column(String(255), nullable=True, comment='小红书链接')
    baikes = Column(Text, nullable=True, comment='百科数据(JSON或文本)')
    challenges = Column(Text, nullable=True, comment='挑战数据(JSON或文本)')
    effects = Column(Text, nullable=True, comment='特效数据(JSON或文本)')
    is_red_uniqueid = Column(Boolean, default=False, comment='是否为红人唯一ID')
    items = Column(Text, nullable=True, comment='作品数据(JSON或文本)')
    mix_list = Column(Text, nullable=True, comment='混剪数据(JSON或文本)')
    musics = Column(Text, nullable=True, comment='音乐数据(JSON或文本)')
    position = Column(Text, nullable=True, comment='地理位置数据(JSON或文本)')
    product_info = Column(Text, nullable=True, comment='单个商品信息(JSON或文本)')
    product_list = Column(Text, nullable=True, comment='商品列表(JSON或文本)')
    shop_product_info = Column(Text, nullable=True, comment='店铺商品信息(JSON或文本)')
    uniqid_position = Column(Text, nullable=True, comment='唯一ID位置数据(JSON或文本)')
    userSubLightApp = Column(Text, nullable=True, comment='用户轻应用信息(JSON或文本)')
    user_info = Column(Text, nullable=True, comment='用户信息(JSON对象，存为字符串)')
    user_service_info = Column(Text, nullable=True, comment='用户服务信息(JSON或文本)')
    raw_response = Column(Text, nullable=True, comment='接口原始数据（user_list整条记录）')
    created_at = Column(TIMESTAMP, nullable=True, comment='创建时间')
    updated_at = Column(TIMESTAMP, nullable=True, comment='更新时间')

class DouyinSearchList(Base):
    __tablename__ = 'douyin_search_list'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    import_status = Column(BigInteger)
    attribute_datas = Column(JSON, nullable=True, comment='属性数据(JSON或文本)')
    extra_data = Column(Text, nullable=True, comment='额外数据(JSON或文本)')
    items = Column(Text, nullable=True, comment='项目数据(JSON或文本)')
    star_id = Column(String(255), comment='星标ID')
    task_infos = Column(Text, nullable=True, comment='任务信息(JSON或文本)')
    category = Column(String(100), nullable=True, comment='博主品类')
    created_at = Column(DateTime, nullable=True, comment='创建时间')
    updated_at = Column(DateTime, nullable=True, comment='更新时间')
    status = Column(Integer, nullable=True, default=0, comment='状态：0-未处理，1-处理成功，2-无创作能力')

class FpCreator(Base):
    """账号基础信息表"""
    __tablename__ = 'fp_creator'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    platform_id = Column(BigInteger, nullable=True, comment='所属平台')
    platform_user_id = Column(String(34), nullable=True, comment='平台user_id(如小红书id)')
    platform_account_id = Column(String(30), nullable=True, unique=True, comment='平台账号id(如小红书id)')
    mcn_id = Column(Integer, nullable=True, comment='所属机构')
    creator_nickname = Column(String(32), nullable=True, comment='昵称')
    creator_avatar = Column(String(128), nullable=True, comment='头像')
    creator_gender = Column(Integer, nullable=True, comment='性别')
    creator_location = Column(String(32), nullable=True, comment='地理位置')
    fans_count = Column(Integer, nullable=False, default=0, comment='粉丝数')
    like_collect_count = Column(Integer, nullable=False, default=0, comment='赞藏数量')
    business_note_count = Column(Integer, nullable=False, default=0, comment='商业笔记数量')
    picture_price = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='图文合作价格')
    video_price = Column(DECIMAL(10, 2), nullable=False, default=0.00, comment='视频合作价格')
    read_mid_num = Column(Integer, nullable=False, default=0, comment='阅读中位数')
    interact_mid_num = Column(Integer, nullable=False, default=0, comment='互动中位数')
    account_level = Column(Integer, nullable=True, comment='账号等级')
    content_field = Column(String(255), nullable=True, comment='领域标签')
    status = Column(Integer, nullable=False, default=1, comment='状态')
    delete_time = Column(Integer, nullable=False, default=0, comment='删除时间')
    create_time = Column(Integer, nullable=True, comment='创建时间')
    create_user = Column(Integer, nullable=True, comment='创建用户')
    update_time = Column(Integer, nullable=True, comment='更新时间')
    update_user = Column(Integer, nullable=True, comment='更新用户')
    picture_show_state = Column(Integer, nullable=False, default=1, comment='picture暂停接单')
    video_show_state = Column(Integer, nullable=False, default=1, comment='video暂停接单')

class Sheet1(Base):
    __tablename__ = 'Sheet1'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform_user_id = Column(String(244), nullable=True)  # 小红书ID
    kol_nickname = Column(String(255), nullable=True)  # 昵称
    create_time = Column(String(255), nullable=True)  # 昵称
    baobei_30 = Column(Integer)  # 荷公英ID
    baobei_90 = Column(Integer)  # 粉丝量

class BaokuanLink(Base):
    __tablename__ = 'baokuan_link'

    id = Column(Integer, primary_key=True)
    mid = Column(String(255))
    douyin_link = Column(String(255))
    status = Column(Integer)
    message = Column(Text)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class CreatorBusinessOut(Base):
    __tablename__ = 'fp_creator_business_out'

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform_user_id = Column(String(255), nullable=True, comment='KOL唯一ID')
    creator_mcn = Column(Integer, nullable=True, comment='博主机构')
    sign_status = Column(Integer, nullable=True, comment='博主机构')
    creator_nickname = Column(String(255), nullable=True, comment='KOL名称')
    picture_price = Column(Integer, nullable=True, comment='图文报价')
    video_price = Column(Integer, nullable=True, comment='视频报价')
    fans_count = Column(String(255), nullable=True, comment='粉丝数量')
    like_collect_count = Column(String(255), nullable=True, comment='粉丝数量')
    gender = Column(String(10), nullable=True, comment='性别')
    location = Column(String(255), nullable=True, comment='地区')
    content_tags = Column(String(255), nullable=True, comment='KOL标签')

class FpCreatorFansSummary(Base):
    """
    博主粉丝汇总数据表 (fp_creator_fans_summary)
    """
    __tablename__ = 'fp_creator_fans_summary'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    creator_id = Column(BigInteger, nullable=True, comment='博主id')
    platform_user_id = Column(String(32), nullable=True, comment='平台user_id')

    fans_increase_num = Column(Integer, nullable=True, comment='粉丝增量')
    fans_growth_rate = Column(DECIMAL(10, 2), nullable=True, comment='粉丝量变化幅度')
    active_fans_rate = Column(DECIMAL(10, 2), nullable=True, comment='活跃粉丝占比')
    read_fans_rate = Column(DECIMAL(10, 2), nullable=True, comment='阅读粉丝占比')
    engage_fans_rate = Column(DECIMAL(10, 2), nullable=True, comment='互动粉丝占比')
    pay_fans_user_rate_30d = Column(DECIMAL(10, 2), nullable=True, comment='下单粉丝占比')

    fans_growth_beyond_rate = Column(DECIMAL(10, 2), nullable=True, comment='粉丝量增幅同类比率')
    active_fans_beyond_rate = Column(DECIMAL(10, 2), nullable=True, comment='活跃粉丝同类比率')
    read_fans_beyond_rate = Column(DECIMAL(10, 2), nullable=True, comment='阅读粉丝同类比率')
    engage_fans_beyond_rate = Column(DECIMAL(10, 2), nullable=True, comment='互动粉丝同类比率')

    pay_fans_user_num_30d = Column(Integer, nullable=True, comment='30天下单粉丝数量')
    read_fans_in_30 = Column(Integer, nullable=True, comment='阅读粉丝数量30天内')
    fans_num = Column(Integer, nullable=True, comment='粉丝数量')
    engage_fans_l30 = Column(Integer, nullable=True, comment='30天内互动粉丝数量')
    active_fans_l28 = Column(Integer, nullable=True, comment='30天内活跃粉丝数量')

    create_time = Column(Integer, nullable=True, comment='创建时间')
    update_time = Column(Integer, nullable=True, comment='更新时间')

class FpCreatorNoteRate(Base):
    __tablename__ = 'fp_creator_note_rate'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform_user_id = Column(BigInteger, nullable=False)
    business = Column(BigInteger, nullable=False)

    # 中位数表现指标
    imp_median = Column(Integer)
    read_median = Column(Integer)
    mengagement_num = Column(Integer)
    like_median = Column(Integer)
    collect_median = Column(Integer)
    comment_median = Column(Integer)
    share_median = Column(Integer)
    mfollow_cnt = Column(Integer)

    interaction_rate = Column(DECIMAL(10, 4))
    video_full_view_rate = Column(DECIMAL(10, 4))
    picture3s_view_rate = Column(DECIMAL(10, 4))

    thousand_like_percent = Column(DECIMAL(10, 4))
    hundred_like_percent = Column(DECIMAL(10, 4))

    create_time = Column(Integer)
    update_time = Column(Integer)