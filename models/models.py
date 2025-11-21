from datetime import datetime

from sqlalchemy import Column, Integer, String, Numeric, DateTime, Text, TIMESTAMP, BigInteger, DECIMAL, \
    UniqueConstraint, text, Date, Index, Date, Index, SmallInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TrainingBloggerDetails(Base):
    __tablename__ = 'training_blogger_details_1月大梦拆'

    id = Column(Integer, primary_key=True)
    account_id = Column(String(256))
    nickname = Column(Integer)
    blogger_dandelion_id = Column(String(300))
    followers_count = Column(String(300))
    organization_name = Column(Integer)
    graphic_price = Column(String(256))
    video_price = Column(String(256))
    current_note_count = Column(String(256))
    graphic_orders_count = Column(String(256))
    video_orders_count = Column(String(256))
    tags = Column(String(256))
    graphic_revenue = Column(String(256))
    video_revenue = Column(String(256))
    total_revenue = Column(String(256))
    page = Column(String(256))
    month = Column(String(256))
    is_updated = Column(String(256))
    intro = Column(String(256))
    type = Column(String(256))
    updated_at = Column(String(100))
    created_at = Column(String(100))


class KolMediaAccountsJianlian(Base):
    __tablename__ = 'kol_media_accounts_jianlian'

    id = Column(Integer, primary_key=True)
    nickname = Column(String(255))
    pgy_id = Column(String(50))
    contact_info = Column(String(255))
    monthly_income = Column(Integer)
    updated_at = Column(DateTime)
    created_at = Column(DateTime)
    record_id = Column(String(50))

class FeiShuToken(Base):
    __tablename__ = 'feishu_tokens'

    id = Column(Integer, primary_key=True)
    token = Column(String(255))
    fetch_time = Column(DateTime)
    expire = Column(Integer)
    updated_at = Column(DateTime)
    created_at = Column(DateTime)


class BoZhu(Base):
    __tablename__ = '博主'

    id = Column(Integer, primary_key=True)
    pgy_id = Column(String(255))

class JiGou(Base):
    __tablename__ = '机构'

    id = Column(Integer, primary_key=True)
    jigou = Column(String(255))

class PaiMing(Base):
    __tablename__ = '排名总'

    id = Column(Integer, primary_key=True)
    达人昵称 = Column(String(255))
    达人粉丝量 = Column(Integer)
    达人所属机构 = Column(String(255))
    标签 = Column(String(100))
    图文商单数量 = Column(Integer)
    视频商单数量 = Column(Integer)
    图文营收 = Column(Numeric(10, 2))
    视频营收 = Column(Numeric(10, 2))
    月总营收 = Column(Numeric(10, 2))
    博主id = Column(String(100))
    图文价格 = Column(Numeric(10, 2))
    视频价格 = Column(Numeric(10, 2))
    所属月份 = Column(String(255))
    小红书账号 = Column(String(255))
    简介 = Column(String(255))

class PaiMing25_1(Base):
    __tablename__ = '母婴'

    id = Column(Integer, primary_key=True)
    达人昵称 = Column(String(255))
    达人粉丝量 = Column(Integer)
    达人所属机构 = Column(String(255))
    标签 = Column(String(100))
    图文商单数量 = Column(Integer)
    视频商单数量 = Column(Integer)
    图文营收 = Column(Numeric(10, 2))
    视频营收 = Column(Numeric(10, 2))
    月总营收 = Column(Numeric(10, 2))
    博主id = Column(String(100))
    图文价格 = Column(Numeric(10, 2))
    视频价格 = Column(Numeric(10, 2))
    所属月份 = Column(String(255))
    小红书账号 = Column(String(255))
    简介 = Column(String(255))

class KolMediaAccountsWaicai(Base):
    __tablename__ = 'kol_media_accounts_waicai'

    id = Column(Integer, primary_key=True)
    nickname = Column(String(255))
    user_id = Column(Integer)
    pgy_id = Column(String(50))
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    delete_type = Column(Integer)
    status = Column(Integer)


class KolMediaAccountsWaicaiInfo(Base):
    __tablename__ = 'kol_media_accounts_waicai_info'

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_update = Column(Integer)
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


class QgBloggerRank(Base):
    __tablename__ = 'qg_blogger_rank'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增ID')
    nickname = Column(String(100), nullable=False, comment='博主昵称')
    rank_number = Column(Integer, default=0, comment='排名')
    change_number = Column(Integer, default=0, comment='排名变化值')
    rank_value = Column(BigInteger, default=0, comment='榜单分值')
    rank_value_attach = Column(BigInteger, default=0, comment='附加分值')
    increase_rank_value = Column(DECIMAL(10, 2), default=0, comment='涨幅（百分比）')
    mcn_user_id = Column(String(64), comment='MCN用户ID')
    small_avatar = Column(String(255), comment='头像URL')
    blogger_tags = Column(String(255), comment='标签文本汇总')
    blogger_count = Column(Integer, default=0, comment='合作博主数量')
    note_count = Column(Integer, default=0, comment='笔记数量')
    like_collect = Column(Integer, default=0, comment='点赞收藏总数')
    fans_count = Column(Integer, default=0, comment='粉丝数')
    brand_count = Column(Integer, default=0, comment='合作品牌数')
    institute_name = Column(String(100), comment='机构名称')
    is_certification = Column(Integer, default=0, comment='是否认证：否 1是')
    current_user_is_favorite = Column(Integer, default=0, comment='当前用户是否收藏：否 1是')
    create_time = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), comment='创建时间')
    month = Column(String(20), comment='数据月份')
    update_time = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        comment='更新时间'
    )


class QgBrandInfo(Base):
    __tablename__ = 'qg_brand_info'
    __table_args__ = (
        UniqueConstraint('brand_id', name='uniq_brand_id'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增主键')
    brand_id = Column(String(500), nullable=False, comment='品牌ID（BrandId）')
    brand_id_key = Column(String(64), comment='品牌唯一Key（BrandIdKey）')
    brand_name = Column(String(255), nullable=False, comment='品牌名称（BrandName）')
    brand_logo = Column(String(500), comment='品牌Logo地址（BrandLogo）')
    brand_intro = Column(Text, comment='品牌简介（BrandIntro）')
    note_count = Column(Integer, default=0, comment='笔记数量（NoteCount）')
    active_count = Column(Integer, default=0, comment='活跃数量（ActiveCount）')
    amount_desc = Column(String(50), comment='金额描述（AmountDesc）')
    amount = Column(BigInteger, default=0, comment='实际金额数值（Amount）')
    blogger_id = Column(BigInteger, comment='关联的博主ID')
    month = Column(String(20), comment='数据月份')
    create_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), comment='创建时间')
    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), comment='更新时间')



class QgNoteInfo(Base):
    __tablename__ = 'qg_note_info'
    __table_args__ = (
        UniqueConstraint('note_id', name='uniq_note_id'),
        Index('idx_blogger_id', 'blogger_id'),
        Index('idx_date_code', 'date_code'),
        Index('idx_tag_name', 'tag_name'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增主键')
    note_id = Column(BigInteger, nullable=False, comment='笔记ID（NoteId）')
    date_code = Column(Integer, comment='日期代码（DateCode）')
    note_id_key = Column(String(64), comment='笔记唯一短Key（NoteIdKey）')
    unique_id = Column(String(64), comment='系统唯一标识Id')
    user_id = Column(String(64), comment='用户ID（UserId）')
    title = Column(String(500), comment='笔记标题')
    cover_image = Column(String(500), comment='封面图链接（CoverImage）')
    blogger_id = Column(BigInteger, comment='博主ID（BloggerId）')
    blogger_id_key = Column(String(64), comment='博主Key（BloggerIdKey）')
    blogger_nickname = Column(String(255), comment='博主昵称')
    blogger_prop = Column(String(100), comment='博主等级称号（如腰部达人）')
    publish_time = Column(DateTime, comment='发布时间（PublishTime）')
    note_type = Column(String(50), comment='笔记类型（NoteType）')
    is_business = Column(Integer, default=0, comment='是否为商业笔记（IsBusiness）')
    note_type_desc = Column(String(50), comment='笔记类型描述（NoteTypeDesc）')
    props = Column(Integer, default=0, comment='笔记附加属性（Props）')
    pub_date = Column(Date, comment='发布日期（PubDate）')
    update_time_raw = Column(DateTime, comment='原始更新时间（UpdateTime）')
    video_duration = Column(String(50), comment='视频时长（VideoDuration）')
    gender = Column(Integer, comment='作者性别（男/女/未知）')
    big_avatar = Column(String(500), comment='博主头像（大图）')
    small_avatar = Column(String(500), comment='博主头像（小图）')
    tag_name = Column(String(255), comment='笔记标签名称（TagName）')
    cooperate_binds_name = Column(String(255), comment='合作品牌名称（CooperateBindsName）')
    view_count = Column(Integer, default=0, comment='浏览量（ViewCount）')
    active_count = Column(Integer, default=0, comment='互动量（ActiveCount）')
    amount = Column(BigInteger, default=0, comment='笔记广告报价金额（Amount）')
    ad_price_desc = Column(String(100), comment='广告报价描述（AdPriceDesc，如1.9万）')
    ad_price_update_status = Column(Integer, default=0, comment='广告报价更新状态（AdPriceUpdateStatus）')
    is_ad_note = Column(Integer, default=0, comment='是否广告笔记（IsAdNote）')
    kol_id = Column(BigInteger, comment='关联的博主ID')
    month = Column(String(20), comment='数据月份')
    create_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), comment='记录创建时间')
    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), comment='记录更新时间')

class QgBrandBusinessNote(Base):
    """千瓜品牌商业笔记数据表"""
    __tablename__ = 'qg_brand_business_note'
    __table_args__ = (
        UniqueConstraint('note_id', 'keyword_tag', name='uniq_note_id_keyword'),
        Index('idx_note_id', 'note_id'),
        Index('idx_blogger_id', 'blogger_id'),
        Index('idx_date_code', 'date_code'),
        Index('idx_keyword_tag', 'keyword_tag'),
        Index('idx_brand_id', 'brand_id'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增主键')
    note_id = Column(BigInteger, nullable=False, comment='笔记ID（NoteId）')
    date_code = Column(Integer, comment='日期代码（DateCode）')
    note_id_key = Column(String(64), comment='笔记唯一短Key（NoteIdKey）')
    active_count = Column(Integer, default=0, comment='活跃数（ActiveCount）')
    ad_price = Column(BigInteger, default=0, comment='广告价格（AdPrice）')
    ad_price_desc = Column(String(100), comment='广告价格描述（AdPriceDesc）')
    ad_price_update_status = Column(Integer, default=0, comment='广告价格更新状态（AdPriceUpdateStatus）')
    blogger_id = Column(BigInteger, comment='博主ID（BloggerId）')
    blogger_id_key = Column(String(64), comment='博主Key（BloggerIdKey）')
    blogger_nickname = Column(String(255), comment='博主昵称（BloggerNickName）')
    blogger_prop = Column(String(100), comment='博主等级称号（BloggerProp）')
    collected_count = Column(Integer, default=0, comment='收藏数（CollectedCount）')
    comments_count = Column(Integer, default=0, comment='评论数（CommentsCount）')
    cooperate_binds_name = Column(String(255), comment='合作品牌名称（CooperateBindsName）')
    cover_image = Column(String(500), comment='封面图链接（CoverImage）')
    current_user_is_favorite = Column(SmallInteger, default=0, comment='当前用户是否收藏（CurrentUserIsFavorite）')
    fans = Column(Integer, default=0, comment='粉丝数（Fans）')
    is_ad_note = Column(SmallInteger, default=0, comment='是否广告笔记（IsAdNote）')
    is_business = Column(SmallInteger, default=0, comment='是否为商业笔记（IsBusiness）')
    like_collect = Column(Integer, default=0, comment='点赞收藏数（LikeCollect）')
    liked_count = Column(Integer, default=0, comment='点赞数（LikedCount）')
    monitor_id = Column(BigInteger, default=0, comment='监控ID（MonitorId）')
    note_type = Column(String(50), comment='笔记类型（NoteType）')
    price_type = Column(String(50), comment='价格类型（PriceType）')
    publish_time = Column(DateTime, comment='发布时间（PublishTime）')
    title = Column(Text, comment='发布时间（PublishTime）')
    share_count = Column(Integer, default=0, comment='分享数（ShareCount）')
    view_count = Column(Integer, default=0, comment='分享数（ShareCount）')
    small_avatar = Column(String(500), comment='博主头像（SmallAvatar）')
    tag = Column(String(255), comment='笔记标签（Tag）')
    keyword_tag = Column(String(100), comment='搜索关键词标签')
    brand_id = Column(String(100), comment='品牌ID')
    brand_name = Column(String(255), comment='品牌名称')
    create_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), comment='记录创建时间')
    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), onupdate=text('CURRENT_TIMESTAMP'), comment='记录更新时间')


class BusinessContractPhone(Base):
    __tablename__ = 'business_contract_phone'

    id = Column(Integer, primary_key=True)
    nickname = Column(String(255))
    xiaohongshu_url = Column(String(255))
    contract_person = Column(String(255))
    contract_person_feishu = Column(String(255))
    contract_person_img = Column(String(255))
    contract_person_name = Column(String(255))
    train_person_feishu = Column(String(255))
    train_person_img = Column(String(255))
    train_person_name = Column(String(255))
    contract_person_group = Column(String(255))
    contract_person_phone = Column(String(255))
    teacher = Column(String(255))
    teacher_phone = Column(String(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class FrontTow(Base):
    __tablename__ = 'front_tow'

    id = Column(Integer, primary_key=True)
    机构名称 = Column(String(255))
    业务模式 = Column(String(255))

class XiaohongshuComment(Base):
    __tablename__ = 'xiaohongshu_comment'

    id = Column(Integer, primary_key=True)
    comment_content = Column(Text)
    comment_id = Column(String(255))
    comment_time = Column(String(255))
    # reply_content = Column(String(255))
    # reply_id = Column(String(255))
    # reply_time = Column(String(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class SpiderQianguaHotNote(Base):
    __tablename__ = 'spider_qiangua_hot_note'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    kol_name = Column(String(128), comment='博主名称')
    note_title = Column(String(128), comment='笔记标题')
    xiaohongshu_user_id = Column(String(32), comment='博主user_id')
    xiaohongshu_note_id = Column(String(32), comment='笔记小红书id')
    kol_img = Column(String(128), comment='博主头像')
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
    note_tags = Column(String(255), comment='笔记标签')
    note_tag_classify = Column(String(128), comment='笔记标签分类')
    hot_note_24h = Column(String(128), comment='排名时间（近24小时）')
    note_type = Column(Integer, comment='笔记类型')
    hot_date = Column(Integer, comment='日期0点时间戳')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')

class PgyUser(Base):
    __tablename__ = 'pgy_user'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    userId = Column(String(128), comment='博主名称')
    nick_name = Column(String(128), comment='博主名称')
    xsec_token = Column(String(255), comment='笔记标题')
    team_name = Column(String(32), comment='博主user_id')
    team_boss = Column(String(32), comment='笔记小红书id')
    douyin_xsec_token = Column(String(500), comment='笔记小红书id')
    douyin_name = Column(String(500), comment='笔记小红书id')
    status = Column(Integer, comment='笔记小红书id')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')

class PgyUserFans(Base):
    __tablename__ = 'pgy_user_fans'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    user_id = Column(String(128), comment='博主名称')
    fans = Column(String(32), comment='博主user_id')
    platform_id = Column(Integer, comment='博主user_id')
    create_time = Column(DateTime, comment='创建时间')
    update_time = Column(DateTime, comment='更新时间')

class PgyNoteDetail(Base):
    __tablename__ = 'pgy_note_detail'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    pgy_id = Column(BigInteger, comment='博主id')
    note_id = Column(String(32), comment='笔记ID')
    note_title = Column(Text, comment='笔记标题')
    note_date = Column(String(16), comment='日期')
    note_type = Column(String(255), comment='笔记类型')
    xsec_token = Column(String(255), comment='笔记类型')
    like_num = Column(Integer, comment='点赞数')
    collect_num = Column(Integer, comment='收藏数')
    share_num = Column(Integer, comment='分享数')
    platform_id = Column(Integer, comment='博主user_id')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')
    note_message = Column(Text, comment='笔记内容')

class DouyinMcn(Base):
    __tablename__ = 'douyin_mcn'

    id = Column(Integer, primary_key=True, autoincrement=True)
    author_num = Column(Integer, comment='达人数')
    name = Column(String(255), comment='标签')
    mcn_tags = Column(String(255), comment='标签')
    avatar_uri = Column(String(500), comment='MCN头像')
    complex_score = Column(DECIMAL(10, 5), comment='综合评分')
    growth_score = Column(DECIMAL(10, 5), comment='成长评分')
    introduction = Column(Text, comment='简介')
    user_id = Column(BigInteger, default=0, comment='MCN ID')
    sum_follower = Column(BigInteger, default=0, comment='粉丝数')
    create_time = Column(DateTime, comment='创建时间')
    update_time = Column(DateTime, comment='更新时间')
    status = Column(Integer, default=0, comment='状态')

class DouyinMcnDetail(Base):
    __tablename__ = 'douyin_mcn_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    mcn_id = Column(Integer, default=0, comment='MCN ID')
    author_id = Column(String(255), comment='作者ID')
    avatar_uri = Column(String(500), comment='头像')
    nick_name = Column(String(255), comment='昵称')
    tags = Column(String(255), comment='标签')
    sum_follower = Column(BigInteger, default=0, comment='粉丝数')
    create_time = Column(DateTime, comment='创建时间')
    update_time = Column(DateTime, comment='更新时间')


class DouyinSearchList(Base):
    __tablename__ = 'douyin_search_list'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    import_status = Column(BigInteger)
    attribute_datas = Column(Text, nullable=True, comment='属性数据(JSON或文本)')
    extra_data = Column(Text, nullable=True, comment='额外数据(JSON或文本)')
    items = Column(Text, nullable=True, comment='项目数据(JSON或文本)')
    star_id = Column(String(255), comment='星标ID')
    task_infos = Column(Text, nullable=True, comment='任务信息(JSON或文本)')
    category = Column(String(100), nullable=True, comment='博主品类')
    created_at = Column(TIMESTAMP, nullable=True, comment='创建时间')
    updated_at = Column(TIMESTAMP, nullable=True, comment='更新时间')
    status = Column(Integer, nullable=True, default=0, comment='状态：0-未处理，1-处理成功，2-无创作能力')

class Creator(Base):
    __tablename__ = 'fp_creator'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    platform_user_id = Column(String(34), comment='平台user_id(如小红书id)')
    platform_account_id = Column(String(30), unique=True, comment='平台账号id(如小红书id)')
    mcn_name = Column(String(32), comment='所属机构')
    creator_nickname = Column(String(32), comment='昵称')
    creator_avatar = Column(String(128), comment='头像')
    creator_gender = Column(String(32), comment='性别')
    creator_location = Column(String(32), comment='地理位置')
    fans_count = Column(Integer, default=0, comment='粉丝数')
    like_collect_count = Column(Integer, default=0, comment='赞藏数量')
    picture_price = Column(DECIMAL(10, 2), default=0.00, comment='图文合作价格')
    video_price = Column(DECIMAL(10, 2), default=0.00, comment='视频合作价格')
    account_level = Column(Integer, comment='账号等级')
    content_field = Column(String(255), comment='领域标签')
    status = Column(SmallInteger, default=1, comment='状态')
    delete_time = Column(Integer, default=0, comment='删除时间')
    create_time = Column(Integer, comment='创建时间')
    create_user = Column(Integer, comment='创建人')
    update_time = Column(Integer, comment='更新时间')
    update_user = Column(Integer, comment='更新人')
    picture_show_state = Column(SmallInteger, default=1, nullable=False, comment='picture暂停接单')
    video_show_state = Column(SmallInteger, default=1, nullable=False, comment='video暂停接单')
    employee_id = Column(Integer, comment='员工ID')
    creator_id = Column(Integer, comment='创作者ID')
    emoloyee_name = Column(String(255), comment='员工姓名')
    dept_name = Column(String(255), comment='部门名称')
    sign_start_time = Column(DateTime, comment='签约开始时间')
    sign_end_time = Column(DateTime, comment='签约结束时间')

class CreatorNoteDetail(Base):
    __tablename__ = 'fp_creator_note_detail'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键ID')
    creator_id = Column(BigInteger, nullable=True, comment='博主id', index=True)
    platform_user_id = Column(String(32), nullable=True, comment='平台user_id')
    note_id = Column(String(32), nullable=True, comment='笔记ID')
    note_title = Column(String(128), nullable=True, comment='标题')
    brand_name = Column(String(32), nullable=True, comment='品牌名称')
    note_date = Column(String(16), nullable=True, comment='日期')
    img_url = Column(String(128), nullable=True, comment='封面图片')
    is_advertise = Column(Boolean, nullable=True, comment='是否广告')
    is_video = Column(Boolean, nullable=True, comment='是否视频')
    like_num = Column(Integer, nullable=True, comment='点赞数')
    collect_num = Column(Integer, nullable=True, comment='收藏数')
    read_num = Column(Integer, nullable=True, comment='阅读数')
    create_time = Column(Integer, nullable=True, comment='创建时间')
    update_time = Column(Integer, nullable=True, comment='更新时间')

class DouyinKol(Base):
    __tablename__ = 'douyin_kol'

    id = Column(Integer, primary_key=True, autoincrement=True)
    douyin_id = Column(String(255), comment='抖音id')
    douyin_url = Column(String(500), comment='抖音链接')
    douyin_fans = Column(String(255), comment='抖音粉丝数')
    douyin_nickname = Column(String(255), comment='抖音名')
    douyin_sec_uid = Column(String(255), comment='抖音uid')
    create_time = Column(Integer, default=lambda: int(datetime.now().timestamp()), comment='创建时间（时间戳）')
    update_time = Column(Integer, default=lambda: int(datetime.now().timestamp()), onupdate=lambda: int(datetime.now().timestamp()), comment='更新时间（时间戳）')

class DouyinNote(Base):
    __tablename__ = 'douyin_note'

    id = Column(Integer, primary_key=True, autoincrement=True)
    douyin_kol_id = Column(Integer, comment='关联抖音KOL表id')
    note_id = Column(String(255), comment='作品ID')
    note_link = Column(Text, comment='作品链接')
    note_title = Column(Text, comment='作品标题')
    note_like = Column(Integer, comment='点赞数')
    note_collect = Column(Integer, comment='收藏数')
    note_comment = Column(Integer, comment='评论数')
    note_share = Column(Integer, comment='分享数')
    note_publish_time = Column(Integer, comment='发布时间（时间戳）')
    note_tags = Column(String(255), comment='标签')
    note_img = Column(String(255), comment='封面图片')
    create_time = Column(Integer, default=lambda: int(datetime.now().timestamp()), comment='创建时间（时间戳）')
    update_time = Column(Integer, default=lambda: int(datetime.now().timestamp()), onupdate=lambda: int(datetime.now().timestamp()), comment='更新时间（时间戳）')

class QianguaTag(Base):
    __tablename__ = 'qiangua_tag'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, comment='标签ID')
    tag_name = Column(String(255), comment='标签名称')
    create_time = Column(Integer, default=lambda: int(datetime.now().timestamp()), comment='创建时间（时间戳）')
    update_time = Column(Integer, default=lambda: int(datetime.now().timestamp()),
                         onupdate=lambda: int(datetime.now().timestamp()), comment='更新时间（时间戳）')

class QianguaZifu(Base):
    __tablename__ = 'qiangua_zifu'

    id = Column(Integer, primary_key=True, autoincrement=True)
    BrandCount = Column(String(255), comment='参与投放的品牌')
    Amount = Column(String(255), comment='预估合作费用')
    AmountChange = Column(String(255), comment='费用增长')
    BrandCountChange = Column(String(255), comment='品牌数增长')
    catorgr = Column(String(255), comment='分类')
    month = Column(String(255), comment='月份')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')

class BloggerInfo(Base):
    __tablename__ = "blogger_info"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    pgy_link = Column(String(255))
    identity_tags = Column(String(255))
    nickname = Column(String(100))
    homepage_link = Column(String(255))
    daily_read_median = Column(Integer)
    status = Column(Integer)
    daily_engagement_median = Column(Integer)
    gender = Column(String(20))
    location = Column(String(100))
    picture_price = Column(Integer)
    video_price = Column(Integer)
    trade_type = Column(String(100))
    red_id = Column(String(100), index=True)
    fans_count = Column(Integer)
    tags = Column(String(500))
    tag = Column(String(255))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP,  server_default=text("CURRENT_TIMESTAMP"), server_onupdate=text("CURRENT_TIMESTAMP"))



