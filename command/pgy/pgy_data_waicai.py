import time
from datetime import datetime, timedelta

from core.database_text_tibao_2 import session
from models.models_tibao import ChinaProvince, ChinaCity
from service.feishu_service import read_table_content, update_record
from service.pgy_service import get_blogger_info, get_notes_rate, get_fans_profile, get_data_summary, get_core_data, \
    get_fans_summary, get_notes_detail

"""
从飞书获取博主ID，然后请求小红书接口获取报价和指标数据，并更新回飞书
"""

# 飞书表格信息
APP_TOKEN = "O5uNbHYLdaWQV1sPn8FcdsbSnmb"
TABLE_ID = "tbl2iQIKChSTKzu7"
VIEW_ID = "vew5gkxLlp"

now = datetime.now()
one_months_ago = now - timedelta(days=30)
two_months_ago = now - timedelta(days=60)
three_months_ago = now - timedelta(days=90)
six_months_ago = now - timedelta(days=180)
# eight_months_ago = now - timedelta(days=240)

def main():
    try:
        # 从飞书获取数据
        records = read_table_content(APP_TOKEN, TABLE_ID, VIEW_ID)

        if not records:
            print("未从飞书获取到记录")
            return

        print(f"从飞书获取到{len(records)}条记录")

        # 处理每条记录
        for record in records:
            record_id = record.get("record_id")
            fields = record.get("fields", {})
            # 获取博主ID
            # user_id = fields.get("博主id")[0]['text']
            user_id = fields.get("博主id")['value'][0]['text']
            if not user_id:
                print(f"记录{record_id}无博主ID，跳过")
                continue
            print(f"处理博主ID: {user_id}")
            # 获取博主信息
            blogger_info = get_blogger_info(user_id)
            nationality = '国外'
            city = '非一线'
            shipping_address = blogger_info.get("location").strip() if blogger_info.get("location") else ''
            tags = set()
            for tag in (blogger_info.get("contentTags") or []):
                if tag.get("taxonomy1Tag"):
                    tags.add(tag["taxonomy1Tag"])
                if tag.get("taxonomy2Tag"):
                    tags.add(tag["taxonomy2Tag"])

            if shipping_address:
                province_prefix = shipping_address[:2]
                # 模糊匹配省份
                province_exists = session.query(ChinaProvince).filter(
                    ChinaProvince.province_name.like(f"{province_prefix}%")
                ).first()
                if province_exists:
                    nationality = '国内'

                province_suffix = shipping_address[-2:]
                # 模糊匹配城市
                city_exists = session.query(ChinaCity).filter(
                    ChinaCity.city_name.like(f"{province_suffix}%")
                ).first()
                if city_exists:
                    city = '一线'
            time.sleep(6)

            brands_1_months = set()
            brands_2_months = set()
            brands_3_months = set()
            brands_6_months = set()
            # brands_8_months = set()
            bloggers = get_notes_detail(user_id)

            last_publish_date = None
            notes_total = len(bloggers)
            video_total = 0
            picture_total = 0
            for blogger in bloggers:
                is_video = blogger.get('isVideo')
                if is_video == 1:
                    video_total += 1
                else:
                    picture_total += 1
                date_str = blogger.get('date')
                brand = blogger.get('brandName')
                if not (date_str and brand):
                    continue
                try:
                    note_date = datetime.strptime(date_str, "%Y-%m-%d")
                except:
                    continue

                if not last_publish_date or note_date > last_publish_date:
                    last_publish_date = note_date

                if note_date >= one_months_ago:
                    brands_1_months.add(brand)
                if note_date >= two_months_ago:
                    brands_2_months.add(brand)
                if note_date >= three_months_ago:
                    brands_3_months.add(brand)
                if note_date >= six_months_ago:
                    brands_6_months.add(brand)
                # if note_date >= eight_months_ago:
                #     brands_8_months.add(brand)

            # 生成逗号拼接字段
            brand_str_1m = "，".join(sorted(brands_1_months))
            brand_str_2m = "，".join(sorted(brands_2_months))
            brand_str_3m = "，".join(sorted(brands_3_months))
            brand_str_6m = "，".join(sorted(brands_6_months))
            # brand_str_8m = "，".join(sorted(brands_8_months))
            time.sleep(6)

            # 粉丝分析数据
            fans_summary = get_fans_summary(user_id)
            time.sleep(6)

            # 日常30天 图文+视频cpe，cpm，cpc，阅读，曝光，合作信息
            rc_core_data = get_core_data(user_id, business=0, noteType=3, advertiseSwitch=1)
            rc_metrics = extract_sum_data(rc_core_data)
            time.sleep(6)

            # 日常30天 图文cpe，cpm，cpc，阅读，曝光，合作信息
            tu_core_data = get_core_data(user_id, business=0, noteType=1, advertiseSwitch=1)
            tu_metrics = extract_sum_data(tu_core_data)
            time.sleep(6)
            #
            # 合作30天 图文cpe，cpm，cpc，阅读，曝光，合作信息
            hz_tu_core_data = get_core_data(user_id, business=1, noteType=1, advertiseSwitch=1)
            hz_tu_metrics = extract_sum_data(hz_tu_core_data)
            time.sleep(6)
            #
            # 日常30天 视频cpe，cpm，cpc，阅读，曝光，合作信息
            shi_core_data = get_core_data(user_id, business=0, noteType=2, advertiseSwitch=1)
            shi_metrics = extract_sum_data(shi_core_data)
            time.sleep(6)
            #
            # 合作30天 视频cpe，cpm，cpc，阅读，曝光，合作信息
            hz_shi_core_data = get_core_data(user_id, business=0, noteType=2, advertiseSwitch=1)
            hz_shi_metrics = extract_sum_data(hz_shi_core_data)
            time.sleep(6)
            #
            # 合作30天 图文+视频cpe，cpm，cpc，阅读，曝光，合作信息
            hz_core_data = get_core_data(user_id, business=0, noteType=3, advertiseSwitch=1)
            hz_metrics = extract_sum_data(hz_core_data)
            time.sleep(6)

            # 48小时邀约回复率
            data_summary = get_data_summary(user_id)
            time.sleep(6)
            hz_data_summary = get_data_summary(user_id, business=1)
            time.sleep(6)

            # 粉丝数据
            fans_profile = get_fans_profile(user_id)
            # 年龄占比
            ages = fans_profile.get('ages') or []
            # 地域占比
            provinces = fans_profile.get('provinces') or []
            # 性别占比
            gender_info = fans_profile.get('gender', {})
            # 用户兴趣
            interests = fans_profile.get('interests', {})
            # 省份
            provinces = fans_profile.get('provinces', {})
            # 城市
            cities = fans_profile.get('cities', {})
            # 设备
            devices = fans_profile.get('devices', {})
            time.sleep(6)

            # 日常-视频-图文
            notes_rate = get_notes_rate(user_id, 0, 3, 1, 1)
            time.sleep(6)
            
            # 合作-视频-图文
            # hz_notes_rate = get_notes_rate(user_id, 1, 3, 1, 0)
            # time.sleep(5)
            rt3_notes_rate = get_notes_rate(user_id, 0, 1, 1, 1)
            time.sleep(6)
            rs3_notes_rate = get_notes_rate(user_id, 0, 2, 1, 1)
            time.sleep(6)
            ht3_notes_rate = get_notes_rate(user_id, 1, 1, 1, 1)
            time.sleep(6)
            hs3_notes_rate = get_notes_rate(user_id, 1, 2, 1, 1)
            time.sleep(6)
            rt9_notes_rate = get_notes_rate(user_id, 0, 1, 2, 1)
            time.sleep(6)
            rs9_notes_rate = get_notes_rate(user_id, 0, 1, 2, 1)
            time.sleep(6)
            ht9_notes_rate = get_notes_rate(user_id, 1, 1, 2, 1)
            time.sleep(6)
            hs9_notes_rate = get_notes_rate(user_id, 1, 2, 2, 1)
            time.sleep(6)

            # 安全获取 pagePercentVo
            page_percent_vo = notes_rate.get("pagePercentVo", {}) if notes_rate else {};
            # 更新飞书记录
            update_fields = {

                # blogger_info接口
                "达人昵称": str(blogger_info.get("name") or ""),
                # '小红书主页链接': f"https://www.xiaohongshu.com/user/profile/{user_id}",
                "小红书id": str(blogger_info.get("redId") or ""),
                "粉丝数": f"{(blogger_info.get('fansCount') or 0) / 10000:.1f}",
                # "达人量级": ("尾部" if blogger_info.get("fansCount") or 0 <= 50000 else "腰部" if blogger_info.get("fansCount") or 0 <= 500000 else "头部"),
                "赞藏": f"{(blogger_info.get('likeCollectCountInfo') or 0) / 10000:.1f}",
                # "赞粉比": str(blogger_info.get('likeCollectCountInfo') or 0 / blogger_info.get("fansCount") or 0),
                "所在机构": str((blogger_info.get("noteSign") or {}).get("name") or ""),
                "所在地区": str(blogger_info.get("location") or ""),
                "系统抓取标签": list(tags),
                "博主身份": blogger_info.get("personalTags") or [],
                "蒲公英健康状态": {0: "异常", 1: "普通", 2: "优秀"}.get(blogger_info.get("currentLevel"), ""),
                '图文报备价格': str(blogger_info.get("picturePrice") or 0),
                '视频报备价格': str(blogger_info.get("videoPrice") or 0),
                '国籍': nationality,
                '一线城市占比': city,
                # '图文报价含服务费': str(blogger_info.get("picturePrice") or 0),
                # '视频报价含服务费': str(blogger_info.get("videoPrice") or 0),

                # fans_summary接口
                # '粉丝增量': str(fans_summary.get("fansGrowthRate", 0)),
                '活跃粉丝占比': str(fans_summary.get("activeFansRate", 0) or "0"),
                # '粉丝量变化维度': str(fans_summary.get("fansGrowthRate", 0)),

                # data_summary接口
                "邀约48小时回复率": str(data_summary.get("responseRate") or "0"),
                "近七天活跃天数": str(data_summary.get("activeDayInLast7")),
                "预估阅读单价图文": str(data_summary.get("picReadCost") or "0"),
                "预估阅读单价视频": str(data_summary.get("videoReadCostV2") or "0"),
                "预估cpm图文": str(data_summary.get("estimatePictureCpm") or "0"),
                "预估cpm视频": str(data_summary.get("estimateVideoCpm") or "0"),
                "30天曝光中位数": str(data_summary.get("mAccumImpNum") or "0"),
                "30天互动中位数": str(data_summary.get("interactionMedian") or "0"),
                "30天阅读中位数": str(data_summary.get("mValidRawReadFeedNum") or "0"),
                # "曝光优于": str(data_summary.get("mAccumImpCompare")),
                "日常互动中位数优于同类博主": str(data_summary.get("mEngagementNumCompare") or "0"),
                "日常阅读中位数优于同类博主": str(data_summary.get("mValidRawReadFeedCompare") or "0"),
                "合作互动中位数优于同类博主": str(hz_data_summary.get("mEngagementNumCompare") or "0"),
                "合作阅读中位数优于同类博主": str(hz_data_summary.get("mValidRawReadFeedCompare") or "0"),
                "内容类目1": str(data_summary.get("noteType", [])[0].get('contentTag', '')) if data_summary.get("noteType") and len(data_summary.get("noteType")) > 0 else "",
                "内容类目2": str(data_summary.get("noteType", [])[1].get('contentTag', '')) if data_summary.get("noteType") and len(data_summary.get("noteType")) > 1 else "",

                # 预估数据
                "预估曝光": str((rc_metrics.get("imp", 0) + hz_metrics.get("imp", 0))/2),
                "预估阅读": str((rc_metrics.get("read", 0) + hz_metrics.get("read", 0))/2),
                "预估互动": str((rc_metrics.get("engage", 0) + hz_metrics.get("engage", 0))/2),
                # "平均互动率": str((float(notes_rate.get('interactionRate')) + float(hz_notes_rate.get('interactionRate')))/2),
                # "预估CPM": str(safe_min(rc_metrics["cpm"], hz_metrics["cpm"])),
                # "预估CPE": str(safe_min(rc_metrics["cpe"], hz_metrics["cpe"])),
                # "预估cpc": float(safe_min(rc_metrics["cpc"], hz_metrics["cpc"])),


                # fans_profile接口
                "女性粉丝占比": str(round(float(gender_info.get('female', 0.0)), 2)),
                # "男粉占比": str(round(float(gender_info.get('male', 0.0)), 2)),
                # "年龄分布": "；".join([
                #     f"年龄<18：{round(float(ages[0].get('percent', 0.0)), 2)}%" if len(ages) > 0 else "年龄<18：0.0%",
                #     f"18-24岁：{round(float(ages[1].get('percent', 0.0)), 2)}%" if len(ages) > 1 else "18-24岁：0.0%",
                #     f"25-34岁：{round(float(ages[2].get('percent', 0.0)), 2)}%" if len(ages) > 2 else "25-34岁：0.0%",
                #     f"35-44岁：{round(float(ages[3].get('percent', 0.0)), 2)}%" if len(ages) > 3 else "35-44岁：0.0%",
                #     f"年龄>44：{round(float(ages[4].get('percent', 0.0)), 2)}%" if len(ages) > 4 else "年龄>44：0.0%",
                # ]),
                # "地域分布": "；".join([
                #     f"{p['name']}：{round(p['percent'] * 100, 2)}%"
                #     for p in provinces[:7]
                # ]),
                # "性别分布": "；".join([
                #     f"女粉：{round(float(gender_info.get('female', 0.0)), 2)}%",
                #     f"男粉：{round(float(gender_info.get('male', 0.0)), 2)}%"
                # ])
                "年龄<18": str(round(float(ages[0].get('percent', 0.0)), 2)) if len(ages) > 0 else "0.0",
                "年龄18_24": str(round(float(ages[1].get('percent', 0.0)), 2)) if len(ages) > 1 else "0.0",
                "年龄25_34": str(round(float(ages[2].get('percent', 0.0)), 2)) if len(ages) > 2 else "0.0",
                "年龄35_44": str(round(float(ages[3].get('percent', 0.0)), 2)) if len(ages) > 3 else "0.0",
                "年龄>44": str(round(float(ages[4].get('percent', 0.0)), 2)) if len(ages) > 4 else "0.0",
                "用户兴趣top1": f"{interests[0]['name']}({round(float(interests[0]['percent']) * 100, 2)}%)" if len(interests) > 0 else "",
                "用户兴趣top2": f"{interests[1]['name']}({round(float(interests[1]['percent']) * 100, 2)}%)" if len(interests) > 1 else "",
                "用户兴趣top3": f"{interests[2]['name']}({round(float(interests[2]['percent']) * 100, 2)}%)" if len(interests) > 2 else "",
                "用户兴趣top4": f"{interests[3]['name']}({round(float(interests[3]['percent']) * 100, 2)}%)" if len(interests) > 3 else "",
                "用户兴趣top5": f"{interests[4]['name']}({round(float(interests[4]['percent']) * 100, 2)}%)" if len(interests) > 4 else "",
                '省份top1': f"{provinces[0]['name']}({round(float(provinces[0]['percent']) * 100, 2)}%)" if len( provinces) > 0 else "",
                '省份top2': f"{provinces[1]['name']}({round(float(provinces[1]['percent']) * 100, 2)}%)" if len( provinces) > 1 else "",
                '省份top3': f"{provinces[2]['name']}({round(float(provinces[2]['percent']) * 100, 2)}%)" if len( provinces) > 2 else "",
                '城市top1': f"{cities[0]['name']}({round(float(cities[0]['percent']) * 100, 2)}%)" if len(cities) > 0 else "",
                '城市top2': f"{cities[1]['name']}({round(float(cities[1]['percent']) * 100, 2)}%)" if len(cities) > 1 else "",
                '城市top3': f"{cities[2]['name']}({round(float(cities[2]['percent']) * 100, 2)}%)" if len(cities) > 2 else "",

                # notes_rate接口
                # "近30天点赞中位数": str(notes_rate.get("likeMedian")),
                # "日常图文+视频曝光中位数3s": str(notes_rate.get("picture3sViewRate")),
                "日常图文+视频曝光中位数": str(rc_metrics["imp"] or "0"),
                "日常图文+视频阅读中位数": str(rc_metrics["read"] or "0"),
                "日常图文+视频互动中位数": str(rc_metrics["engage"] or "0"),
                "日常图文+视频互动率": str(notes_rate.get("interactionRate") if notes_rate and notes_rate.get("interactionRate") is not None else "0"),
                "阅读量来源的【发现页】占比": str(page_percent_vo.get("readHomefeedPercent") if page_percent_vo and page_percent_vo.get("readHomefeedPercent") is not None else "0"),
                "曝光量来源的【发现页】占比": str(page_percent_vo.get("impHomefeedPercent") if page_percent_vo and page_percent_vo.get("impHomefeedPercent") is not None else "0"),
                "日常图文+视频千赞笔记比例": str(notes_rate.get("thousandLikePercent") if notes_rate and notes_rate.get("thousandLikePercent") is not None else "0"),
                "日常图文+视频完播率": str(notes_rate.get("videoFullViewRate") or "0"),
                "30天内日常图文点赞中位数": str(rt3_notes_rate.get("likeMedian") or "0"),
                "30天内日常视频点赞中位数": str(rs3_notes_rate.get("likeMedian") or "0"),
                "30天内合作图文点赞中位数": str(ht3_notes_rate.get("likeMedian") or "0"),
                "30天内合作视频点赞中位数": str(hs3_notes_rate.get("likeMedian") or "0"),
                "90天内日常图文点赞中位数": str(rt9_notes_rate.get("likeMedian") or "0"),
                "90天内日常视频点赞中位数": str(rs9_notes_rate.get("likeMedian") or "0"),
                "90天内合作图文点赞中位数": str(ht9_notes_rate.get("likeMedian") or "0"),
                "90天内合作视频点赞中位数": str(hs9_notes_rate.get("likeMedian") or "0"),

                # "90天图文视频点赞中位数": str(hz_like),
                # "90天日常点赞": str(rc_like_90),
                # "90天合作点赞": str(hz_like_90),

                # "曝光中位数自然流量合作30天": str(hz_metrics["imp"]),
                # "阅读中位数自然流量合作30天": str(hz_metrics["read"]),
                # "互动中位数自然流量合作30天": str(hz_metrics["engage"]),

                # "视频平均点赞": str(likeMedian),
                "日常图文cpe": str(tu_metrics["cpe"] or "0"),
                "日常视频cpe": str(shi_metrics["cpe"] or "0"),
                "日常图文cpm": str(tu_metrics["cpm"] or "0"),
                "日常视频cpm": str(shi_metrics["cpm"] or "0"),
                "日常图文cpc": str(tu_metrics["cpc"] or "0"),
                "日常视频cpc": str(shi_metrics["cpc"] or "0"),
                "合作图文cpe": str(hz_tu_metrics["cpe"] or "0"),
                "合作视频cpe": str(hz_shi_metrics["cpe"] or "0"),
                "合作图文cpm": str(hz_tu_metrics["cpm"] or "0"),
                "合作视频cpm": str(hz_shi_metrics["cpm"] or "0"),
                "合作图文cpc": str(hz_tu_metrics["cpc"] or "0"),
                "合作视频cpc": str(hz_shi_metrics["cpc"] or "0"),
                # "cpe": str(safe_min(rc_metrics["cpe"], hz_metrics["cpe"])),
                # "cpm": str(safe_min(rc_metrics["cpm"], hz_metrics["cpm"])),
                # "cpc": str(safe_min(rc_metrics["cpc"], hz_metrics["cpc"])),

                # notes_detail接口
                "30天内合作品牌名": brand_str_1m,
                "60天内合作品牌名": brand_str_2m,
                "90天内合作品牌名": brand_str_3m,
                "6个月内合作品牌名": brand_str_6m,
                # "8个月内合作品牌": brand_str_8m,
                "笔记最后发布时间": last_publish_date.strftime('%Y-%m-%d') if last_publish_date else '',
                '图文笔记占比': str(safe_divide(picture_total, notes_total)),
                '视频笔记占比': str(safe_divide(video_total, notes_total)),
            }

            update_record(APP_TOKEN, TABLE_ID, record_id, update_fields)

        print("数据更新完成")

    except Exception as e:
        print(f"程序执行出错: {str(e)}")

# 计算 min 值（非0）
def safe_min(*args):
    return min([x for x in args if x > 0], default=0)

def safe_divide(numerator, denominator):
    return float(numerator) / denominator if denominator != 0 else 0.0

def extract_sum_data(core_data):
    """提取 core_data 中的关键字段，默认值为 0"""
    sum_data = core_data.get("sumData", {}) if core_data else {}
    return {
        "cpe": sum_data.get("cpe", 0),
        "cpm": sum_data.get("cpm", 0),
        "cpc": sum_data.get("cpv", 0),
        "read": sum_data.get("read", 0),
        "imp": sum_data.get("imp", 0),
        "engage": sum_data.get("engage", 0),
    }

if __name__ == '__main__':
    main()
    # schedule.every().day.at('08:00').do(main)
    # # 如果你想在程序运行时一直保持调度
    # print("自动化发送邀约已启动，每天指定时间运行，按 Ctrl+C 退出...")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)