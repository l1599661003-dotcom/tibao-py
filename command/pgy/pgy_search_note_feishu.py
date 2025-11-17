import requests
import json
import time
import urllib3
import pandas as pd
from datetime import datetime
import os


class GetPGYMediaAll:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            "cookie": 'a1=19a5eb1a425pmd1q4v02v1zlqfls7z25o8f34547h50000131391; webId=58e94014b58a170000190f4b2283aaaf; customer-sso-sid=68c517569988680951332869hzm0ncku1hk7oudo; x-user-id-pgy.xiaohongshu.com=68fb0f1c1558000000000001; customerClientId=064982114373578; xsecappid=ratlin; solar.beaker.session.id=AT-68c5175725519646781931555rzlmzneowaiiowl; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c5175725519646781931555rzlmzneowaiiowl; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c5175725519646781931555rzlmzneowaiiowl; acw_tc=0a42252317631247772785371e8c7a86e54ceaa6cdc9f66bf51b34b4e33f62; loadts=1763125630940',
        }

    def _extract_tags(self, blogger):
        """提取并拼接标签"""
        tags_list = []
        
        # 处理 contentTags
        content_tags = blogger.get('contentTags', [])
        if content_tags and isinstance(content_tags, list):
            for content_tag in content_tags:
                if not isinstance(content_tag, dict):
                    continue
                # 添加 taxonomy1Tag
                taxonomy1_tag = content_tag.get('taxonomy1Tag', '')
                if taxonomy1_tag:
                    tags_list.append(taxonomy1_tag)
                # 循环添加 taxonomy2Tags
                taxonomy2_tags = content_tag.get('taxonomy2Tags', [])
                if taxonomy2_tags and isinstance(taxonomy2_tags, list):
                    tags_list.extend([tag for tag in taxonomy2_tags if tag])
        
        # 处理 featureTags
        feature_tags = blogger.get('featureTags', [])
        if feature_tags and isinstance(feature_tags, list):
            tags_list.extend([tag for tag in feature_tags if tag])
        
        # 拼接所有标签，用中文逗号分隔
        return '、'.join(tags_list) if tags_list else ''

    def handle(self):
        base_data = {"searchType":0,"column":"comprehensiverank","sort":"desc","pageSize":20,"brandUserId":"62b58a79000000001b024664","marketTarget":None,"audienceGroup":[],"contentTag":["家居装饰","花艺园艺","家居用品","装修","家具","室内设计","家居家装其他","居家经验"],"personalTags":[],"gender":None,"location":None,"signed":-1,"featureTags":[],"fansNumberLower":50000,"fansNumberUpper":None,"fansAge":0,"fansGender":0,"accumCommonImpMedinNum30d":[],"readMidNor30":[],"interMidNor30":[],"thousandLikePercent30":[],"noteType":0,"videoPriceLower":3000,"videoPriceUpper":8500,"progressOrderCnt":[],"tradeType":"不限","tradeReportBrandIdSet":[],"excludedTradeReportBrandId":False,"estimateCpuv30d":[],"inStar":0,"firstIndustry":"","secondIndustry":"","newHighQuality":0,"filterIntention":False,"flagList":[{"flagType":"HAS_BRAND_COOP_BUYER_AUTH","flagValue":"0"},{"flagType":"IS_HIGH_QUALITY","flagValue":"0"}],"activityCodes":[],"excludeLowActive":True,"fansNumUp":0,"excludedTradeReportBrand":False,"excludedTradeInviteReportBrand":False,"filterList":[]}

        all_data = []  # 存储所有博主数据

        for i in range(1, 200):
            data = base_data.copy()
            data['pageNum'] = i

            try:
                # First request to get trackId
                response = requests.post(
                    "https://pgy.xiaohongshu.com/api/solar/cooperator/blogger/track",
                    headers=self.headers,
                    json=data,
                    verify=False
                )
                response_data = response.json()

                data['trackId'] = response_data['data']['trackId']

                # Sleep to avoid rate limiting
                time.sleep(6)

                # Second request to get blogger data
                response1 = requests.post(
                    "https://pgy.xiaohongshu.com/api/solar/cooperator/blogger/v2",
                    headers=self.headers,
                    json=data,
                    verify=False
                )

                bloggers = response1.json()['data']['kols']

                if len(bloggers) == 0:
                    print(f"第 {i} 页没有数据，已到达最后一页")
                    break

                # 收集这一页的所有博主数据
                for blogger in bloggers:
                    # 提取标签
                    tags = self._extract_tags(blogger)
                    
                    # 构建数据行
                    row_data = {
                        '蒲公英链接': f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{blogger.get('userId', '')}",
                        '博主身份标签': '、'.join(blogger.get('personalTags', [])) if isinstance(blogger.get('personalTags', []), list) else blogger.get('personalTags', ''),
                        '昵称': blogger.get('name', ''),
                        '主页链接': f"https://www.xiaohongshu.com/user/profile/{blogger.get('userId', '')}",
                        '阅读中位数（日常）': blogger.get('clickMidNum', ''),
                        '互动中位数（日常）': blogger.get('mengagementNum', ''),
                        '性别': blogger.get('gender', ''),
                        '地区': blogger.get('location', ''),
                        '图文报价': blogger.get('picturePrice', ''),
                        '视频报价': blogger.get('videoPrice', ''),
                        '合作行业': blogger.get('tradeType', ''),
                        '小红书ID': blogger.get('redId', ''),
                        '粉丝数': blogger.get('fansNum', ''),
                        '标签': tags,
                    }
                    all_data.append(row_data)

                print(f"完成第 {i} 页，共收集 {len(all_data)} 条数据")
                time.sleep(2)  # 添加延迟避免请求过快

            except requests.RequestException as e:
                print(f"第 {i} 页请求错误: {str(e)}")
                continue
            except Exception as e:
                print(f"第 {i} 页发生意外错误: {str(e)}")
                continue

        # 导出数据到Excel
        if all_data:
            self._export_to_excel(all_data)
        else:
            print("没有收集到任何数据")

    def _export_to_excel(self, data):
        """导出数据到Excel文件"""
        try:
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 生成文件名（包含时间戳）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'蒲公英博主数据_{timestamp}.xlsx'
            
            # 确保输出目录存在
            output_dir = 'output'
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            filepath = os.path.join(output_dir, filename)
            
            # 导出到Excel
            df.to_excel(filepath, index=False, engine='openpyxl')
            
            print(f"\n✅ 数据导出成功！")
            print(f"📁 文件路径: {filepath}")
            print(f"📊 共导出 {len(data)} 条数据")
            
        except Exception as e:
            print(f"❌ 导出Excel失败: {str(e)}")
            # 如果导出失败，尝试保存到当前目录
            try:
                fallback_filename = f'蒲公英博主数据_{timestamp}.xlsx'
                df.to_excel(fallback_filename, index=False, engine='openpyxl')
                print(f"✅ 已保存到当前目录: {fallback_filename}")
            except Exception as e2:
                print(f"❌ 备用导出也失败: {str(e2)}")


if __name__ == "__main__":
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Run the script
    script = GetPGYMediaAll()
    script.handle()
