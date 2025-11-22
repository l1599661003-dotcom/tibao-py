"""
获取公司博主报价等信息
"""

import os
import time
import json
from datetime import datetime

import requests
from loguru import logger

from service.pgy_service import get_mcn_detail

"""
    获取公司博主的信息
"""
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    "cookie": 'a1=19882f48bcap04qq1r777p9ggafrdrroz4abocygo50000339517; webId=e7111fec356dc781ca1d14d236afda9a; customerClientId=016216997022661; abRequestId=e7111fec356dc781ca1d14d236afda9a; x-user-id-creator.xiaohongshu.com=634cc30badd08a00019ee4e3; gid=yjYSKK8dd0uiyjYYJi4YD4SvS0U84AAyFWWW0klUjkk0iD28FfFFll888qqj2yW8DY2KKijK; x-user-id-ad-market.xiaohongshu.com=67bbea69000000000d009ec6; access-token-ad-market.xiaohongshu.com=customer.ad_market.AT-68c517561719769394855947z3wtukxckssgow61; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; web_session=0400698efe0fc8579efe17fe2f3b4b3c80cddc; xsecappid=ratlin; acw_tc=0a42442417637752149322403eb5e96e023a5ddb96ab1976ca79a5079d0dbe; customer-sso-sid=68c517575356887560142862seuvxywtt2viehbo; solar.beaker.session.id=AT-68c51757535689185511014655kw50rxftnhdoh7; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c51757535689185511014655kw50rxftnhdoh7; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c51757535689185511014655kw50rxftnhdoh7; loadts=1763775228746',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
}

def save_creator_data(data_to_save):
    """保存创作者数据到后端接口"""
    save_url = "https://tianji.fangpian999.com/api/admin/creator/CreatorOut/saveData"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(save_url, headers=headers, json=data_to_save, timeout=3000, verify=False)
        if response.status_code == 200:
            logger.info(f"数据保存成功: creator_mcn={data_to_save.get('creator_mcn')}, 共 {len(data_to_save.get('raw_data', []))} 条数据")
            return True
        else:
            logger.error(f"数据保存失败: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"保存数据时出错: {str(e)}")
        return False


def extract_creator_info(kol_data):
    """提取创作者关键信息"""
    return {
        "userId": kol_data.get("userId"),
        "name": kol_data.get("name"),
        "location": kol_data.get("location"),
        "fansCount": kol_data.get("fansCount"),
        "likeCollectCountInfo": kol_data.get("likeCollectCountInfo"),
        "picturePrice": kol_data.get("picturePrice"),
        "videoPrice": kol_data.get("videoPrice"),
        "contentTags": kol_data.get("contentTags", []),
        "headPhoto": kol_data.get("headPhoto"),
        "redId": kol_data.get("redId"),
        "personalTags": kol_data.get("personalTags", []),
        "businessNoteCount": kol_data.get("businessNoteCount"),
        "lowerPrice": kol_data.get("lowerPrice"),
        "gender": kol_data.get("gender"),
        "featureTags": kol_data.get("featureTags", []),
    }


def get_kols_data():
    """获取kols数据,循环处理is_fetch_creator从1到4"""
    headers = {"Content-Type": "application/json"}
    base_url = "https://tianji.fangpian999.com/api/admin/CreatorOut/getSpiderMcn"

    # 确保output目录存在
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"创建输出目录: {output_dir}")

    # 外层循环: is_fetch_creator 从 1 到 4
    for fetch_num in range(2, 4):
        logger.info(f"\n{'='*50}")
        logger.info(f"开始处理 is_fetch_creator={fetch_num}")
        logger.info(f"{'='*50}")

        api_url = f"{base_url}?is_fetch_creator={fetch_num}"

        try:
            # 获取MCN列表
            response = requests.post(api_url, headers=headers, timeout=30, verify=False)
            creator_data = response.json()['data']
            logger.info(f"获取到 {len(creator_data)} 个MCN")

            # 用于收集当前fetch_num的所有创作者数据
            all_creators = []
            total_success = 0
            total_failed = 0

            # 遍历每个MCN
            for item in creator_data:
                try:
                    mcn_id = item['mcn_user_id']
                    logger.info(f"开始获取MCN {mcn_id} 的数据...")

                    # 调用get_mcn_detail获取创作者列表
                    kols = get_mcn_detail(mcn_id, header)
                    # kols = get_mcn_detail(mcn_id)

                    if kols and len(kols) > 0:
                        # 提取并添加到all_creators数组
                        for kol in kols:
                            creator_info = extract_creator_info(kol)
                            all_creators.append(creator_info)

                        success_count = len(kols)
                        total_success += success_count
                        logger.info(f"MCN {mcn_id} 返回了 {len(kols)} 个博主数据")
                    else:
                        logger.warning(f"MCN {mcn_id} 没有返回数据")

                    time.sleep(6)  # 避免请求过快

                except Exception as e:
                    total_failed += 1
                    logger.error(f"获取MCN {mcn_id} 数据失败: {str(e)}")
                    time.sleep(3)  # 出错后稍微等待一下再继续

            # 组装数据并保存
            data_to_save = {
                "creator_mcn": str(fetch_num),
                "platform_id": 1,
                "raw_data": all_creators
            }

            logger.info(f"\nis_fetch_creator={fetch_num} 数据收集完成:")
            logger.info(f"- 成功处理 {total_success} 个博主")
            logger.info(f"- 失败 {total_failed} 个MCN")
            logger.info(f"- 总计收集 {len(all_creators)} 条创作者数据")

            # 调用保存接口
            if len(all_creators) > 0:
                logger.info(f"开始保存数据到后端...")
                save_creator_data(data_to_save)
            else:
                logger.warning(f"没有数据需要保存")

            # 保存到本地JSON文件作为备份
            try:
                json_filename = f"output/mcn_data_fetch{fetch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, ensure_ascii=False, indent=2)
                logger.info(f"数据已备份到JSON文件: {json_filename}")
            except Exception as json_error:
                logger.error(f"保存JSON文件时出错: {str(json_error)}")

            logger.info(f"is_fetch_creator={fetch_num} 处理完成!\n")
            time.sleep(3)  # 每个fetch_num处理完后休息一下

        except Exception as e:
            logger.error(f"处理 is_fetch_creator={fetch_num} 时出错: {str(e)}")
            continue

    logger.info(f"\n{'='*50}")
    logger.info("所有数据处理完成!")
    logger.info(f"{'='*50}")


def main():
    """主函数"""
    try:
        logger.info("开始获取公司博主信息...")
        start_time = datetime.now()

        get_kols_data()

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"程序执行完成,耗时: {duration}")

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")


if __name__ == "__main__":
    main()
