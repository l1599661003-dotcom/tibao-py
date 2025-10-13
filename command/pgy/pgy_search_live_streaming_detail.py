import requests
import time
import urllib3
from service.feishu_service import update_record, read_table_content

def handle():
    aaa = read_table_content('O5uNbHYLdaWQV1sPn8FcdsbSnmb', 'tblI3822c2ttZfZM', 'vew3GrjYjE')
    for aa in aaa:
        record_id = aa['record_id']
        user_id = aa['fields']['博主id'][0]['text']

        time.sleep(6)
        response = requests.get(
            f"https://pgy.xiaohongshu.com/api/draco/distribution/kol_detail/overview?buyer_id={user_id}&date_type=1",
            headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Cookie': 'acw_tc=0a42520717470475214992049e0e7c38269aa1eb45a76cbda7203171650a80; xsecappid=ratlin; a1=196c425a012r8h8f5xo88qnk32ldi9ckunrpfzgc650000462469; webId=6b316248f406bb8b2c32fa8d383fb5a8; websectiga=16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134; sec_poison_id=e872b020-3b49-4695-844a-6996f439a337; gid=yjKS4J20S07dyjKS4J208IukyJFYxYi2ClYYFSKAITqJ1U28f3jSTi8884KJ4Kj84Wiff840; customer-sso-sid=68c517503512186623135155oturupush3gbqrnl; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; customerClientId=660072243234351; solar.beaker.session.id=AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn'
            },
            verify=False
        )
        res = response.json()['data']['data']

        time.sleep(6)

        response1 = requests.get(
            f"https://pgy.xiaohongshu.com/api/draco/distribution/kol_detail/commerce_transformation?buyer_id={user_id}&date_type=1",
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
                'Cookie': 'acw_tc=0a42520717470475214992049e0e7c38269aa1eb45a76cbda7203171650a80; xsecappid=ratlin; a1=196c425a012r8h8f5xo88qnk32ldi9ckunrpfzgc650000462469; webId=6b316248f406bb8b2c32fa8d383fb5a8; websectiga=16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134; sec_poison_id=e872b020-3b49-4695-844a-6996f439a337; gid=yjKS4J20S07dyjKS4J208IukyJFYxYi2ClYYFSKAITqJ1U28f3jSTi8884KJ4Kj84Wiff840; customer-sso-sid=68c517503512186623135155oturupush3gbqrnl; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; customerClientId=660072243234351; solar.beaker.session.id=AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn'
            },
            verify=False
        )
        res1 = response1.json()['data']['data']

        result = {
            '客单价': str(res['customer_price']),
            '近期直播天数': str(res['recent_live_days']),
            '单场关播人数': str(res['single_live_viewer_number']),
            '粉丝画像': f"{res['max_ratio_fans_profile']}|{res['max_ratio_fans_profile_gender']}",
            '总销售额': str(res1['total_sale_amount']),
            '单场最高销售额': str(res1['single_max_sale_amount']),
            '场均销售额': str(res['average_sale_amount']),
            '场均客单价': str(res1['single_average_customer_price']),
        }
        update_record('O5uNbHYLdaWQV1sPn8FcdsbSnmb', 'tblI3822c2ttZfZM', record_id, result)

        time.sleep(6)  # 添加延迟避免请求过快

if __name__ == "__main__":
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Run the script
    handle()
