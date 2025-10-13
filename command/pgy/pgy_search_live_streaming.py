import requests
import json
import time
from datetime import datetime
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from core.database_text_tibao_2 import Session


class GetPGYMediaAll:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Cookie': 'acw_tc=0a42520717470475214992049e0e7c38269aa1eb45a76cbda7203171650a80; xsecappid=ratlin; a1=196c425a012r8h8f5xo88qnk32ldi9ckunrpfzgc650000462469; webId=6b316248f406bb8b2c32fa8d383fb5a8; websectiga=16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134; sec_poison_id=e872b020-3b49-4695-844a-6996f439a337; gid=yjKS4J20S07dyjKS4J208IukyJFYxYi2ClYYFSKAITqJ1U28f3jSTi8884KJ4Kj84Wiff840; customer-sso-sid=68c517503512186623135155oturupush3gbqrnl; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; customerClientId=660072243234351; solar.beaker.session.id=AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517503512186622952237agcvm3s2uwuyr9zn'
        }

    def get_table_columns(self, session):
        # 获取表的列名
        result = session.execute(text("SHOW COLUMNS FROM fp_out_blogger_cooperator_v2"))
        return [row[0] for row in result]

    def save_blogger_data(self, blogger_data: dict):
        session = Session()
        try:
            # 获取表的列名
            columns = self.get_table_columns(session)

            # 检查是否存在相同userId的记录
            exists = session.execute(
                text("SELECT id FROM fp_out_blogger_cooperator_v2 WHERE userId = :userId"),
                {"userId": blogger_data.get('userId')}
            ).fetchone()

            if not exists:
                # 准备插入的数据
                insert_data = {}
                for key, value in blogger_data.items():
                    # 只处理存在于表中的列
                    if key in columns:
                        # 特殊处理某些字段
                        if key in ['featureTags', 'personalTags']:
                            value = ','.join(value) if value else None
                        elif isinstance(value, (list, dict)):
                            value = json.dumps(value, ensure_ascii=False)
                        elif isinstance(value, bool):
                            value = 1 if value else 0

                        insert_data[key] = value

                # 构建INSERT语句
                columns_str = ', '.join(insert_data.keys())
                values_str = ', '.join(f':{key}' for key in insert_data.keys())
                sql = f"INSERT INTO fp_out_blogger_cooperator_v2 ({columns_str}) VALUES ({values_str})"

                try:
                    session.execute(text(sql), insert_data)
                    session.commit()
                    print(f"Successfully inserted blogger: ({blogger_data.get('userId')})")
                except Exception as e:
                    print(f"SQL Error: {str(e)}")
                    print(f"SQL: {sql}")
                    print(f"Values: {insert_data}")
                    session.rollback()
                    raise
            else:
                print(f"Blogger already exists:  ({blogger_data.get('userId')})")

        except Exception as e:
            print(f"Error saving blogger data: {str(e)}")
            session.rollback()
        finally:
            session.close()

    def handle(self):
        base_data ={"live_first_category":["生活电器"],"live_second_category":["清洁电器"],"fans_count":[100000,None],"avg_agmv_90d":[None,5000,10000,10000,100000,100000,500000,500000,1000000,1000000,2000000,2000000,5000000,5000000],"query_param":{"limit":20,"sort":"atv","asc":False,"seed":0}}
        for i in range(1, 200):
            data = base_data.copy()
            data['query_param']['page'] = i

            try:
                time.sleep(6)
                # Second request to get blogger data
                response1 = requests.post(
                    "https://pgy.xiaohongshu.com/api/draco/distributor-square/live/buyers",
                    headers=self.headers,
                    json=data,  # 使用包含trackId的data，而不是base_data
                    verify=False
                )

                bloggers = response1.json()['data']['distributor_info_list']
                # 保存每个博主的数据
                for blogger in bloggers:
                    result = {
                        'userId': blogger['distributor_data_info']['distributor_id'],
                        'type': '餐饮'
                    }
                    self.save_blogger_data(result)

                print(f"Completed page {i}")
                time.sleep(6)  # 添加延迟避免请求过快

            except requests.RequestException as e:
                print(f"Error on page {i}: {str(e)}")
                continue
            except Exception as e:
                print(f"Unexpected error on page {i}: {str(e)}")
                continue


if __name__ == "__main__":
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Run the script
    script = GetPGYMediaAll()
    script.handle()
