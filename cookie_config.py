# -*- coding: utf-8 -*-
"""
Cookie配置文件
用于管理多个Cookie的轮换使用
"""

# Cookie配置列表 - 每个cookie每天限制500次调用
COOKIE_CONFIGS = [
    # {
    #     "cookie": 'passport_csrf_token=6849f54910d4b4c2979f4d8dabb93a6b; passport_csrf_token_default=6849f54910d4b4c2979f4d8dabb93a6b; is_staff_user=false; tt_webid=7552842709368161835; s_v_web_id=verify_mfv85tdl_F8gFQvfU_KTrW_4Yrj_9hxq_JS9ee8c9uWDH; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; star_sessionid=7d7d193e69c1086cbd1bd917a1efc674; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1758533208,1758851673,1760087389; HMACCOUNT=A9193F3F989E70E1; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760087394; passport_auth_status=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; passport_auth_status_ss=7cd667876f4d437c6dd0f7f7fc06cd27%2Cf213180cef4ca69d911f1fdb51109633; sid_guard=7b47b5c1e4302bc5075e27a3aa376cb0%7C1760087460%7C5184002%7CTue%2C+09-Dec-2025+09%3A11%3A02+GMT; uid_tt=ed7cb7326eed763cc0293bffa66419a2; uid_tt_ss=ed7cb7326eed763cc0293bffa66419a2; sid_tt=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid=7b47b5c1e4302bc5075e27a3aa376cb0; sessionid_ss=7b47b5c1e4302bc5075e27a3aa376cb0; session_tlb_tag=sttt%7C10%7Ce0e1weQwK8UHXiejqjdssP________-noV-qfG8S9nB4yyNwTYJ5h77Qhsn_HqhFuQgzccYHWgQ%3D; sid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; ssid_ucp_v1=1.0.0-KDhkNzNiM2IwZWZhNTJmMjE4OWY5NWE2NzA1YzZlMjkwNjE4YmEyNGEKFwi5yND90a38AhCkm6PHBhimDDgBQOsHGgJsZiIgN2I0N2I1YzFlNDMwMmJjNTA3NWUyN2EzYWEzNzZjYjA; possess_scene_star_id=1844017439585284',
    #     "name": "Cookie-1",
    #     "daily_limit": 500,
    #     "used_count": 0,
    #     "last_reset_date": None
    # },
    # {
    #     "cookie": "Hm_lvt_5d77c979053345c4bd8db63329f818ec=1760160992; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760160992; HMACCOUNT=789AFAB59A507B7E; passport_csrf_token=facb68c48a6eb50f5b8b66b6a528083b; passport_csrf_token_default=facb68c48a6eb50f5b8b66b6a528083b; passport_auth_status=f28ba6a90d9c2aeeaa6009a805a4ab05%2C; passport_auth_status_ss=f28ba6a90d9c2aeeaa6009a805a4ab05%2C; sid_guard=1a9c6545ef7884efcd70ad20a9bc28a5%7C1760161053%7C5184002%7CWed%2C+10-Dec-2025+05%3A37%3A35+GMT; uid_tt=a40f1d758f91050a7c20b70f28155892; uid_tt_ss=a40f1d758f91050a7c20b70f28155892; sid_tt=1a9c6545ef7884efcd70ad20a9bc28a5; sessionid=1a9c6545ef7884efcd70ad20a9bc28a5; sessionid_ss=1a9c6545ef7884efcd70ad20a9bc28a5; session_tlb_tag=sttt%7C6%7CGpxlRe94hO_NcK0gqbwopf_________92N63yCf4LLVrB8wna2g_q5WsF6kl0jx5HXbJkAf3BkA%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KDZkN2UxNzM3NjY0NjI3MjYwNDdmZjE1MDVhN2QzODJiOTQwYTM3MmYKFgjZopCY0q1sEJ3ap8cGGKYMOAFA6wcaAmxmIiAxYTljNjU0NWVmNzg4NGVmY2Q3MGFkMjBhOWJjMjhhNQ; ssid_ucp_v1=1.0.0-KDZkN2UxNzM3NjY0NjI3MjYwNDdmZjE1MDVhN2QzODJiOTQwYTM3MmYKFgjZopCY0q1sEJ3ap8cGGKYMOAFA6wcaAmxmIiAxYTljNjU0NWVmNzg4NGVmY2Q3MGFkMjBhOWJjMjhhNQ; star_sessionid=1a9c6545ef7884efcd70ad20a9bc28a5; possess_scene_star_id=1844031649796107; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; tt_webid=7559834121767306806",
    #     "name": "Cookie-2",
    #     "daily_limit": 500,
    #     "used_count": 0,
    #     "last_reset_date": None
    # },
    # {
    #     "cookie": "csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; tt_webid=7559834526390765119; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1760161150; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760161150; HMACCOUNT=F36F9525894E62DB; passport_csrf_token=83cb5ca0826caf51d1ad96dd0247b473; passport_csrf_token_default=83cb5ca0826caf51d1ad96dd0247b473; passport_auth_status=8de2a75c13c120d0f4d4c74636825937%2C; passport_auth_status_ss=8de2a75c13c120d0f4d4c74636825937%2C; sid_guard=6e7a56e41a53a890b4c17622f1f66d1b%7C1760161172%7C5184001%7CWed%2C+10-Dec-2025+05%3A39%3A33+GMT; uid_tt=b275adb89f13d1da30f913ca7dfeb3aa; uid_tt_ss=b275adb89f13d1da30f913ca7dfeb3aa; sid_tt=6e7a56e41a53a890b4c17622f1f66d1b; sessionid=6e7a56e41a53a890b4c17622f1f66d1b; sessionid_ss=6e7a56e41a53a890b4c17622f1f66d1b; session_tlb_tag=sttt%7C5%7CbnpW5BpTqJC0wXYi8fZtG__________i87djNuLb-e84PzkH-Xk9wpQPnfvtgbF6xjJW9tBoJcg%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KDY4NzAwZWEzYmQzMzYxYWQzMmIxYmUxYzdhZWQ4MTcxMTc3M2QxOTAKFwjru_D60a3wBRCU26fHBhimDDgBQOsHGgJsZiIgNmU3YTU2ZTQxYTUzYTg5MGI0YzE3NjIyZjFmNjZkMWI; ssid_ucp_v1=1.0.0-KDY4NzAwZWEzYmQzMzYxYWQzMmIxYmUxYzdhZWQ4MTcxMTc3M2QxOTAKFwjru_D60a3wBRCU26fHBhimDDgBQOsHGgJsZiIgNmU3YTU2ZTQxYTUzYTg5MGI0YzE3NjIyZjFmNjZkMWI; star_sessionid=6e7a56e41a53a890b4c17622f1f66d1b; possess_scene_star_id=1844016035306568",
    #     "name": "Cookie-3",
    #     "daily_limit": 500,
    #     "used_count": 0,
    #     "last_reset_date": None
    # },
    {
        "cookie": "Hm_lvt_5d77c979053345c4bd8db63329f818ec=1760161305; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760161305; HMACCOUNT=FA433A5604D4FEDA; passport_csrf_token=98853ce14acda6d3c9fa06424e0a1ba5; passport_csrf_token_default=98853ce14acda6d3c9fa06424e0a1ba5; passport_auth_status=45b7032b09952fac9b092e76eb29ce43%2C; passport_auth_status_ss=45b7032b09952fac9b092e76eb29ce43%2C; sid_guard=eb84a48d2f860cd1e28678aa46196793%7C1760161326%7C5184002%7CWed%2C+10-Dec-2025+05%3A42%3A08+GMT; uid_tt=85b744986e5ca0fe0001f611374ececf; uid_tt_ss=85b744986e5ca0fe0001f611374ececf; sid_tt=eb84a48d2f860cd1e28678aa46196793; sessionid=eb84a48d2f860cd1e28678aa46196793; sessionid_ss=eb84a48d2f860cd1e28678aa46196793; session_tlb_tag=sttt%7C16%7C64SkjS-GDNHihniqRhlnk__________0AWpWs9efRO1O7XBWT0Z-BSpn0a5yy8F0sEBQ1u0OxuM%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KGVlODg3NTM3NzU5OWE1MzMxOGZlZjIxNGVmYmIxNmQ2NDkzY2NmMDgKFwjK1fDzuK38ARCu3KfHBhimDDgBQOsHGgJsZiIgZWI4NGE0OGQyZjg2MGNkMWUyODY3OGFhNDYxOTY3OTM; ssid_ucp_v1=1.0.0-KGVlODg3NTM3NzU5OWE1MzMxOGZlZjIxNGVmYmIxNmQ2NDkzY2NmMDgKFwjK1fDzuK38ARCu3KfHBhimDDgBQOsHGgJsZiIgZWI4NGE0OGQyZjg2MGNkMWUyODY3OGFhNDYxOTY3OTM; star_sessionid=eb84a48d2f860cd1e28678aa46196793; possess_scene_star_id=1844017701334153; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; tt_webid=7559835299065857572",
        "name": "Cookie-4", 
        "daily_limit": 500,
        "used_count": 0,
        "last_reset_date": None
    },
    {
        "cookie": "Hm_lvt_5d77c979053345c4bd8db63329f818ec=1760161374; HMACCOUNT=CA9CDA8823E5ACE6; csrf_session_id=7d35df4137d66166aa0e06f7635b90a1; tt_webid=7559835500699551271; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1760161377; passport_csrf_token=d489e4c304e92932abebca176b2a45b8; passport_csrf_token_default=d489e4c304e92932abebca176b2a45b8; passport_auth_status=d48ff10cbc129250e144022e6f93a0cd%2C; passport_auth_status_ss=d48ff10cbc129250e144022e6f93a0cd%2C; sid_guard=e69f12ab28e0110163e39d36a9ab2ff9%7C1760161397%7C5184002%7CWed%2C+10-Dec-2025+05%3A43%3A19+GMT; uid_tt=6e489fa990befcba3d73fad992835c66; uid_tt_ss=6e489fa990befcba3d73fad992835c66; sid_tt=e69f12ab28e0110163e39d36a9ab2ff9; sessionid=e69f12ab28e0110163e39d36a9ab2ff9; sessionid_ss=e69f12ab28e0110163e39d36a9ab2ff9; session_tlb_tag=sttt%7C4%7C5p8SqyjgEQFj4502qasv-f________-4kc9NvX-7BfGo58a5MhqWFrOnmdT3RYNsiw8PQagvof0%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KDRjMzQ2NTJmMzcyODJkYjIyOTA2NWYzNjJjMzQ4YzczZDVmNGZhNjAKFwjus4DHgI3zARD13KfHBhimDDgBQOsHGgJsZiIgZTY5ZjEyYWIyOGUwMTEwMTYzZTM5ZDM2YTlhYjJmZjk; ssid_ucp_v1=1.0.0-KDRjMzQ2NTJmMzcyODJkYjIyOTA2NWYzNjJjMzQ4YzczZDVmNGZhNjAKFwjus4DHgI3zARD13KfHBhimDDgBQOsHGgJsZiIgZTY5ZjEyYWIyOGUwMTEwMTYzZTM5ZDM2YTlhYjJmZjk; star_sessionid=e69f12ab28e0110163e39d36a9ab2ff9; possess_scene_star_id=1843954326386436",
        "name": "Cookie-5", 
        "daily_limit": 500,
        "used_count": 0,
        "last_reset_date": None
    }
]
