import random
import Agently


def agent_config(message, keyword):

    moonshot_llm = {
        "base_url": "https://api.moonshot.cn/v1",
        "api_key": "sk-ycWLHr8AXE3dR4bNAUwNgWUDxTSS72jHAkR1e9JH50Cwhv3c"
    }

    agent = (
        Agently.create_agent()
        .set_settings("current_model", "OAIClient")
        .set_settings("model.OAIClient.auth", {"api_key": moonshot_llm['api_key']})
        .set_settings("model.OAIClient.url", moonshot_llm['base_url'])
        .set_settings("model.OAIClient.options", {"model": "moonshot-v1-32k"})
    )
    airdrop_result = (
        agent
        .input(message)
        .instruct("输出语言", "中文")
        .output({
            "twitter_list":
                {
                    "twitter_description": ("str", "推文信息，没有则返回None"),
                    "keyword": keyword,
                }

            ,
        })
        .start()
    )
    processed_twitter_list = []
    # for item in airdrop_result['twitter_list']:
    if airdrop_result['twitter_list'] and airdrop_result['twitter_list'][0]['twitter_description'] is not None:
        # 为每篇推文添加话题标签
        for item in airdrop_result['twitter_list']:
            item['twitter_description'] += " #XRAID"

        # 随机选择一篇推文
        selected_tweet = random.choice(airdrop_result['twitter_list'])
        return {"twitter_list": selected_tweet}
    # processed_twitter_list.append(airdrop_result['twitter_list'])
    #
    # # 返回处理后的推文列表，保持原有格式不变
    # return {"twitter_list": processed_twitter_list}
