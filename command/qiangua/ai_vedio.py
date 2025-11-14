import Agently

def ai_token_tweets():
    messages = '就是我们的二胎问题大家都很想知道你们什么时候提上这个日常哦 现在再被运解端就是你看我又去看中医嘛有在吃中医啊然后不是在减肥吗保持一个健康的身体要多运动就是不要压力太大吧但我们也会做一些运钱的检查做好准备嘛马亮是不是很想要个妹妹想不想要个妹妹好 我把我任务妈妈妹妹送下来我送了弟弟在这里一颗送了弟弟在这里一颗给我一个你让我把弟弟给你结哥我们是弟弟送给结哥那能送吗你送的送还能送啊 弟弟也得用啊真是白得一个 不来 不好意思啊。'
    message = (
        f"请根据以下视频内容进行分析：\n\n"
        f"{messages}\n\n"
        f"请按照以下格式进行分析总结：\n\n"
        f"🍀【视频主题】\n"
        f"[此处填写一句话概括视频主要内容]\n\n"
        f"✨【内容框架结构】\n"
        f"**开头**：[描述视频开头部分]\n"
        f"**中段**：[描述视频中间部分]\n"
        f"**结尾**：[描述视频结尾部分]\n\n"
        f"🔥【核心爆点（底层逻辑）】\n"
        f"[分析视频吸引观众的核心元素和情感共鸣点]\n\n"
        f"📒【方法论总结】\n"
        f"- **选题方向**：[分析视频选题的热点或痛点]\n"
        f"- **表现形式**：[分析视频的表现手法]\n"
        f"- **结构设计**：[分析视频的内容结构]\n"
        f"- **内容爆点元素**：[列举视频中的关键吸引元素]"
    )
    agent = agent_config(message)
    print(agent)

def agent_config(message):
    api_key = "sk-rf6Jt2vuF3WKzCs3MOz7LoffGTG5zyHYtMm2A9JmWr1QPUaI"
    base_url = "https://api.moonshot.cn/v1"

    agent = (
        Agently.create_agent()
        .set_settings("current_model", "OAIClient")
        .set_settings("model.OAIClient.auth", {"api_key": api_key})
        .set_settings("model.OAIClient.url", base_url)
        .set_settings("model.OAIClient.options", {"model": "moonshot-v1-32k"})
    )

    result = (
        agent
        .input(message)
        .instruct("输出语言", "中文")
        .output({
            "analysis": {
                "theme": ("str", "视频主题概括"),
                "structure": {
                    "intro": ("str", "开头部分描述"),
                    "middle": ("str", "中段部分描述"),
                    "ending": ("str", "结尾部分描述")
                },
                "core_points": ("str", "核心爆点分析"),
                "methodology": {
                    "topic": ("str", "选题方向分析"),
                    "expression": ("str", "表现形式分析"),
                    "structure": ("str", "结构设计分析"),
                    "key_elements": ("str", "内容爆点元素分析")
                }
            }
        })
        .start()
    )

    if result and result.get('analysis'):
        analysis = result['analysis']
        formatted_result = (
            f"🍀【视频主题】\n{analysis['theme']}\n\n"
            f"✨【内容框架结构】\n"
            f"**开头**：{analysis['structure']['intro']}\n"
            f"**中段**：{analysis['structure']['middle']}\n"
            f"**结尾**：{analysis['structure']['ending']}\n\n"
            f"🔥【核心爆点（底层逻辑）】\n{analysis['core_points']}\n\n"
            f"📒【方法论总结】\n"
            f"- **选题方向**：{analysis['methodology']['topic']}\n"
            f"- **表现形式**：{analysis['methodology']['expression']}\n"
            f"- **结构设计**：{analysis['methodology']['structure']}\n"
            f"- **内容爆点元素**：{analysis['methodology']['key_elements']}"
        )
        return {"analysis": formatted_result}

    return {"analysis": "无法生成分析结果"}

ai_token_tweets()