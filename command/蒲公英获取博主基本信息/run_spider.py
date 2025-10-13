from command.蒲公英获取博主基本信息.reptile_pgy_data_waicai import PGYSpider


def main():
    # 1. 初始化爬虫实例
    spider = PGYSpider()
    try:

        # 2. 执行登录
        login_success = spider.login()
        if not login_success:
            print("登录失败，请重试")
            return

        # 3. 执行抓取和收藏
        spider.scrape_user_notes()

    except Exception as e:
        print(f"运行出错: {e}")
    finally:
        if spider:  # 只有在spider成功创建时才调用close
            spider.close()


if __name__ == "__main__":
    main()