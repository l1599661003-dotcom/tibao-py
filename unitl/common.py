import random
import time


class Common:
    def random_sleep(self, min_seconds=1, max_seconds=5):
        """随机等待一段时间，避免被反爬"""
        sleep_time = random.uniform(min_seconds, max_seconds)
        time.sleep(sleep_time)
        return sleep_time
