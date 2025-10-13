import uvicorn
import sys

from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager

from starlette.middleware.cors import CORSMiddleware

from api import train

sys.setrecursionlimit(100000)

scheduler = BackgroundScheduler()

# 在这里添加任务到调度器
# scheduler.add_job(PGYSendYaoyue, 'cron', hour=4, minute=0) # 获取蒲公英微信
# scheduler.add_job(PGYGetInviteTime, 'cron', hour=4, minute=0) # 获取蒲公英微信
# print("定时任务启动成功。")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    print("程序结束，停止任务")
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，生产环境建议设置具体的源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)
# 引入路由
app.include_router(train.router, prefix="/api/train")
# app.include_router(contract.router, prefix="/api/contract")

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True)
