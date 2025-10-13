@echo off
chcp 65001 >nul
echo.
echo ================================================
echo           任务管理插件安装助手
echo ================================================
echo.
echo 请按照以下步骤安装插件：
echo.
echo 1. 打开Chrome浏览器
echo 2. 在地址栏输入: chrome://extensions/
echo 3. 开启右上角的"开发者模式"
echo 4. 点击"加载已解压的扩展程序"
echo 5. 选择当前文件夹: %cd%
echo 6. 点击"选择文件夹"完成安装
echo.
echo ================================================
echo.
echo 安装完成后，您可以：
echo - 点击浏览器工具栏中的插件图标使用
echo - 右键点击插件图标选择"固定"以便快速访问
echo.
echo ================================================
echo           Firefox用户安装方法
echo ================================================
echo.
echo 1. 打开Firefox浏览器
echo 2. 在地址栏输入: about:debugging#/runtime/this-firefox
echo 3. 点击"临时载入附加组件"
echo 4. 选择本文件夹中的manifest.json文件
echo 5. 点击"打开"完成安装
echo.
echo 注意：Firefox中的临时载入在浏览器关闭后会失效
echo.
pause