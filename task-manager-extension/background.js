// 背景脚本 - 处理插件初始化和存储管理

chrome.runtime.onInstalled.addListener((details) => {
    if (details.reason === 'install') {
        console.log('任务管理插件已安装');
        // 初始化存储数据
        chrome.storage.local.set({
            tasks: {
                daily: {},
                weekly: {},
                monthly: {}
            }
        }, () => {
            console.log('初始数据已设置');
        });
    } else if (details.reason === 'update') {
        console.log('任务管理插件已更新');
    }
});

// 监听存储变化
chrome.storage.onChanged.addListener((changes, namespace) => {
    if (namespace === 'local' && changes.tasks) {
        console.log('任务数据已更新');
    }
});

// 提供备份和恢复功能的API
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'exportTasks') {
        chrome.storage.local.get(['tasks'], (result) => {
            sendResponse({
                success: true,
                data: result.tasks || { daily: {}, weekly: {}, monthly: {} }
            });
        });
        return true; // 异步响应
    } else if (request.action === 'importTasks') {
        chrome.storage.local.set({ tasks: request.data }, () => {
            sendResponse({ success: true });
        });
        return true;
    } else if (request.action === 'clearAllTasks') {
        chrome.storage.local.set({
            tasks: {
                daily: {},
                weekly: {},
                monthly: {}
            }
        }, () => {
            sendResponse({ success: true });
        });
        return true;
    }
});