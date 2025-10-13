// 数据管理器 - 提供数据导入导出和备份功能

class DataManager {
    constructor() {
        this.init();
    }

    init() {
        // 添加数据管理按钮到界面
        this.addDataManagementUI();
        this.bindEvents();
    }

    // 添加数据管理UI到弹窗
    addDataManagementUI() {
        const footer = document.querySelector('.status-bar');
        if (footer) {
            const dataManagementDiv = document.createElement('div');
            dataManagementDiv.className = 'data-management';
            dataManagementDiv.innerHTML = `
                <div class="data-buttons">
                    <button id="exportBtn" class="data-btn" title="导出所有任务数据">导出</button>
                    <button id="importBtn" class="data-btn" title="从文件导入任务数据">导入</button>
                    <button id="clearBtn" class="data-btn danger" title="清除所有任务数据">清空</button>
                </div>
                <input type="file" id="fileInput" accept=".csv,.json" style="display: none;">
            `;

            // 添加样式
            const style = document.createElement('style');
            style.textContent = `
                .data-management {
                    margin-top: 10px;
                    padding-top: 10px;
                    border-top: 1px solid #eee;
                }
                .data-buttons {
                    display: flex;
                    gap: 5px;
                    justify-content: center;
                }
                .data-btn {
                    padding: 4px 8px;
                    font-size: 11px;
                    border: 1px solid #ddd;
                    background: #f8f9fa;
                    border-radius: 3px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }
                .data-btn:hover {
                    background: #e9ecef;
                }
                .data-btn.danger {
                    color: #dc3545;
                    border-color: #dc3545;
                }
                .data-btn.danger:hover {
                    background: #dc3545;
                    color: white;
                }
            `;
            document.head.appendChild(style);

            footer.appendChild(dataManagementDiv);
        }
    }

    // 绑定事件
    bindEvents() {
        document.getElementById('exportBtn')?.addEventListener('click', () => this.exportData());
        document.getElementById('importBtn')?.addEventListener('click', () => this.importData());
        document.getElementById('clearBtn')?.addEventListener('click', () => this.clearAllData());
        document.getElementById('fileInput')?.addEventListener('change', (e) => this.handleFileSelect(e));
    }

    // 导出为Excel格式（CSV）
    async exportData() {
        try {
            const result = await new Promise((resolve) => {
                chrome.storage.local.get(['worklist_tasks'], resolve);
            });

            const tasks = result.worklist_tasks || { daily: {}, weekly: {}, monthly: {} };

            // 生成CSV内容
            let csvContent = this.generateCSVContent(tasks);

            const blob = new Blob([csvContent], {
                type: 'text/csv;charset=utf-8;'
            });

            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `工作清单数据-${this.formatDate(new Date())}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            this.showMessage('数据导出成功！已保存为Excel文件', 'success');
        } catch (error) {
            console.error('导出失败:', error);
            this.showMessage('导出失败，请重试', 'error');
        }
    }

    // 生成CSV内容 - 日任务矩阵格式
    generateCSVContent(tasks) {
        let csvContent = '\uFEFF'; // UTF-8 BOM for Excel

        // 获取当前月份
        const currentDate = new Date();
        const currentYear = currentDate.getFullYear();
        const currentMonth = currentDate.getMonth();
        const monthKey = `${currentYear}-${String(currentMonth + 1).padStart(2, '0')}`;

        // 添加标题和导出信息
        csvContent += `工作清单 - ${currentYear}年${currentMonth + 1}月日任务完成情况\n`;
        csvContent += `导出时间：${new Date().toLocaleString()}\n\n`;

        // 获取当前月的任务
        const monthTasks = tasks.daily[monthKey] || [];

        if (monthTasks.length === 0) {
            csvContent += '本月暂无日任务\n';
            return csvContent;
        }

        // 获取当月天数
        const daysInMonth = new Date(currentYear, currentMonth + 1, 0).getDate();

        // 创建表头 - 任务名称 + 各个日期
        csvContent += '任务名称';
        for (let day = 1; day <= daysInMonth; day++) {
            csvContent += `,${day}日`;
        }
        csvContent += '\n';

        // 创建任务行数据
        monthTasks.forEach(task => {
            csvContent += `"${task.text}"`;

            // 为每一天添加完成状态
            for (let day = 1; day <= daysInMonth; day++) {
                const dayDate = new Date(currentYear, currentMonth, day);
                const dayKey = this.formatDateKey(dayDate);
                const isCompleted = task.dailyCompletions && task.dailyCompletions[dayKey];
                csvContent += `,${isCompleted ? '✓' : ''}`;
            }
            csvContent += '\n';
        });

        return csvContent;
    }

    // 格式化日期为 YYYY-MM-DD
    formatDateKey(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    // 导入数据
    importData() {
        document.getElementById('fileInput').click();
    }

    // 处理文件选择
    async handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;

        try {
            const text = await this.readFileAsText(file);
            let data;

            if (file.name.endsWith('.csv')) {
                // CSV文件处理
                data = this.parseCSVData(text);
            } else {
                // JSON文件处理
                data = JSON.parse(text);
            }

            // 验证数据格式
            if (!this.validateImportData(data)) {
                this.showMessage('无效的数据格式', 'error');
                return;
            }

            // 询问用户是否要覆盖现有数据
            if (confirm('导入数据将覆盖现有所有任务，是否继续？')) {
                await new Promise((resolve) => {
                    chrome.storage.local.set({
                        worklist_tasks: data.tasks,
                        lastUpdated: new Date().toISOString()
                    }, resolve);
                });

                this.showMessage('数据导入成功！请刷新页面查看', 'success');

                // 刷新页面数据
                setTimeout(() => {
                    window.location.reload();
                }, 1000);
            }
        } catch (error) {
            console.error('导入失败:', error);
            this.showMessage('导入失败，请检查文件格式', 'error');
        }

        // 清空文件输入
        event.target.value = '';
    }

    // 解析CSV数据（简单实现，实际使用可能需要更复杂的CSV解析）
    parseCSVData(csvText) {
        // 这里实现一个简单的CSV解析
        // 实际项目中建议使用专门的CSV解析库
        this.showMessage('CSV导入功能正在开发中，请使用JSON格式', 'info');
        return null;
    }

    // 清空所有数据
    async clearAllData() {
        if (!confirm('确定要清空所有任务数据吗？此操作不可恢复！')) {
            return;
        }

        if (!confirm('请再次确认：这将删除所有日、周、月任务数据！')) {
            return;
        }

        try {
            await new Promise((resolve) => {
                chrome.storage.local.set({
                    worklist_tasks: { daily: {}, weekly: {}, monthly: {} },
                    lastUpdated: new Date().toISOString()
                }, resolve);
            });

            this.showMessage('所有数据已清空', 'success');

            // 刷新页面
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } catch (error) {
            console.error('清空失败:', error);
            this.showMessage('清空失败，请重试', 'error');
        }
    }

    // 读取文件内容
    readFileAsText(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => resolve(e.target.result);
            reader.onerror = reject;
            reader.readAsText(file);
        });
    }

    // 验证导入数据格式
    validateImportData(data) {
        if (!data || typeof data !== 'object') return false;
        if (!data.tasks) return false;

        const { tasks } = data;

        // 检查是否为数组格式（新版本）
        if (Array.isArray(tasks)) {
            return true;
        }

        // 检查是否为旧版本格式
        if (tasks.daily || tasks.weekly || tasks.monthly) {
            return true;
        }

        return false;
    }

    // 显示消息
    showMessage(message, type = 'info') {
        // 创建消息元素
        const messageDiv = document.createElement('div');
        messageDiv.className = `message message-${type}`;
        messageDiv.textContent = message;
        messageDiv.style.cssText = `
            position: fixed;
            top: 10px;
            left: 50%;
            transform: translateX(-50%);
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 12px;
            z-index: 10000;
            color: white;
            background: ${type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : '#6c757d'};
        `;

        document.body.appendChild(messageDiv);

        // 3秒后自动移除
        setTimeout(() => {
            document.body.removeChild(messageDiv);
        }, 3000);
    }

    // 格式化日期为文件名
    formatDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hour = String(date.getHours()).padStart(2, '0');
        const minute = String(date.getMinutes()).padStart(2, '0');
        return `${year}${month}${day}-${hour}${minute}`;
    }
}

// 自动初始化数据管理器
document.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => {
        new DataManager();
    }, 500); // 稍微延迟以确保主应用已加载
});