class WorklistManager {
    constructor() {
        this.currentView = 'day';
        this.currentDate = new Date();
        this.tasks = {
            daily: {}, // 日任务: {monthKey: [{id, text, dailyCompletions: {dateKey: true}}]}
            weekly: {}, // 周任务: {monthKey: [{id, text, weeklyCompletions: {weekNum: true}}]}
            monthly: {} // 月任务: {monthKey: [{id, text, completed: boolean}]}
        };
        this.init();
    }

    init() {
        this.loadTasks();
        this.bindEvents();
        this.updateDateDisplay();
        this.renderCurrentView();
        this.updateStats();
    }

    // 绑定事件监听器
    bindEvents() {
        // 视图切换
        document.getElementById('dayView').addEventListener('click', () => this.switchView('day'));
        document.getElementById('weekView').addEventListener('click', () => this.switchView('week'));
        document.getElementById('monthView').addEventListener('click', () => this.switchView('month'));
        document.getElementById('overviewView').addEventListener('click', () => this.switchView('overview'));

        // 日期导航
        document.getElementById('prevPeriod').addEventListener('click', () => this.navigatePeriod(-1));
        document.getElementById('nextPeriod').addEventListener('click', () => this.navigatePeriod(1));

        // 日视图任务添加
        document.getElementById('addDayTaskBtn').addEventListener('click', () => this.addDayTask());
        document.getElementById('dayTaskInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addDayTask();
        });

        // 月视图任务添加
        document.getElementById('addMonthTaskBtn').addEventListener('click', () => this.addMonthTask());
        document.getElementById('monthTaskInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addMonthTask();
        });

        // 周视图任务添加
        document.getElementById('addWeekTaskBtn').addEventListener('click', () => this.addWeekTask());
        document.getElementById('weekTaskInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addWeekTask();
        });

        // 总览视图标签切换
        this.bindOverviewTabs();
    }

    // 绑定总览视图标签切换事件
    bindOverviewTabs() {
        document.addEventListener('DOMContentLoaded', () => {
            document.getElementById('overviewDaily')?.addEventListener('click', () => this.switchOverviewTab('daily'));
            document.getElementById('overviewWeekly')?.addEventListener('click', () => this.switchOverviewTab('weekly'));
            document.getElementById('overviewMonthly')?.addEventListener('click', () => this.switchOverviewTab('monthly'));
        });
    }

    // 切换总览标签
    switchOverviewTab(tabType) {
        // 更新标签按钮状态
        document.querySelectorAll('.overview-tab-btn').forEach(btn => btn.classList.remove('active'));
        document.getElementById(`overview${tabType.charAt(0).toUpperCase() + tabType.slice(1)}`)?.classList.add('active');

        // 更新内容显示
        document.querySelectorAll('.overview-tab-content').forEach(content => content.classList.remove('active'));
        document.getElementById(`overview-${tabType}-content`)?.classList.add('active');

        // 渲染对应的总览内容
        this.renderOverviewTab(tabType);
    }

    // 视图切换
    switchView(view) {
        if (this.currentView === view) return;

        this.currentView = view;

        // 更新按钮状态
        document.querySelectorAll('.view-btn').forEach(btn => btn.classList.remove('active'));
        document.getElementById(view + 'View').classList.add('active');

        // 更新视图内容
        document.querySelectorAll('.view-content').forEach(content => content.classList.remove('active'));
        document.getElementById(view + '-view').classList.add('active');

        this.updateDateDisplay();
        this.renderCurrentView();
        this.updateStats();
    }

    // 日期导航
    navigatePeriod(direction) {
        if (this.currentView === 'day') {
            this.currentDate.setDate(this.currentDate.getDate() + direction);
        } else if (this.currentView === 'week' || this.currentView === 'month') {
            this.currentDate.setMonth(this.currentDate.getMonth() + direction);
        }
        this.updateDateDisplay();
        this.renderCurrentView();
        this.updateStats();
    }

    // 更新日期显示
    updateDateDisplay() {
        const periodElement = document.getElementById('currentPeriod');
        const subtitleElement = document.getElementById('dateSubtitle');

        if (this.currentView === 'day') {
            periodElement.textContent = this.formatDate(this.currentDate);
            subtitleElement.textContent = this.isToday(this.currentDate) ? '今日任务清单' : '当日任务清单';
        } else if (this.currentView === 'week') {
            const year = this.currentDate.getFullYear();
            const month = this.currentDate.getMonth() + 1;
            periodElement.textContent = `${year}年${month}月`;
            subtitleElement.textContent = '每周任务管理';
        } else {
            const year = this.currentDate.getFullYear();
            const month = this.currentDate.getMonth() + 1;
            periodElement.textContent = `${year}年${month}月`;
            subtitleElement.textContent = '月度任务目标';
        }
    }

    // 添加日视图月度任务
    addDayTask() {
        const input = document.getElementById('dayTaskInput');
        const text = input.value.trim();
        if (!text) return;

        const monthKey = this.getMonthKey(this.currentDate);
        if (!this.tasks.daily[monthKey]) {
            this.tasks.daily[monthKey] = [];
        }

        const task = {
            id: Date.now(),
            text: text,
            dailyCompletions: {}, // 存储每天的完成状态 {"2024-01-15": true}
            createdAt: new Date().toISOString()
        };

        this.tasks.daily[monthKey].push(task);
        input.value = '';
        this.saveTasks();
        this.renderDayView();
        this.updateStats();
    }

    // 添加周任务
    addWeekTask() {
        const input = document.getElementById('weekTaskInput');
        const text = input.value.trim();
        if (!text) return;

        const monthKey = this.getMonthKey(this.currentDate);
        if (!this.tasks.weekly[monthKey]) {
            this.tasks.weekly[monthKey] = [];
        }

        const task = {
            id: Date.now(),
            text: text,
            weeklyCompletions: {}, // 存储每周的完成状态 {1: true, 2: false, 3: true, 4: false}
            createdAt: new Date().toISOString()
        };

        this.tasks.weekly[monthKey].push(task);
        input.value = '';
        this.saveTasks();
        this.renderWeekView();
        this.updateStats();
    }

    // 添加月任务
    addMonthTask() {
        const input = document.getElementById('monthTaskInput');
        const text = input.value.trim();
        if (!text) return;

        const monthKey = this.getMonthKey(this.currentDate);
        if (!this.tasks.monthly[monthKey]) {
            this.tasks.monthly[monthKey] = [];
        }

        const task = {
            id: Date.now(),
            text: text,
            completed: false,
            createdAt: new Date().toISOString()
        };

        this.tasks.monthly[monthKey].push(task);
        input.value = '';
        this.saveTasks();
        this.renderMonthView();
        this.updateStats();
    }

    // 切换日视图任务完成状态
    toggleDailyTask(taskId, dateKey) {
        const monthKey = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.daily[monthKey] || [];
        const task = tasks.find(t => t.id === taskId);

        if (task) {
            if (!task.dailyCompletions) task.dailyCompletions = {};

            if (task.dailyCompletions[dateKey]) {
                delete task.dailyCompletions[dateKey];
            } else {
                task.dailyCompletions[dateKey] = true;
            }

            this.saveTasks();
            this.renderDayView();
            this.updateStats();
        }
    }

    // 切换周视图任务完成状态
    toggleWeeklyTask(taskId, week) {
        const monthKey = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.weekly[monthKey] || [];
        const task = tasks.find(t => t.id === taskId);

        if (task) {
            if (!task.weeklyCompletions) task.weeklyCompletions = {};

            if (task.weeklyCompletions[week]) {
                delete task.weeklyCompletions[week];
            } else {
                task.weeklyCompletions[week] = true;
            }

            this.saveTasks();
            this.renderWeekView();
            this.updateStats();
        }
    }

    // 切换任务完成状态
    toggleTask(type, key, taskId, week = null, dateKey = null) {
        let taskList;
        if (type === 'daily') {
            taskList = this.tasks.daily[key] || [];
        } else if (type === 'weekly') {
            taskList = this.tasks.weekly[key] ? this.tasks.weekly[key][week] || [] : [];
        } else if (type === 'monthly') {
            taskList = this.tasks.monthly[key] || [];
        }

        const task = taskList.find(t => t.id === taskId);
        if (task) {
            if (type === 'weekly' && week !== null) {
                // 周视图：切换特定周的完成状态
                if (!task.weeklyCompletions) task.weeklyCompletions = {};
                if (task.weeklyCompletions[week]) {
                    delete task.weeklyCompletions[week];
                } else {
                    task.weeklyCompletions[week] = true;
                }
            } else {
                // 月视图：切换常规完成状态
                task.completed = !task.completed;
            }
            this.saveTasks();
            this.renderCurrentView();
            this.updateStats();
        }
    }

    // 删除任务
    deleteTask(type, key, taskId, week = null) {
        let taskList;
        if (type === 'daily') {
            taskList = this.tasks.daily[key] || [];
        } else if (type === 'weekly') {
            taskList = this.tasks.weekly[key] ? this.tasks.weekly[key][week] || [] : [];
        } else if (type === 'monthly') {
            taskList = this.tasks.monthly[key] || [];
        }

        const index = taskList.findIndex(t => t.id === taskId);
        if (index !== -1) {
            taskList.splice(index, 1);
            this.saveTasks();
            this.renderCurrentView();
            this.updateStats();
        }
    }

    // 创建任务元素
    createTaskElement(task, type, key, week = null) {
        const taskItem = document.createElement('div');
        taskItem.className = `task-item ${task.completed ? 'completed' : ''}`;

        taskItem.innerHTML = `
            <input type="checkbox" class="task-checkbox" ${task.completed ? 'checked' : ''}>
            <span class="task-text">${task.text}</span>
            <div class="task-actions">
                <button class="task-delete" title="删除任务">×</button>
            </div>
        `;

        // 绑定勾选框事件
        const checkbox = taskItem.querySelector('.task-checkbox');
        checkbox.addEventListener('change', (e) => {
            e.stopPropagation();
            this.toggleTask(type, key, task.id, week);
        });

        // 绑定删除按钮事件
        const deleteBtn = taskItem.querySelector('.task-delete');
        deleteBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            if (confirm('确定要删除这个任务吗？')) {
                this.deleteTask(type, key, task.id, week);
            }
        });

        return taskItem;
    }

    // 渲染日视图 - 显示今日任务清单
    renderDayView() {
        const container = document.getElementById('dayTaskList');
        const countElement = document.getElementById('dayTaskCount');
        const monthKey = this.getMonthKey(this.currentDate);
        const dateKey = this.getDateKey(this.currentDate);
        const tasks = this.tasks.daily[monthKey] || [];

        countElement.textContent = `${tasks.length} 项任务`;
        container.innerHTML = '';

        if (tasks.length === 0) {
            container.innerHTML = `
                <div class="empty-state show">
                    <div class="empty-icon">📝</div>
                    <div class="empty-text">还没有本月任务</div>
                    <div class="empty-hint">添加本月任务，每天可以打勾完成</div>
                </div>
            `;
            return;
        }

        // 渲染每个任务
        tasks.forEach(task => {
            const isCompleted = task.dailyCompletions && task.dailyCompletions[dateKey];

            const taskItem = document.createElement('div');
            taskItem.className = `task-item ${isCompleted ? 'completed' : ''}`;

            taskItem.innerHTML = `
                <input type="checkbox" class="task-checkbox" ${isCompleted ? 'checked' : ''}
                       data-task-id="${task.id}" data-date="${dateKey}">
                <span class="task-text">${task.text}</span>
                <div class="task-actions">
                    <button class="task-delete" data-task-id="${task.id}" title="删除任务">×</button>
                </div>
            `;

            container.appendChild(taskItem);
        });

        // 绑定复选框事件
        container.querySelectorAll('.task-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                const taskId = parseInt(e.target.dataset.taskId);
                const dateKey = e.target.dataset.date;
                this.toggleDailyTask(taskId, dateKey);
            });
        });

        // 绑定删除按钮事件
        container.querySelectorAll('.task-delete').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const taskId = parseInt(e.target.dataset.taskId);
                if (confirm('确定要删除这个任务吗？')) {
                    this.deleteTask('daily', monthKey, taskId);
                }
            });
        });
    }

    // 渲染周视图
    renderWeekView() {
        const monthKey = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.weekly[monthKey] || [];

        for (let week = 1; week <= 4; week++) {
            const container = document.getElementById(`week${week}TaskList`);
            const countElement = document.getElementById(`week${week}Count`);

            container.innerHTML = '';
            countElement.textContent = `${tasks.length} 项任务`;

            if (tasks.length === 0) {
                container.innerHTML = '<div class="empty-state show"><div class="empty-text" style="font-size: 12px;">暂无任务</div></div>';
                continue;
            }

            // 为每周渲染所有任务
            tasks.forEach(task => {
                const isCompleted = task.weeklyCompletions && task.weeklyCompletions[week];

                const taskItem = document.createElement('div');
                taskItem.className = `task-item ${isCompleted ? 'completed' : ''}`;

                taskItem.innerHTML = `
                    <input type="checkbox" class="task-checkbox" ${isCompleted ? 'checked' : ''}
                           data-task-id="${task.id}" data-week="${week}">
                    <span class="task-text">${task.text}</span>
                    <div class="task-actions">
                        <button class="task-delete" data-task-id="${task.id}" title="删除任务">×</button>
                    </div>
                `;

                container.appendChild(taskItem);
            });

            // 绑定复选框事件
            container.querySelectorAll('.task-checkbox').forEach(checkbox => {
                checkbox.addEventListener('change', (e) => {
                    const taskId = parseInt(e.target.dataset.taskId);
                    const week = parseInt(e.target.dataset.week);
                    this.toggleWeeklyTask(taskId, week);
                });
            });

            // 绑定删除按钮事件
            container.querySelectorAll('.task-delete').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const taskId = parseInt(e.target.dataset.taskId);
                    if (confirm('确定要删除这个任务吗？')) {
                        this.deleteTask('weekly', monthKey, taskId);
                    }
                });
            });
        }
    }

    // 渲染月视图
    renderMonthView() {
        const container = document.getElementById('monthTaskList');
        const countElement = document.getElementById('monthTaskCount');
        const monthKey = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.monthly[monthKey] || [];

        container.innerHTML = '';
        countElement.textContent = `${tasks.length} 项任务`;

        if (tasks.length === 0) {
            container.innerHTML = '<div class="empty-state show"><div class="empty-icon">📝</div><div class="empty-text">还没有任务</div><div class="empty-hint">添加第一个任务开始管理</div></div>';
            return;
        }

        tasks.forEach(task => {
            const taskElement = this.createTaskElement(task, 'monthly', monthKey);
            container.appendChild(taskElement);
        });
    }

    // 渲染当前视图
    renderCurrentView() {
        if (this.currentView === 'day') {
            this.renderDayView();
        } else if (this.currentView === 'week') {
            this.renderWeekView();
        } else if (this.currentView === 'month') {
            this.renderMonthView();
        } else if (this.currentView === 'overview') {
            this.renderOverviewView();
        }
    }

    // 更新统计信息 - 按当前视图计算
    updateStats() {
        let totalTasks = 0;
        let completedTasks = 0;
        const today = this.getDateKey(this.currentDate);
        const monthKey = this.getMonthKey(this.currentDate);

        if (this.currentView === 'day') {
            // 日视图：统计今日任务
            const tasks = this.tasks.daily[monthKey] || [];
            totalTasks = tasks.length;
            completedTasks = tasks.filter(task =>
                task.dailyCompletions && task.dailyCompletions[today]
            ).length;
        } else if (this.currentView === 'week') {
            // 周视图：统计所有周任务完成情况
            const tasks = this.tasks.weekly[monthKey] || [];
            tasks.forEach(task => {
                for (let week = 1; week <= 4; week++) {
                    totalTasks++;
                    if (task.weeklyCompletions && task.weeklyCompletions[week]) {
                        completedTasks++;
                    }
                }
            });
        } else if (this.currentView === 'month') {
            // 月视图：统计月度任务
            const tasks = this.tasks.monthly[monthKey] || [];
            totalTasks = tasks.length;
            completedTasks = tasks.filter(t => t.completed).length;
        } else {
            // 总览视图：统计所有任务
            Object.values(this.tasks.daily).forEach(monthTasks => {
                monthTasks.forEach(task => {
                    if (task.dailyCompletions) {
                        const completionCount = Object.keys(task.dailyCompletions).length;
                        totalTasks += completionCount;
                        completedTasks += completionCount;
                    }
                });
            });

            Object.values(this.tasks.weekly).forEach(monthTasks => {
                monthTasks.forEach(task => {
                    if (task.weeklyCompletions) {
                        const completionCount = Object.keys(task.weeklyCompletions).length;
                        totalTasks += completionCount;
                        completedTasks += completionCount;
                    }
                });
            });

            Object.values(this.tasks.monthly).forEach(monthTasks => {
                totalTasks += monthTasks.length;
                completedTasks += monthTasks.filter(t => t.completed).length;
            });
        }

        const completionRate = totalTasks > 0 ? Math.round((completedTasks / totalTasks) * 100) : 0;

        document.getElementById('totalTasks').textContent = `总任务: ${totalTasks}`;
        document.getElementById('completedTasks').textContent = `已完成: ${completedTasks}`;
        document.getElementById('completionRate').textContent = `完成率: ${completionRate}%`;
    }

    // 渲染总览视图
    renderOverviewView() {
        // 默认显示日任务总览
        this.renderOverviewTab('daily');

        // 绑定标签切换事件（如果尚未绑定）
        setTimeout(() => {
            document.getElementById('overviewDaily')?.addEventListener('click', () => this.switchOverviewTab('daily'));
            document.getElementById('overviewWeekly')?.addEventListener('click', () => this.switchOverviewTab('weekly'));
            document.getElementById('overviewMonthly')?.addEventListener('click', () => this.switchOverviewTab('monthly'));
        }, 100);
    }

    // 渲染总览标签内容
    renderOverviewTab(tabType) {
        const container = document.getElementById(`overview${tabType.charAt(0).toUpperCase() + tabType.slice(1)}Matrix`);
        if (!container) return;

        if (tabType === 'daily') {
            this.renderDailyOverview(container);
        } else if (tabType === 'weekly') {
            this.renderWeeklyOverview(container);
        } else if (tabType === 'monthly') {
            this.renderMonthlyOverview(container);
        }
    }

    // 渲染日任务总览矩阵
    renderDailyOverview(container) {
        const currentMonth = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.daily[currentMonth] || [];

        if (tasks.length === 0) {
            container.innerHTML = `
                <div class="overview-matrix-title">本月日任务完成情况</div>
                <div class="matrix-empty-state">
                    <div class="matrix-empty-icon">📅</div>
                    <div class="matrix-empty-text">还没有日任务</div>
                    <div class="matrix-empty-hint">在日视图中添加任务开始追踪</div>
                </div>
            `;
            return;
        }

        // 获取当月天数
        const year = this.currentDate.getFullYear();
        const month = this.currentDate.getMonth();
        const daysInMonth = new Date(year, month + 1, 0).getDate();
        const today = new Date();
        const todayDateKey = this.getDateKey(today);

        let matrixHTML = `
            <div class="overview-matrix-title">本月日任务完成情况</div>
            <div class="matrix-header">
        `;

        // 添加日期头部
        for (let day = 1; day <= daysInMonth; day++) {
            const dayDate = new Date(year, month, day);
            const dayDateKey = this.getDateKey(dayDate);
            const isToday = dayDateKey === todayDateKey;
            matrixHTML += `<div class="matrix-date ${isToday ? 'today' : ''}">${day}</div>`;
        }
        matrixHTML += '</div>';

        // 添加任务行
        tasks.forEach(task => {
            matrixHTML += `
                <div class="matrix-row">
                    <div class="matrix-task-name">
                        <span title="${task.text}">${task.text}</span>
                    </div>
                    <div class="matrix-days">
            `;

            for (let day = 1; day <= daysInMonth; day++) {
                const dayDate = new Date(year, month, day);
                const dayDateKey = this.getDateKey(dayDate);
                const isCompleted = task.dailyCompletions && task.dailyCompletions[dayDateKey];

                matrixHTML += `
                    <div class="matrix-cell ${isCompleted ? 'completed' : ''}"
                         title="${task.text} - ${dayDateKey}">
                    </div>
                `;
            }

            matrixHTML += '</div></div>';
        });

        container.innerHTML = matrixHTML;
    }

    // 渲染周任务总览
    renderWeeklyOverview(container) {
        const currentMonth = this.getMonthKey(this.currentDate);
        const tasks = this.tasks.weekly[currentMonth] || [];

        // 计算当前是第几周
        const currentWeek = this.getCurrentWeekOfMonth();

        if (tasks.length === 0) {
            container.innerHTML = `
                <div class="overview-matrix-title">当前第${currentWeek}周任务完成情况</div>
                <div class="matrix-empty-state">
                    <div class="matrix-empty-icon">📊</div>
                    <div class="matrix-empty-text">还没有周任务</div>
                    <div class="matrix-empty-hint">在周视图中添加任务开始追踪</div>
                </div>
            `;
            return;
        }

        let matrixHTML = `
            <div class="overview-matrix-title">第${currentWeek}周任务完成情况</div>
            <div class="weekly-current-list">
        `;

        tasks.forEach(task => {
            const isCompleted = task.weeklyCompletions && task.weeklyCompletions[currentWeek];
            matrixHTML += `
                <div class="weekly-task-item ${isCompleted ? 'completed' : ''}">
                    <span class="task-status">${isCompleted ? '✓' : '○'}</span>
                    <span class="task-text">${task.text}</span>
                </div>
            `;
        });

        matrixHTML += '</div>';
        container.innerHTML = matrixHTML;
    }

    // 获取当前是第几周
    getCurrentWeekOfMonth() {
        const date = new Date(this.currentDate);
        const firstDay = new Date(date.getFullYear(), date.getMonth(), 1);
        const dayOfMonth = date.getDate();
        const firstDayOfWeek = firstDay.getDay(); // 0 = Sunday, 1 = Monday, etc.

        // 计算第一周有几天
        const firstWeekDays = 7 - firstDayOfWeek;

        if (dayOfMonth <= firstWeekDays) {
            return 1;
        } else {
            return Math.ceil((dayOfMonth - firstWeekDays) / 7) + 1;
        }
    }

    // 渲染月任务总览
    renderMonthlyOverview(container) {
        let matrixHTML = `
            <div class="overview-matrix-title">月任务完成情况</div>
            <div class="overview-monthly-list">
        `;

        // 显示最近几个月的月任务
        const currentDate = new Date();
        for (let i = 0; i < 6; i++) {
            const monthDate = new Date(currentDate.getFullYear(), currentDate.getMonth() - i, 1);
            const monthKey = this.getMonthKey(monthDate);
            const tasks = this.tasks.monthly[monthKey] || [];

            const year = monthDate.getFullYear();
            const month = monthDate.getMonth() + 1;

            matrixHTML += `
                <div class="monthly-overview-section">
                    <h4>${year}年${month}月 (${tasks.length}个任务)</h4>
                    <div class="monthly-tasks">
            `;

            if (tasks.length === 0) {
                matrixHTML += `<div class="empty-hint">无任务</div>`;
            } else {
                tasks.forEach(task => {
                    matrixHTML += `
                        <div class="monthly-task-item ${task.completed ? 'completed' : ''}">
                            <span class="task-status">${task.completed ? '✓' : '○'}</span>
                            <span class="task-text">${task.text}</span>
                        </div>
                    `;
                });
            }

            matrixHTML += '</div></div>';
        }

        matrixHTML += '</div>';
        container.innerHTML = matrixHTML;
    }

    // 辅助方法
    getDateKey(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    getMonthKey(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        return `${year}-${month}`;
    }

    formatDate(date) {
        const year = date.getFullYear();
        const month = date.getMonth() + 1;
        const day = date.getDate();
        const weekdays = ['日', '一', '二', '三', '四', '五', '六'];
        const weekday = weekdays[date.getDay()];
        return `${year}年${month}月${day}日 (周${weekday})`;
    }

    isToday(date) {
        const today = new Date();
        return date.toDateString() === today.toDateString();
    }

    // 数据持久化
    saveTasks() {
        chrome.storage.local.set({
            worklist_tasks: this.tasks,
            lastUpdated: new Date().toISOString()
        }, () => {
            console.log('Tasks saved successfully');
        });
    }

    loadTasks() {
        chrome.storage.local.get(['worklist_tasks'], (result) => {
            if (result.worklist_tasks) {
                this.tasks = result.worklist_tasks;
            }
            this.renderCurrentView();
            this.updateStats();
        });
    }
}

// 初始化应用
document.addEventListener('DOMContentLoaded', () => {
    new WorklistManager();
});