import json
import os
import sys
import re
from datetime import datetime
from loguru import logger
import traceback
from core.database_text_tibao_2 import session
from models.models_tibao import FpOutBloggerNoteDetail


def setup_logger():
    """设置日志配置"""
    # 设置日志目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(current_dir, 'logs')
    os.makedirs(log_path, exist_ok=True)

    # 移除默认处理器，避免重复输出
    logger.remove()

    # 添加控制台输出
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

    # 添加文件输出
    logger.add(
        os.path.join(log_path, "process_notes_logs_{time:YYYY-MM-DD}.log"),
        rotation="1 day",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )


def get_exe_directory():
    """获取exe文件所在目录"""
    if hasattr(sys, '_MEIPASS'):
        # exe环境下，使用exe文件所在目录（不是临时解压目录）
        exe_dir = os.path.dirname(sys.executable)
    else:
        exe_dir = os.path.dirname(os.path.abspath(__file__))
    return exe_dir


def find_log_files():
    """查找日志文件"""
    current_dir = get_exe_directory()
    log_path = os.path.join(current_dir, 'logs')
    
    if not os.path.exists(log_path):
        logger.error(f"日志目录不存在: {log_path}")
        return []
    
    # 查找所有pgy_开头的日志文件
    log_files = []
    for file in os.listdir(log_path):
        if file.startswith('pgy_') and file.endswith('.log'):
            log_files.append(os.path.join(log_path, file))
    
    # 按修改时间排序，最新的在前面
    log_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    logger.info(f"找到 {len(log_files)} 个日志文件")
    for log_file in log_files:
        logger.info(f"日志文件: {os.path.basename(log_file)}")
    
    return log_files


def extract_notes_data_from_logs(log_files):
    """从日志文件中提取笔记详情数据"""
    all_notes_data = []
    failed_lines = []  # 记录失败的行
    total_processed_lines = 0
    
    for log_file in log_files:
        logger.info(f"正在处理日志文件: {os.path.basename(log_file)}")
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 用于存储当前处理的用户ID
            current_user_id = None
            user_id_found = False  # 标记是否找到用户ID
            file_processed_lines = 0
            
            for line_num, line in enumerate(lines, 1):
                # 每处理5000行输出一次进度
                if line_num % 5000 == 0:
                    logger.info(f"进度: {line_num}/{len(lines)} 行 ({line_num/len(lines)*100:.1f}%)")
                
                # 查找包含"访问页面:"的行，可能包含用户ID
                if "访问页面:" in line:
                    try:
                        # 页面URL通常包含用户ID
                        if "blogger-detail/" in line:
                            # 提取URL中的用户ID
                            url_match = re.search(r'blogger-detail/([^/\s]+)', line)
                            if url_match:
                                current_user_id = url_match.group(1)
                                user_id_found = True
                            
                    except Exception:
                        pass
                
                # 查找包含"笔记详情数据:"的行
                elif "笔记详情数据:" in line:
                    file_processed_lines += 1
                    
                    # 检查是否有有效的用户ID
                    if not user_id_found or not current_user_id:
                        # 尝试从当前行附近查找用户ID
                        current_user_id = find_user_id_nearby(lines, line_num, 10)  # 在前后10行内查找
                        if not current_user_id:
                            logger.warning(f"跳过第 {line_num} 行: 无法找到用户ID")
                            continue
                    
                    try:
                        # 提取JSON数据部分
                        json_start = line.find('{')
                        if json_start != -1:
                            json_str = line[json_start:]
                            
                            # 尝试清理和修复JSON字符串
                            cleaned_json = json_str.strip()
                            
                            # 处理单引号问题（Python字典使用单引号，JSON需要双引号）
                            if "'" in cleaned_json:
                                cleaned_json = cleaned_json.replace("'", '"')
                            
                            # 处理Python值
                            cleaned_json = cleaned_json.replace('None', 'null')
                            cleaned_json = cleaned_json.replace('True', 'true')
                            cleaned_json = cleaned_json.replace('False', 'false')
                            
                            # 处理前导零问题（如 05-26 -> 5-26）
                            cleaned_json = re.sub(r'\b0+(\d)', r'\1', cleaned_json)
                            
                            # 尝试解析JSON数据
                            data = None
                            parse_method = "unknown"
                            
                            # 方法1: 标准JSON解析
                            try:
                                data = json.loads(cleaned_json)
                                parse_method = "json.loads"
                            except json.JSONDecodeError:
                                # 方法2: 使用ast.literal_eval (更安全)
                                try:
                                    import ast
                                    data = ast.literal_eval(cleaned_json)
                                    parse_method = "ast.literal_eval"
                                except Exception:
                                    # 方法3: 使用eval (最后手段)
                                    try:
                                        data = eval(cleaned_json)
                                        parse_method = "eval"
                                    except Exception:
                                        continue
                            
                            # 验证数据结构
                            notes_list = None
                            
                            # 结构1: {'data': {'list': [...]}}
                            if isinstance(data, dict) and 'data' in data and 'list' in data['data']:
                                notes_list = data['data']['list']
                            
                            # 结构2: {'list': [...]} (实际日志中的结构)
                            elif isinstance(data, dict) and 'list' in data:
                                notes_list = data['list']
                            
                            # 验证笔记列表
                            if isinstance(notes_list, list) and len(notes_list) > 0:
                                # 为每条笔记数据添加来源信息和user_id
                                for note in notes_list:
                                    if isinstance(note, dict):
                                        # 添加缺失的字段
                                        note['user_id'] = current_user_id
                                        note['_source_file'] = os.path.basename(log_file)
                                        note['_source_line'] = line_num
                                        note['_extract_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        note['_parse_method'] = parse_method
                                
                                all_notes_data.extend(notes_list)
                                if len(notes_list) >= 5:  # 只记录较大的数据集
                                    logger.info(f"从第 {line_num} 行提取 {len(notes_list)} 条笔记数据 (用户ID: {current_user_id})")
                            else:
                                logger.debug(f"第 {line_num} 行: 笔记列表为空或格式不正确")
                        else:
                            logger.debug(f"第 {line_num} 行: 未找到JSON开始标记")
                    
                    except Exception as e:
                        logger.warning(f"处理第 {line_num} 行失败: {str(e)[:100]}")
                        failed_lines.append({
                            'file': os.path.basename(log_file),
                            'line': line_num,
                            'error': str(e),
                        })
                        continue
            
            total_processed_lines += file_processed_lines
            logger.info(f"文件 {os.path.basename(log_file)} 处理完成: 提取了 {file_processed_lines} 行笔记数据")
        
        except Exception as e:
            logger.error(f"处理日志文件 {log_file} 时出错: {str(e)}")
            continue
    
    # 输出统计信息
    logger.info(f"总共提取到 {len(all_notes_data)} 条笔记数据，处理了 {total_processed_lines} 行笔记详情")
    
    return all_notes_data


def find_user_id_nearby(lines, current_line, search_range=10):
    """在指定行附近查找用户ID"""
    try:
        start_line = max(1, current_line - search_range)
        end_line = min(len(lines), current_line + search_range)
        
        for line_num in range(start_line, end_line + 1):
            line = lines[line_num - 1]  # 转换为0基索引
            
            # 查找包含"访问页面:"的行
            if "访问页面:" in line and "blogger-detail/" in line:
                url_match = re.search(r'blogger-detail/([^/\s]+)', line)
                if url_match:
                    user_id = url_match.group(1)
                    return user_id
            
            # 查找包含"正在处理博主:"的行，可能包含用户信息
            elif "正在处理博主:" in line:
                # 尝试从这行提取用户ID
                user_match = re.search(r'博主ID[:：]\s*([^,\s]+)', line)
                if user_match:
                    return user_match.group(1)
        
        return None
        
    except Exception:
        return None


def process_notes_data(notes_data):
    """处理笔记数据，更新或创建数据库记录"""
    if not notes_data:
        logger.warning("没有笔记数据需要处理")
        return
    
    # 按用户ID分组处理
    user_notes = {}
    for note in notes_data:
        user_id = note.get('user_id')
        if not user_id or user_id == 'unknown':
            continue
        
        if user_id not in user_notes:
            user_notes[user_id] = []
        user_notes[user_id].append(note)
    
    logger.info(f"按用户ID分组后，共有 {len(user_notes)} 个用户的数据需要处理")
    
    total_processed = 0
    total_created = 0
    total_updated = 0
    batch_size = 100  # 批量提交大小
    
    for user_id, user_notes_list in user_notes.items():
        if len(user_notes_list) > 10:  # 只记录较大的数据集
            logger.info(f"处理用户 {user_id} 的 {len(user_notes_list)} 条笔记数据")
        
        user_processed = 0
        user_created = 0
        user_updated = 0
        
        try:
            for note in user_notes_list:
                try:
                    # 检查是否已存在相同的记录
                    existing_record = session.query(FpOutBloggerNoteDetail).filter(
                        FpOutBloggerNoteDetail.noteId == note.get('noteId'),
                        FpOutBloggerNoteDetail.user_id == user_id
                    ).first()

                    if existing_record:
                        # 更新已存在的记录
                        existing_record.readNum = note.get('readNum')
                        existing_record.likeNum = note.get('likeNum')
                        existing_record.collectNum = note.get('collectNum')
                        existing_record.isAdvertise = str(note.get('isAdvertise', False))
                        existing_record.isVideo = str(note.get('isVideo', False))
                        existing_record.imgUrl = note.get('imgUrl')
                        existing_record.title = note.get('title')
                        existing_record.brandName = note.get('brandName')
                        existing_record.date = note.get('date')
                        existing_record.advertise_switch = 1
                        existing_record.order_type = 1
                        existing_record.note_type = 3
                        existing_record.updated_at = datetime.now()
                        
                        user_updated += 1
                    else:
                        # 创建新的笔记记录
                        note_detail = FpOutBloggerNoteDetail(
                            readNum=note.get('readNum'),
                            likeNum=note.get('likeNum'),
                            collectNum=note.get('collectNum'),
                            isAdvertise=str(note.get('isAdvertise', False)),
                            isVideo=str(note.get('isVideo', False)),
                            noteId=note.get('noteId'),
                            imgUrl=note.get('imgUrl'),
                            title=note.get('title'),
                            brandName=note.get('brandName'),
                            date=note.get('date'),
                            user_id=user_id,
                            advertise_switch=1,
                            order_type=1,
                            note_type=3,
                        )

                        session.add(note_detail)
                        user_created += 1
                    
                    user_processed += 1
                    
                    # 批量提交，减少数据库压力
                    if user_processed % batch_size == 0:
                        session.commit()
                    
                except Exception as e:
                    logger.error(f"处理笔记 {note.get('noteId', 'unknown')} 失败: {str(e)[:100]}")
                    continue
            
            # 提交剩余的操作
            session.commit()
            
            total_processed += user_processed
            total_created += user_created
            total_updated += user_updated
            
            if len(user_notes_list) > 10:  # 只记录较大的数据集
                logger.info(f"用户 {user_id} 完成: 处理 {user_processed} 条，创建 {user_created} 条，更新 {user_updated} 条")
            
        except Exception as e:
            logger.error(f"处理用户 {user_id} 数据出错: {str(e)[:100]}")
            session.rollback()
            continue
    
    logger.info(f"所有数据处理完成！总计: 处理 {total_processed} 条，创建 {total_created} 条，更新 {total_updated} 条")


def save_extracted_data_to_file(notes_data, output_file):
    """将提取的数据保存到文件，用于备份和检查"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(notes_data, f, indent=2, ensure_ascii=False)
        logger.info(f"提取的数据已保存到: {output_file}")
    except Exception as e:
        logger.error(f"保存数据到文件时出错: {str(e)}")


def main():
    """主函数"""
    try:
        logger.info("=== 开始从日志文件提取笔记详情数据 ===")
        
        # 1. 查找日志文件
        log_files = find_log_files()
        if not log_files:
            logger.error("未找到可处理的日志文件")
            return
        
        # 2. 从日志文件中提取笔记详情数据
        notes_data = extract_notes_data_from_logs(log_files)
        if not notes_data:
            logger.warning("未从日志文件中提取到任何笔记数据")
            return
        
        # 3. 保存提取的数据到文件（备份）
        current_dir = get_exe_directory()
        backup_file = os.path.join(current_dir, 'extracted_notes_data.json')
        save_extracted_data_to_file(notes_data, backup_file)
        
        # 4. 处理笔记数据，更新或创建数据库记录
        process_notes_data(notes_data)
        
        logger.info("=== 日志数据提取和处理完成 ===")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
    finally:
        # 确保数据库会话被正确关闭
        try:
            session.close()
            logger.info("数据库会话已关闭")
        except:
            pass


if __name__ == "__main__":
    # 设置日志
    setup_logger()
    
    # 执行主程序
    main()