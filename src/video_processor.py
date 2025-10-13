import os
import requests
import subprocess
import time
import whisper
from pydub import AudioSegment
from sqlalchemy import func

from core.database_text_fangpian import session
from models.models_tibao import SpiderQianguaHotNote


class VideoProcessor:
    def __init__(self):
        print("正在加载Whisper模型...")
        # 使用更大的模型以提高准确度
        self.model = whisper.load_model("small")  # 从base升级到small，准确度更高
        print("初始化完成")

    def download_video(self, url: str, output_path: str) -> str:
        """下载视频文件"""
        try:
            # 创建输出目录
            os.makedirs(output_path, exist_ok=True)
            
            # 设置请求头，模拟浏览器行为
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            video_path = os.path.join(output_path, 'video.mp4')
            
            # 下载视频
            print("开始下载视频...")
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            
            # 写入视频文件
            with open(video_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"视频下载完成: {video_path}")
            return video_path
            
        except Exception as e:
            print(f"下载视频时出错: {str(e)}")
            raise

    def extract_audio(self, video_path: str, output_path: str) -> str:
        """从视频中提取音频"""
        try:
            print("开始提取音频...")
            wav_path = os.path.join(output_path, 'audio.wav')
            
            # 使用ffmpeg提取音频，优化音频质量
            command = [
                'ffmpeg', '-i', video_path,
                '-vn',  # 不处理视频
                '-acodec', 'pcm_s16le',  # 16位PCM编码
                '-ar', '16000',  # 采样率16kHz
                '-ac', '1',  # 单声道
                '-af', 'volume=1.5,atempo=0.85,highpass=f=50,lowpass=f=10000,areverse,silenceremove=start_periods=1:start_silence=0.1:start_threshold=-40dB,areverse',  # 音频优化
                '-y',  # 覆盖已存在的文件
                wav_path
            ]
            subprocess.run(command, shell=True, check=True)
            
            print(f"音频提取完成: {wav_path}")
            return wav_path
            
        except Exception as e:
            print(f"提取音频时出错: {str(e)}")
            raise

    def clean_text(self, text: str) -> str:
        """清理和优化识别出的文本"""
        # 移除多余的空格
        text = ' '.join(text.split())
        
        # 常见的标点和语气词处理
        replacements = {
            ' 的 ': '的',
            ' 了 ': '了',
            ' 吗 ': '吗',
            ' 呢 ': '呢',
            ' 啊 ': '啊',
            ' 吧 ': '吧',
            ' 嘛 ': '嘛',
            ' 呀 ': '呀',
            ' 哦 ': '哦',
            ' 噢 ': '噢',
            ' 哈 ': '哈',
            ' 么 ': '么',
            ' 喔 ': '喔',
            '  ': ' ',
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # 在句子结尾添加句号
        sentences = text.split('。')
        cleaned_sentences = []
        for sentence in sentences:
            if sentence.strip():
                # 处理问号和感叹号
                if any(punct in sentence for punct in ['?', '？', '!', '！']):
                    cleaned_sentences.append(sentence.strip())
                else:
                    cleaned_sentences.append(sentence.strip() + '。')
        
        return ''.join(cleaned_sentences)

    def extract_text(self, audio_path: str) -> str:
        """从音频中提取文字"""
        try:
            print("开始识别文字...")
            
            # 使用Whisper进行语音识别，添加更多参数
            result = self.model.transcribe(
                audio_path,
                language='zh',  # 指定中文
                task='transcribe',  # 转录任务
                initial_prompt="这是一段中文视频的音频，内容可能包含日常对话和解说",  # 更详细的提示词
                temperature=0.2,  # 降低随机性
                best_of=2,  # 生成多个结果并选择最佳
                condition_on_previous_text=True,  # 考虑上下文
                compression_ratio_threshold=2.4,  # 压缩比阈值
                no_speech_threshold=0.6  # 无语音检测阈值
            )
            
            # 获取识别结果
            text = result["text"]
            
            # 清理文本
            text = self.clean_text(text)
            
            if text:
                print("文字识别完成")
                return text
            else:
                print("无法识别音频内容")
                return ""
                
        except Exception as e:
            print(f"提取文字时出错: {str(e)}")
            raise

def main():
    videos = session.query(SpiderQianguaHotNote).filter(func.length(SpiderQianguaHotNote.video_url) > 0).all()
    processed_count = 0
    failed_videos = []

    for video in videos:
        video_url = video.video_url
        video_id = video.id  # 假设有id字段，如果没有可以用其他唯一标识

        # 创建输出目录
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        # 创建处理器实例
        processor = VideoProcessor()

        try:
            # 下载视频
            video_path = processor.download_video(video_url, output_dir)

            # 提取音频
            audio_path = processor.extract_audio(video_path, output_dir)

            # 识别文字
            text = processor.extract_text(audio_path)

            # 更新数据库
            try:
                video.note_video_text = text  # 更改字段名以更好地反映其内容
                session.commit()
                processed_count += 1
            except Exception as db_error:
                session.rollback()
                print(f"数据库更新失败 (视频ID: {video_id}): {str(db_error)}")
                failed_videos.append((video_id, "数据库更新失败"))

            # 清理临时文件
            os.remove(video_path)
            os.remove(audio_path)

            print(f"\n视频 {video_id} 处理完成")
            print("-" * 50)
            print(text)
            print("-" * 50)

        except Exception as e:
            failed_videos.append((video_id, str(e)))
            print(f"处理视频 {video_id} 时出错: {str(e)}")
            session.rollback()
        
        finally:
            # 确保清理临时文件，即使处理失败
            try:
                if 'video_path' in locals() and os.path.exists(video_path):
                    os.remove(video_path)
                if 'audio_path' in locals() and os.path.exists(audio_path):
                    os.remove(audio_path)
            except Exception as cleanup_error:
                print(f"清理临时文件失败: {str(cleanup_error)}")

    # 输出处理总结
    print("\n处理完成总结:")
    print(f"成功处理: {processed_count} 个视频")
    print(f"失败: {len(failed_videos)} 个视频")
    
    if failed_videos:
        print("\n失败的视频列表:")
        for video_id, error in failed_videos:
            print(f"视频 ID {video_id}: {error}")

if __name__ == "__main__":
    main() 