"""
消息推送模块
支持多种推送方式：
1. Server酱（微信推送）—— 推荐，最简单
2. pushplus（微信推送）
3. 钉钉机器人
4. Bark（iOS推送）
5. 企业微信
"""
import requests
import json
import os


class Notifier:
    def __init__(self):
        # 从环境变量读取配置（GitHub Secrets）
        self.serverchan_key = os.getenv('SERVERCHAN_KEY', '')
        self.pushplus_token = os.getenv('PUSHPLUS_TOKEN', '')
        self.dingtalk_webhook = os.getenv('DINGTALK_WEBHOOK', '')
        self.bark_key = os.getenv('BARK_KEY', '')
        self.wecom_key = os.getenv('WECOM_KEY', '')
    
    def send(self, title, content):
        """发送通知，按优先级尝试所有配置的渠道"""
        success = False
        
        if self.serverchan_key:
            success = self._send_serverchan(title, content) or success
        
        if self.pushplus_token:
            success = self._send_pushplus(title, content) or success
        
        if self.dingtalk_webhook:
            success = self._send_dingtalk(title, content) or success
        
        if self.bark_key:
            success = self._send_bark(title, content) or success
        
        if self.wecom_key:
            success = self._send_wecom(title, content) or success
        
        if not success:
            print("⚠️ 未配置任何推送渠道，仅打印结果：")
            print(f"标题: {title}")
            print(f"内容:\n{content}")
        
        return success

    def _send_serverchan(self, title, content):
        """
        Server酱推送（微信）
        注册地址: https://sct.ftqq.com/
        免费版每天5条
        """
        try:
            url = f'https://sctapi.ftqq.com/{self.serverchan_key}.send'
            data = {'title': title, 'desp': content}
            resp = requests.post(url, data=data, timeout=10)
            result = resp.json()
            if result.get('code') == 0:
                print("✅ Server酱推送成功")
                return True
            else:
                print(f"❌ Server酱推送失败: {result}")
                return False
        except Exception as e:
            print(f"❌ Server酱推送异常: {e}")
            return False
    
    def _send_pushplus(self, title, content):
        """
        PushPlus推送（微信）
        注册地址: https://www.pushplus.plus/
        免费版每天200条
        """
        try:
            url = 'https://www.pushplus.plus/send'
            data = {
                'token': self.pushplus_token,
                'title': title,
                'content': content.replace('\n', '<br>'),
                'template': 'html'
            }
            resp = requests.post(url, json=data, timeout=10)
            result = resp.json()
            if result.get('code') == 200:
                print("✅ PushPlus推送成功")
                return True
            else:
                print(f"❌ PushPlus推送失败: {result}")
                return False
        except Exception as e:
            print(f"❌ PushPlus推送异常: {e}")
            return False
    
    def _send_dingtalk(self, title, content):
        """
        钉钉机器人推送
        创建方式: 钉钉群 → 群设置 → 智能群助手 → 添加机器人 → 自定义
        """
        try:
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": title,
                    "text": f"## {title}\n\n{content}"
                }
            }
            resp = requests.post(
                self.dingtalk_webhook,
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            result = resp.json()
            if result.get('errcode') == 0:
                print("✅ 钉钉推送成功")
                return True
            else:
                print(f"❌ 钉钉推送失败: {result}")
                return False
        except Exception as e:
            print(f"❌ 钉钉推送异常: {e}")
            return False
    
    def _send_bark(self, title, content):
        """
        Bark推送（iOS）
        App Store搜索 Bark 下载
        """
        try:
            # 截断过长内容（Bark有长度限制）
            short_content = content[:500] if len(content) > 500 else content
            url = f'https://api.day.app/{self.bark_key}/{title}/{short_content}'
            resp = requests.get(url, timeout=10)
            result = resp.json()
            if result.get('code') == 200:
                print("✅ Bark推送成功")
                return True
            else:
                print(f"❌ Bark推送失败: {result}")
                return False
        except Exception as e:
            print(f"❌ Bark推送异常: {e}")
            return False
    
    def _send_wecom(self, title, content):
        """
        企业微信群机器人推送
        """
        try:
            url = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.wecom_key}'
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"## {title}\n\n{content}"
                }
            }
            resp = requests.post(url, json=data, timeout=10)
            result = resp.json()
            if result.get('errcode') == 0:
                print("✅ 企业微信推送成功")
                return True
            else:
                print(f"❌ 企业微信推送失败: {result}")
                return False
        except Exception as e:
            print(f"❌ 企业微信推送异常: {e}")
            return False