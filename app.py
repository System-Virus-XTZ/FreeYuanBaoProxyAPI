#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FreeYuanBaoProxyAPI - API代理平台 + 元宝 Bot 守护进程
Single unified Quart app: OpenAI 兼容代理 + 元宝 WebSocket + 管理后台 + 文件发送
"""

import asyncio
import json
import os
import re
import sys
import time
import uuid
import logging
import random
import hmac
import hashlib
import sqlite3
import secrets
import string
import hashlib
import itertools
from datetime import datetime, timedelta
from functools import wraps
from collections import defaultdict

import aiofiles
from quart import (
    Quart, request, jsonify, render_template, redirect,
    url_for, session, g, Response as QuartResponse, stream_with_context
)

# ─────────────────────────────────────────────────────────────
# 0. 日志配置
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "tmp", "fused.log")

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", datefmt="%H:%M:%S"))
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

logger = logging.getLogger("fused")
logger.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(sh)

# ─────────────────────────────────────────────────────────────
# 1. 加载配置
# ─────────────────────────────────────────────────────────────
FYBPAPI_CONFIG = {}
try:
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        FYBPAPI_CONFIG = json.load(f)
except Exception as e:
    logger.warning(f"未找到 config.json: {e}")

# FYBPAPI 元宝凭证
APP_ID        = FYBPAPI_CONFIG.get("APP_ID", "")
APP_SECRET    = FYBPAPI_CONFIG.get("APP_SECRET", "")
GROUP_CODE    = FYBPAPI_CONFIG.get("GROUP_CODE", "")
YUANBAO_USER_ID = FYBPAPI_CONFIG.get("YUANBAO_USER_ID", "")
YUANBAO_NICK  = FYBPAPI_CONFIG.get("YUANBAO_NICK", "元宝")
API_DOMAIN    = FYBPAPI_CONFIG.get("API_DOMAIN", "bot.yuanbao.tencent.com")
WS_URL        = FYBPAPI_CONFIG.get("WS_URL", "wss://bot-wss.yuanbao.tencent.com/wss/connection")
FYBPAPI_PORT  = int(FYBPAPI_CONFIG.get("PORT", 8000))
FYBPAPI_DEBUG = FYBPAPI_CONFIG.get("debug", False)
ADMIN_PASSWORD = FYBPAPI_CONFIG.get("admin_password", "admin123")
FYBPAPI_KEY   = FYBPAPI_CONFIG.get("API_KEY", "")

# ─────────────────────────────────────────────────────────────
# 4. 解析 apikey.json 和 config.json（支持多模型多key）
# ─────────────────────────────────────────────────────────────
def parse_apikey_json(filepath: str) -> list:
    """从 apikey.json 加载默认keys列表"""
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        default_keys = data.get("default", [])
        # 兼容旧格式（单个字符串）
        if isinstance(default_keys, str):
            default_keys = [default_keys] if default_keys else []
        return default_keys
    except Exception as e:
        logger.warning(f"apikey.json 解析失败: {e}")
        return []

def parse_models_from_config(config_data: dict) -> list:
    """从 config.json 加载模型配置"""
    models = []
    models_data = config_data.get("models", {})
    
    for model_key, model_data in models_data.items():
        if isinstance(model_data, dict):
            base_url = model_data.get("base_url", "")
            name = model_data.get("name", model_key)
            api_keys = model_data.get("api_keys", [])
            if api_keys:
                models.append({
                    "base_url": base_url,
                    "name": name,
                    "api_keys": api_keys
                })
    return models

# 加载配置
APIKEY_FILE = os.path.join(BASE_DIR, "apikey.json")
RAW_MODELS = parse_models_from_config(FYBPAPI_CONFIG)
FYBPAPI_KEYS = parse_apikey_json(APIKEY_FILE)
FYBPAPI_KEY_IDX = 0  # 当前使用的key索引

# FreeYuanBaoProxyAPI 配置
DB_FILE = os.path.join(BASE_DIR, "tmp", "relay.db")
PROXY_PORT    = 8000

def get_next_api_key() -> str:
    """获取下一个API key（轮询）"""
    global FYBPAPI_KEY_IDX
    if not FYBPAPI_KEYS:
        return FYBPAPI_CONFIG.get("API_KEY", "")
    key = FYBPAPI_KEYS[FYBPAPI_KEY_IDX % len(FYBPAPI_KEYS)]
    FYBPAPI_KEY_IDX += 1
    return key

# 兼容：单个key属性（保留第一个）
FYBPAPI_KEY = FYBPAPI_KEYS[0] if FYBPAPI_KEYS else FYBPAPI_CONFIG.get("API_KEY", "")

# ─────────────────────────────────────────────────────────────
# 2. Protobuf 编解码（来自 FYBPAPI）
# ─────────────────────────────────────────────────────────────
class ProtobufCodec:
    @staticmethod
    def encode_varint(value: int) -> bytes:
        result = []
        while value > 127:
            result.append((value & 0x7f) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)

    @staticmethod
    def decode_varint(data: bytes, pos: int = 0) -> tuple:
        result, shift = 0, 0
        while True:
            b = data[pos]; pos += 1
            result |= (b & 0x7f) << shift
            if not (b & 0x80): break
            shift += 7
        return result, pos

    @staticmethod
    def encode_string(field: int, value: str) -> bytes:
        tag = (field << 3) | 2
        encoded = value.encode("utf-8")
        return bytes([tag]) + ProtobufCodec.encode_varint(len(encoded)) + encoded

    @staticmethod
    def encode_bytes(field: int, value: bytes) -> bytes:
        tag = (field << 3) | 2
        return bytes([tag]) + ProtobufCodec.encode_varint(len(value)) + value

    @staticmethod
    def encode_uint32(field: int, value: int) -> bytes:
        tag = (field << 3) | 0
        return bytes([tag]) + ProtobufCodec.encode_varint(value)

    @staticmethod
    def encode_message_field(field: int, inner: bytes) -> bytes:
        tag = (field << 3) | 2
        return bytes([tag]) + ProtobufCodec.encode_varint(len(inner)) + inner

    @staticmethod
    def encode_head(cmd_type: int, cmd: str, seq_no: int, msg_id: str, module: str) -> bytes:
        data = b""
        data += ProtobufCodec.encode_uint32(1, cmd_type)
        data += ProtobufCodec.encode_string(2, cmd)
        data += ProtobufCodec.encode_uint32(3, seq_no)
        data += ProtobufCodec.encode_string(4, msg_id)
        data += ProtobufCodec.encode_string(5, module)
        return data

    @staticmethod
    def encode_conn_msg(head: bytes, data: bytes = b"") -> bytes:
        result = ProtobufCodec.encode_message_field(1, head)
        if data:
            result += ProtobufCodec.encode_bytes(2, data)
        return result

    @staticmethod
    def encode_auth_bind_req(biz_id: str, uid: str, source: str, token: str) -> bytes:
        data = ProtobufCodec.encode_string(1, biz_id)
        auth_info = b""
        auth_info += ProtobufCodec.encode_string(1, uid)
        auth_info += ProtobufCodec.encode_string(2, source)
        auth_info += ProtobufCodec.encode_string(3, token)
        data += ProtobufCodec.encode_message_field(2, auth_info)
        return data

    @staticmethod
    def encode_tim_file_elem(url: str, uuid_str: str = "", file_size: int = 0, file_name: str = "") -> bytes:
        msg_content = b""
        if uuid_str:
            msg_content += ProtobufCodec.encode_string(2, uuid_str)
        msg_content += ProtobufCodec.encode_string(10, url)
        if file_size:
            msg_content += bytes([(11 << 3) | 0]) + ProtobufCodec.encode_varint(file_size)
        if file_name:
            msg_content += ProtobufCodec.encode_string(12, file_name)
        elem = b""
        elem += ProtobufCodec.encode_string(1, "TIMFileElem")
        elem += ProtobufCodec.encode_message_field(2, msg_content)
        return elem

    @staticmethod
    def encode_send_group_msg_req(msg_id: str, group_code: str,
                                   from_account: str, text: str,
                                   at_user_id: str = "", at_nickname: str = "") -> bytes:
        data = b""
        data += ProtobufCodec.encode_string(1, msg_id)
        data += ProtobufCodec.encode_string(2, group_code)
        data += ProtobufCodec.encode_string(3, from_account)
        data += ProtobufCodec.encode_string(5, str(random.randint(1, 999999999)))
        if at_user_id:
            at_data = json.dumps({"elem_type": 1002, "text": f"@{at_nickname or at_user_id}", "user_id": at_user_id})
            at_content = ProtobufCodec.encode_string(4, at_data)
            at_elem = b""
            at_elem += ProtobufCodec.encode_string(1, "TIMCustomElem")
            at_elem += ProtobufCodec.encode_message_field(2, at_content)
            data += ProtobufCodec.encode_message_field(6, at_elem)
        msg_content = ProtobufCodec.encode_string(1, text)
        msg_body_elem = ProtobufCodec.encode_string(1, "TIMTextElem")
        msg_body_elem += ProtobufCodec.encode_message_field(2, msg_content)
        data += ProtobufCodec.encode_message_field(6, msg_body_elem)
        return data

    @staticmethod
    def decode_conn_msg(data: bytes) -> dict:
        result = {"head": {}, "data": b""}
        pos = 0
        while pos < len(data):
            tag = data[pos]; pos += 1
            field, wire = tag >> 3, tag & 7
            if field == 1 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["head"] = ProtobufCodec._decode_head(data[pos:pos+length]); pos += length
            elif field == 2 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["data"] = data[pos:pos+length]; pos += length
            else:
                if wire == 0: ProtobufCodec.decode_varint(data, pos); pos += 1
                elif wire == 2:
                    length, pos = ProtobufCodec.decode_varint(data, pos); pos += length
                else: break
        return result

    @staticmethod
    def _decode_head(data: bytes) -> dict:
        result = {}; pos = 0
        while pos < len(data):
            tag = data[pos]; pos += 1
            field, wire = tag >> 3, tag & 7
            if field == 1 and wire == 0: result["cmdType"], pos = ProtobufCodec.decode_varint(data, pos)
            elif field == 2 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["cmd"] = data[pos:pos+length].decode("utf-8"); pos += length
            elif field == 3 and wire == 0: result["seqNo"], pos = ProtobufCodec.decode_varint(data, pos)
            elif field == 4 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["msgId"] = data[pos:pos+length].decode("utf-8"); pos += length
            elif field == 5 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["module"] = data[pos:pos+length].decode("utf-8"); pos += length
            elif field == 10 and wire == 0: result["status"], pos = ProtobufCodec.decode_varint(data, pos)
            else:
                if wire == 0: ProtobufCodec.decode_varint(data, pos); pos += 1
                elif wire == 2: length, pos = ProtobufCodec.decode_varint(data, pos); pos += length
                else: break
        return result

    @staticmethod
    def decode_auth_bind_rsp(data: bytes) -> dict:
        result = {}; pos = 0
        while pos < len(data):
            tag = data[pos]; pos += 1
            field, wire = tag >> 3, tag & 7
            if field == 1 and wire == 0: result["code"], pos = ProtobufCodec.decode_varint(data, pos)
            elif field == 2 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["message"] = data[pos:pos+length].decode("utf-8"); pos += length
            elif field == 3 and wire == 2:
                length, pos = ProtobufCodec.decode_varint(data, pos)
                result["connectId"] = data[pos:pos+length].decode("utf-8"); pos += length
            else:
                if wire == 0: ProtobufCodec.decode_varint(data, pos); pos += 1
                elif wire == 2: length, pos = ProtobufCodec.decode_varint(data, pos); pos += length
                else: break
        return result

    @staticmethod
    def decode_inbound_push(data: bytes) -> dict:
        try:
            raw = json.loads(data.decode("utf-8"))
            result = {
                "from_account": raw.get("from_account", ""),
                "group_code":   raw.get("group_code", ""),
                "msg_id":       raw.get("msg_id", ""),
                "text": ""
            }
            for elem in raw.get("msg_body", []):
                if isinstance(elem, dict) and elem.get("msg_type") == "TIMTextElem":
                    mc = elem.get("msg_content", {})
                    if isinstance(mc, dict):
                        result["text"] = mc.get("text", "") or ""
                        if result["text"]: break
            return result
        except Exception:
            return {"from_account": "", "group_code": "", "msg_id": "", "text": ""}

    @staticmethod
    def extract_content_text(content) -> str:
        if not content: return ""
        if isinstance(content, str): return content
        if isinstance(content, list):
            return "\n".join(p.get("text","") for p in content if isinstance(p, dict) and p.get("type") == "text")
        return str(content)


# ─────────────────────────────────────────────────────────────
# 3. 元宝 WebSocket 客户端（全局单例）
# ─────────────────────────────────────────────────────────────
# 协议常量
CMD_TYPE_REQUEST = 0; CMD_TYPE_RESPONSE = 1; CMD_TYPE_PUSH = 2; CMD_TYPE_PUSH_ACK = 3
CMD_AUTH_BIND = "auth-bind"; CMD_PING = "ping"
MODULE_CONN_ACCESS = "conn_access"; BIZ_MODULE = "yuanbao_openclaw_proxy"


class YuanbaoClient:
    """元宝 Bot WebSocket 客户端（全局共享，供所有请求使用）"""

    def __init__(self):
        self.codec       = ProtobufCodec
        self.ws         = None
        self.connected  = False
        self.seq_no     = 0
        self.token      = None
        self.bot_id     = None
        self.connect_id = None
        self.instance_id = str(random.randint(1, 1000))
        self._lock      = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}  # correlation_id -> Future

    def _msg_id(self) -> str: return uuid.uuid4().hex
    def _nonce(self) -> str: return "".join(random.choices(string.hexdigits.lower(), k=32))

    def _beijing_time(self) -> str:
        from datetime import timezone, timedelta, datetime
        ts = time.time() + 8 * 3600
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    def _sign(self, nonce: str, ts: str) -> str:
        plain = f"{nonce}{ts}{APP_ID}{APP_SECRET}"
        return hmac.new(APP_SECRET.encode(), plain.encode(), hashlib.sha256).hexdigest()

    def sign_token(self):
        import requests
        url = f"https://{API_DOMAIN}/api/v5/robotLogic/sign-token"
        nonce = self._nonce(); ts = self._beijing_time()
        headers = {"Content-Type": "application/json", "X-AppVersion": "1.0.11",
                   "X-OperationSystem": "linux", "X-Instance-Id": self.instance_id, "X-Bot-Version": "2026.3.22"}
        body = {"app_key": APP_ID, "nonce": nonce, "signature": self._sign(nonce, ts), "timestamp": ts}
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        result = resp.json()
        if result.get("code") == 0:
            data = result["data"]
            self.token  = data["token"]
            self.bot_id = data["bot_id"]
            logger.info(f"[Yuanbao] 签票成功 bot_id={self.bot_id}")
        else:
            raise Exception(f"签票失败: {result}")

    async def connect(self):
        import websockets
        if not self.token: self.sign_token()
        logger.info(f"[Yuanbao] 连接 WebSocket: {WS_URL}")
        self.ws = await websockets.connect(WS_URL)
        auth_id = self._msg_id()
        auth_data = self.codec.encode_auth_bind_req("ybBot", self.bot_id or "", "web", self.token or "")
        head = self.codec.encode_head(CMD_TYPE_REQUEST, CMD_AUTH_BIND, self.seq_no, auth_id, MODULE_CONN_ACCESS)
        self.seq_no += 1
        await self.ws.send(self.codec.encode_conn_msg(head, auth_data))
        resp = await self.ws.recv()
        conn = self.codec.decode_conn_msg(resp); h = conn["head"]
        if h.get("cmd") == CMD_AUTH_BIND:
            rsp = self.codec.decode_auth_bind_rsp(conn["data"])
            code = rsp.get("code", 0)
            if code == 0 or code == 41101:
                self.connect_id = rsp.get("connectId")
                self.connected = True
                logger.info(f"[Yuanbao] 鉴权成功 connectId={self.connect_id}")
            else:
                raise Exception(f"鉴权失败: {rsp}")
        else:
            raise Exception(f"意外响应: {h}")

    async def _send_ws(self, cmd: str, biz_data: bytes, module: str = BIZ_MODULE) -> None:
        head = self.codec.encode_head(CMD_TYPE_REQUEST, cmd, self.seq_no, self._msg_id(), module)
        self.seq_no += 1
        await self.ws.send(self.codec.encode_conn_msg(head, biz_data))

    async def send_group_message(self, text: str, at_user_id: str = "", at_nickname: str = "") -> bool:
        if not self.ws or not self.connected:
            logger.warning("[Yuanbao] 发送失败: 未连接"); return False
        biz_data = self.codec.encode_send_group_msg_req(
            self._msg_id(), GROUP_CODE, self.bot_id or "", text,
            at_user_id=at_user_id, at_nickname=at_nickname)
        await self._send_ws("send_group_message", biz_data)
        logger.info(f"[Yuanbao] 群消息已发送: {text[:60]}")
        return True

    async def _send_ping(self):
        await self._send_ws(CMD_PING, b"", MODULE_CONN_ACCESS)

    async def _send_ack(self, head: dict):
        ack_head = self.codec.encode_head(CMD_TYPE_PUSH_ACK, head.get("cmd",""),
            self.seq_no, head.get("msgId",""), head.get("module",""))
        self.seq_no += 1
        await self.ws.send(self.codec.encode_conn_msg(ack_head))

    async def _get_upload_info(self, filename: str, file_id: str) -> dict | None:
        import requests
        if not self.bot_id or not self.token: return None
        url = f"https://{API_DOMAIN}/api/resource/genUploadInfo"
        headers = {"Content-Type": "application/json", "X-ID": self.bot_id, "X-Token": self.token,
                   "X-Source": "web", "X-AppVersion": "2.0.1", "X-OperationSystem": "Linux", "X-Instance-Id": "99"}
        body = {"fileName": filename, "fileId": file_id, "docFrom": "localDoc", "docOpenId": ""}
        try:
            r = requests.post(url, headers=headers, json=body, timeout=30)
            result = r.json()
            if result.get("code", 0) == 0: return result.get("data", result)
        except Exception as e:
            logger.warning(f"[Yuanbao] 获取上传凭证失败: {e}")
        return None

    async def _upload_to_cos(self, config: dict, data: bytes, filename: str) -> str | None:
        try:
            bucket = config.get("bucketName", ""); region = config.get("region", "")
            secret_id = config.get("encryptTmpSecretId", ""); secret_key = config.get("encryptTmpSecretKey", "")
            token = config.get("encryptToken", "")
            start_t = config.get("startTime", 0); expired_t = config.get("expiredTime", 0)
            location = config.get("location", "")
            key_time = f"{start_t};{expired_t}"
            sign_key = hmac.new(secret_key.encode(), key_time.encode(), hashlib.sha1).hexdigest()
            http_str = f"put\n{location}\n\nhost={bucket}.cos.{region}.myqcloud.com\n"
            str2sign = f"sha1\n{key_time}\n{hashlib.sha1(http_str.encode()).hexdigest()}\n"
            sig = hmac.new(sign_key.encode(), str2sign.encode(), hashlib.sha1).hexdigest()
            auth = f"q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={key_time}&q-key-time={key_time}&q-header-list=host&q-url-param-list=&q-signature={sig}"
            if token: auth += f"&x-cos-security-token={token}"
            upload_url = f"https://{bucket}.cos.{region}.myqcloud.com{location}"
            headers = {"Host": f"{bucket}.cos.{region}.myqcloud.com", "Authorization": auth,
                       "Content-Type": "application/octet-stream"}
            if token: headers["x-cos-security-token"] = token
            import requests
            r = requests.put(upload_url, headers=headers, data=data, timeout=60)
            if r.status_code == 200:
                return config.get("resourceUrl", upload_url)
        except Exception as e:
            logger.warning(f"[Yuanbao] COS 上传失败: {e}")
        return None

    async def _build_file_msg(self, url: str, uuid_str: str = "", file_size: int = 0, file_name: str = "") -> bytes:
        file_elem = self.codec.encode_tim_file_elem(url, uuid_str, file_size, file_name)
        data = b""
        data += self.codec.encode_string(1, self._msg_id())
        data += self.codec.encode_string(2, GROUP_CODE)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(4, "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, file_elem)
        data += self.codec.encode_string(7, "")
        head = self.codec.encode_head(CMD_TYPE_REQUEST, "send_group_message",
                                        self.seq_no, self._msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    async def send_file_bytes(self, data: bytes, filename: str, file_id: str = "") -> bool:
        """发送内存中的文件数据到群聊"""
        if not self.connected or not self.ws: return False
        if not file_id: file_id = uuid.uuid4().hex
        config = await self._get_upload_info(filename, file_id)
        if not config: return False
        url = await self._upload_to_cos(config, data, filename)
        if not url: return False
        try:
            await self.ws.send(await self._build_file_msg(url, file_id, len(data), filename))
            logger.info(f"[Yuanbao] 文件已发送: {filename} ({len(data)} bytes)")
            return True
        except Exception as e:
            logger.warning(f"[Yuanbao] 发送文件失败: {e}"); return False

    async def send_file(self, file_path: str) -> bool:
        """发送本地文件到群聊"""
        if not os.path.exists(file_path): return False
        with open(file_path, "rb") as f: data = f.read()
        return await self.send_file_bytes(data, os.path.basename(file_path))

    async def send_and_wait(self, text: str, correlation_id: str,
                             at_user_id: str = "", at_nickname: str = "") -> str:
        """发送消息并等待回复（按 correlation_id 匹配）"""
        async with self._lock:
            if self._pending:
                raise Exception("已有待处理请求，请稍后再试")
            future = asyncio.get_running_loop().create_future()
            self._pending[correlation_id] = future
        try:
            await self.send_group_message(text, at_user_id=at_user_id, at_nickname=at_nickname)
            reply = await asyncio.wait_for(future, timeout=120)
            return reply
        except asyncio.TimeoutError:
            raise TimeoutError("等待元宝回复超时（120s）")
        finally:
            self._pending.pop(correlation_id, None)

    def deliver_reply(self, from_account: str, group_code: str, text: str, msg_id: str):
        """将收到的消息投递给等待中的请求（由 receive_loop 调用）"""
        if group_code == GROUP_CODE and from_account == YUANBAO_USER_ID:
            for fid, fut in list(self._pending.items()):
                if not fut.done():
                    logger.info(f"[Yuanbao] 投递回复 [{fid[:8]}]: {text[:60]}")
                    fut.set_result(text)
                    return

    async def receive_loop(self):
        """接收循环 - 处理所有 WS 推送消息"""
        import websockets
        try:
            async for raw in self.ws:
                try:
                    if FYBPAPI_DEBUG:
                        logger.info(f"[DEBUG] WS 原始 ({len(raw)} bytes): {raw.hex()}")
                    conn = self.codec.decode_conn_msg(raw); head = conn["head"]
                    cmd_type = head.get("cmdType")
                    if head.get("needAck"):
                        await self._send_ack(head)
                    if cmd_type == CMD_TYPE_PUSH:
                        inbound = self.codec.decode_inbound_push(conn.get("data", b""))
                        self.deliver_reply(inbound.get("from_account",""),
                                          inbound.get("group_code",""),
                                          inbound.get("text",""),
                                          inbound.get("msg_id",""))
                except Exception as e:
                    logger.error(f"[Yuanbao] 消息处理异常: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.warning("[Yuanbao] WebSocket 连接关闭")
        finally:
            self.connected = False
            for fid, fut in list(self._pending.items()):
                if not fut.done(): fut.set_exception(Exception("连接断开"))

    async def heartbeat_loop(self):
        while self.connected:
            await asyncio.sleep(10)
            try: await self._send_ping()
            except Exception as e:
                logger.error(f"[Yuanbao] 心跳失败: {e}"); break

    async def disconnect(self):
        self.connected = False
        if self.ws:
            await self.ws.close(); self.ws = None
        logger.info("[Yuanbao] 已断开连接")


# 全局元宝客户端（惰性初始化）
yuanbao_client: YuanbaoClient | None = None
_yuanbao_init_done = asyncio.Event()


async def ensure_yuanbao() -> YuanbaoClient:
    """确保元宝 WS 已连接"""
    global yuanbao_client  # noqa: used before definition
    if yuanbao_client and yuanbao_client.connected:
        return yuanbao_client
    client = YuanbaoClient()
    try:
        await client.connect()
        # 启动后台任务
        asyncio.create_task(client.receive_loop())
        asyncio.create_task(client.heartbeat_loop())
        # 等待元宝就绪
        try:
            rid = uuid.uuid4().hex
            reply = await asyncio.wait_for(
                client.send_and_wait(f"系统上线测试 [PID:{os.getpid()}]", rid,
                                     at_user_id=YUANBAO_USER_ID, at_nickname=YUANBAO_NICK),
                timeout=30)
            logger.info(f"[Yuanbao] 元宝就绪: {reply[:60]}")
        except Exception as e:
            logger.warning(f"[Yuanbao] 元宝未响应，继续: {e}")
        yuanbao_client = client
        _yuanbao_init_done.set()
        return client
    except Exception as e:
        logger.error(f"[Yuanbao] 连接失败: {e}")
        raise


# 模型别名
MODEL_ALIASES = {}
for idx, m in enumerate(RAW_MODELS):
    n = m["name"].lower().replace(" ", "-").replace("_", "-")
    MODEL_ALIASES[n] = idx; MODEL_ALIASES[m["name"]] = idx

ALIAS_MAP = {
    "yuanbao": -1, "元宝": -1, "元宝派": 0,
    "step": 1, "step-flash": 1, "step-3.5": 1, "step3.5-flash": 1,
    "qwen": 2, "qwen-plus": 2, "qwen3": 2, "qwen3.6": 2,
}
for alias, idx in ALIAS_MAP.items():
    if -1 <= idx < len(RAW_MODELS): MODEL_ALIASES[alias] = idx

# 模型列表（OpenAI 格式）
OPENAI_MODELS = []
for m in RAW_MODELS:
    mid = m["name"].lower().replace(" ", "-").replace("_", "-")
    OPENAI_MODELS.append({"id": mid, "object": "model", "created": int(time.time()), "owned_by": "fused"})
    OPENAI_MODELS.append({"id": m["name"], "object": "model", "created": int(time.time()), "owned_by": "fused"})
# 也注册 yuanbao
OPENAI_MODELS.append({"id": "yuanbao", "object": "model", "created": int(time.time()), "owned_by": "yuanbao"})


def get_model_idx(name: str) -> int:
    if name in MODEL_ALIASES: return MODEL_ALIASES[name]
    lower = name.lower()
    for n, idx in MODEL_ALIASES.items():
        if lower in n or n in lower: return idx
    return -1


# ─────────────────────────────────────────────────────────────
# 5. 数据库
# ─────────────────────────────────────────────────────────────
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_FILE, check_same_thread=False); g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None: db.close()

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False);
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS api_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, key_display TEXT NOT NULL,
        key_hash TEXT NOT NULL UNIQUE, model_source INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_used_at TIMESTAMP,
        status TEXT DEFAULT 'active' CHECK(status IN ('active','disabled')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS usage_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT NOT NULL, model_source TEXT NOT NULL,
        prompt_tokens INTEGER DEFAULT 0, completion_tokens INTEGER DEFAULT 0, total_tokens INTEGER DEFAULT 0,
        request_content TEXT, response_preview TEXT, latency_ms REAL DEFAULT 0,
        status TEXT DEFAULT 'success', error_message TEXT, api_key_index INTEGER DEFAULT 0,
        user_agent TEXT, ip_address TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT NOT NULL, model TEXT NOT NULL, request_count INTEGER DEFAULT 0,
        total_tokens INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, error_count INTEGER DEFAULT 0,
        PRIMARY KEY(date, model))""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_balance (
        id INTEGER PRIMARY KEY CHECK(id=1), balance REAL DEFAULT 0,
        total_recharged REAL DEFAULT 0, total_spent REAL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_created ON usage_logs(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_logs_model ON usage_logs(model)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_stats(date)")
    c.execute("INSERT OR IGNORE INTO user_balance(id,balance,total_recharged,total_spent) VALUES(1,2458.60,5000,2541.40)")
    conn.commit(); conn.close()


# ─────────────────────────────────────────────────────────────
# 6. API Key 轮询器
# ─────────────────────────────────────────────────────────────
class KeyRotator:
    def __init__(self):
        self._indices  = defaultdict(int)
        self._locks    = defaultdict(asyncio.Lock)
        self._status   = defaultdict(lambda: defaultdict(lambda: True))

    def get_next_key(self, model_idx: int):
        if model_idx < 0 or model_idx >= len(RAW_MODELS): return None, -1
        keys = RAW_MODELS[model_idx].get("api_keys", [])
        if not keys: return None, -1
        idx = self._indices[model_idx] % len(keys)
        self._indices[model_idx] = (idx + 1) % len(keys)
        if self._status[model_idx][idx]: return keys[idx], idx
        for i in range(len(keys)):
            j = (idx + i) % len(keys)
            if self._status[model_idx][j]: return keys[j], j
        self._indices[model_idx] = 1; return keys[0], 0

    def disable_key(self, mi, ki): self._status[mi][ki] = False
    def enable_key(self, mi, ki):  self._status[mi][ki] = True

key_rotator = KeyRotator()


# ─────────────────────────────────────────────────────────────
# 7. Quart 应用
# ─────────────────────────────────────────────────────────────
app = Quart(__name__, template_folder=BASE_DIR)
app.secret_key = secrets.token_hex(32)
app.config["JSON_AS_ASCII"] = False
app.teardown_appcontext(close_db)


def format_tokens(n: int) -> str:
    if n < 1000: return str(n)
    if n < 1_000_000: return f"{n/1000:.1f}K"
    return f"{n/1_000_000:.1f}M"

app.jinja_env.filters["format_tokens"] = format_tokens

# ─────────────────────────────────────────────────────────────
# 8. 鉴权装饰器
# ─────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    async def decorated(*args, **kwargs):
        # 允许未登录访问的路径
        public_paths = ['/login', '/v1/', '/api/send', '/api/status', '/api/config', '/api/models', '/api/logs']
        if any(request.path.startswith(p) for p in public_paths):
            return await f(*args, **kwargs)
        if session.get("admin_logged_in"):
            return await f(*args, **kwargs)
        if request.is_json or request.headers.get("Accept") == "application/json":
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("admin_page"))
    return decorated


def api_key_or_no_auth(f):
    """API Key 鉴权（Bearer Token）— 支持多个默认keys"""
    @wraps(f)
    async def decorated(*args, **kwargs):
        # 如果未配置 FYBPAPI_KEYS 且未注册模型，直接放行
        if not FYBPAPI_KEYS and not RAW_MODELS:
            return await f(*args, **kwargs)
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            # 1. 验证默认 keys（支持多key）
            if FYBPAPI_KEYS and token in FYBPAPI_KEYS:
                return await f(*args, **kwargs)
            # 2. 验证 apikey.md 中注册过的 key（按 key_hash 查）
            db = get_db()
            row = db.execute(
                "SELECT * FROM api_keys WHERE key_hash=? AND status='active'",
                (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
            if row:
                db.execute("UPDATE api_keys SET last_used_at=CURRENT_TIMESTAMP WHERE id=?", (row["id"],))
                db.commit()
                return await f(*args, **kwargs)
        # 没有配置默认keys时也放行（兼容旧行为）
        if not FYBPAPI_KEYS:
            return await f(*args, **kwargs)
        return jsonify({"error": {"message": "Invalid API key", "type": "authentication_error"}}), 401
    return decorated


# ─────────────────────────────────────────────────────────────
# 9. 核心代理函数
# ─────────────────────────────────────────────────────────────
async def proxy_to_upstream(model_idx: int, messages: list, api_key: str,
                            stream: bool = False, tools: list = None) -> dict:
    """通过 HTTP 代理到上游 API（非 yuanbao 模型）"""
    import requests
    model = RAW_MODELS[model_idx]
    base_url = model["base_url"].rstrip("/")

    # 构建请求（model 用 apikey.md 中定义的 name）
    upstream_model = model["name"]
    payload = {"model": upstream_model, "messages": messages}
    if stream: payload["stream"] = True
    if tools: payload["tools"] = tools; payload["tool_choice"] = "auto"

    start = time.time()
    try:
        resp = requests.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload, timeout=120, stream=stream
        )
        latency = (time.time() - start) * 1000

        if stream:
            return {"streamed": True, "response": resp}
        else:
            # 处理 SSE / 流式响应（部分上游即使 stream=False 也返回 text/event-stream）
            ct = resp.headers.get("content-type", "")
            raw_body = resp.content

            if "text/event-stream" in ct or not raw_body:
                # SSE 模式：提取 data: {...} 行
                content_text = ""
                for line in raw_body.decode("utf-8", errors="replace").splitlines():
                    if line.startswith("data:"):
                        part = line[5:].strip()
                        if part and part != "[DONE]":
                            content_text += part + "\n"
                if content_text.strip():
                    try: data = json.loads(content_text.strip())
                    except: data = {"choices": [{"message": {"content": content_text.strip()}}]}
                else:
                    raise Exception(f"上游返回空 SSE: {resp.status_code}")
            else:
                try: data = resp.json()
                except Exception as je:
                    raise Exception(f"上游响应 JSON 解析失败: {je} | body={raw_body[:200]}")
            # 记录日志
            try:
                db = get_db()
                today = datetime.now().strftime("%Y-%m-%d")
                usage = data.get("usage", {})
                db.execute("""INSERT INTO usage_logs
                    (model,model_source,prompt_tokens,completion_tokens,total_tokens,
                     request_content,response_preview,latency_ms,status)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (data.get("model",""), model["name"],
                     usage.get("prompt_tokens",0), usage.get("completion_tokens",0), usage.get("total_tokens",0),
                     json.dumps(messages, ensure_ascii=False)[:500],
                     (data.get("choices",[{}])[0].get("message",{}).get("content","") or "")[:200],
                     latency, "success"))
                db.execute("""INSERT INTO daily_stats(date,model,request_count,total_tokens,success_count,error_count)
                    VALUES(?,?,1,?,1,0) ON CONFLICT(date,model) DO UPDATE SET
                    request_count=request_count+1, total_tokens=total_tokens+?,
                    success_count=success_count+1""",
                    (today, data.get("model",""), usage.get("total_tokens",0), usage.get("total_tokens",0)))
                db.commit()
            except Exception as e:
                logger.warning(f"[Stats] 记录失败: {e}")
            return {"streamed": False, "data": data, "latency_ms": latency}
    except Exception as e:
        latency = (time.time() - start) * 1000
        logger.error(f"[Proxy] 请求失败: {e}")
        # 禁用失败的 key
        key_idx = -1
        for i, k in enumerate(model.get("api_keys", [])):
            if k == api_key: key_idx = i; break
        if key_idx >= 0: key_rotator.disable_key(model_idx, key_idx)
        raise Exception(str(e))


async def chat_with_yuanbao(messages: list, tools: list = None) -> dict:
    """通过元宝 WS 发送聊天请求，等待回复"""
    client = await ensure_yuanbao()
    last_msg = messages[-1] if messages else {}
    last_role = last_msg.get("role", "user")
    last_content = ProtobufCodec.extract_content_text(last_msg.get("content", ""))

    # 生成历史文件
    history_lines = []
    for msg in messages[:-1]:
        role = msg.get("role", "unknown")
        content = ProtobufCodec.extract_content_text(msg.get("content", ""))
        if content: history_lines.append(f"{role}: {content}")
    history_content = "\n".join(history_lines)

    # 生成工具文件
    tools_content = ""
    if tools:
        lines = ["【工具调用请求】", "",
                 "请根据用户问题从可用工具中选择一个，并严格按以下 JSON 格式回复（只返回 JSON，不要包含其他任何内容）：", ""]
        lines.append("可用工具定义：\n" + json.dumps(tools, ensure_ascii=False, indent=2))
        lines.append("")
        lines.append('请仅返回 JSON 格式：{"tool_calls": [{"id": "call_xxx", "type": "function", "function": {"name": "工具名", "arguments": {"参数名": "参数值"}}}]}')
        tools_content = "\n".join(lines)

    # 发送文件
    tmp_dir = os.path.join(BASE_DIR, "tmp"); os.makedirs(tmp_dir, exist_ok=True)
    history_path = os.path.join(tmp_dir, "历史.txt")
    tools_path = os.path.join(tmp_dir, "工具.txt")

    try:
        if history_content:
            with open(history_path, "w", encoding="utf-8") as f: f.write(history_content)
            if await client.send_file(history_path):
                os.remove(history_path); logger.info(f"[Yuanbao] 已发送: 历史.txt")
            await asyncio.sleep(0.5)

        if tools_content:
            with open(tools_path, "w", encoding="utf-8") as f: f.write(tools_content)
            if await client.send_file(tools_path):
                os.remove(tools_path); logger.info(f"[Yuanbao] 已发送: 工具.txt")
            await asyncio.sleep(0.5)

        # 构造 @元宝 消息
        prefix = f"System:请读取历史.txt和工具.txt（有哪个读哪个），直接回答用户问题，无需告知我已读取\n"
        if last_role == "tool":
            user_msg = prefix + f"Tool:{last_content}"
        else:
            user_msg = prefix + f"User:{last_content}"

        # 发送并等待
        correlation_id = uuid.uuid4().hex
        reply = await client.send_and_wait(user_msg, correlation_id,
                                           at_user_id=YUANBAO_USER_ID, at_nickname=YUANBAO_NICK)

        # 解析工具调用
        tool_calls = None
        if tools:
            try:
                text = reply.strip()
                if text.startswith("```"):
                    parts = text.split("```")
                    for p in parts:
                        p = p.strip()
                        if p and not p.startswith("json"):
                            try: text = json.loads(p); break
                            except: pass
                if isinstance(text, str):
                    try: text = json.loads(text)
                    except: pass
                if isinstance(text, dict) and "tool_calls" in text:
                    raw = text["tool_calls"]
                    if isinstance(raw, list) and raw:
                        valid = []
                        for tc in raw:
                            if isinstance(tc, dict) and tc.get("type") == "function":
                                func = tc.get("function", {})
                                args = func.get("arguments", {})
                                args_str = json.dumps(args, ensure_ascii=False) if isinstance(args, dict) else str(args)
                                valid.append({
                                    "id": tc.get("id", f"call_{uuid.uuid4().hex[:8]}"),
                                    "type": "function",
                                    "function": {"name": func.get("name",""), "arguments": args_str}
                                })
                        if valid: tool_calls = valid
            except Exception as e:
                logger.warning(f"[Yuanbao] 工具解析失败: {e}")

        content = None if tool_calls else reply
        resp_body = {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "yuanbao",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content, "tool_calls": tool_calls},
                "finish_reason": "tool_calls" if tool_calls else "stop"
            }],
            "usage": {"prompt_tokens": len(user_msg), "completion_tokens": len(reply),
                      "total_tokens": len(user_msg) + len(reply)}
        }
        logger.info(f"[Yuanbao] 回复: {reply[:60]}")
        return resp_body

    finally:
        # 清理临时文件
        for p in (history_path, tools_path):
            if os.path.exists(p):
                try: os.remove(p)
                except: pass


# ─────────────────────────────────────────────────────────────
# 10. API 路由：OpenAI 兼容端点
# ─────────────────────────────────────────────────────────────
@app.route("/v1/models")
async def api_models():
    return jsonify({"object": "list", "data": OPENAI_MODELS})


@app.route("/v1/chat/completions", methods=["POST"])
@api_key_or_no_auth
async def api_chat_completions():
    body = await request.get_json()
    model_name = body.get("model", "")
    messages   = body.get("messages", [])
    stream     = body.get("stream", False)
    tools      = body.get("tools")

    if not messages:
        return jsonify({"error": {"message": "No messages", "type": "invalid_request_error"}}), 400

    model_idx = get_model_idx(model_name)

    # ── 元宝（特殊：通过 WS 发送）──
    if model_idx == -1 or model_name.lower() in ("yuanbao", "元宝"):
        try:
            result = await chat_with_yuanbao(messages, tools)
            return jsonify(result)
        except TimeoutError as e:
            return jsonify({"error": {"message": str(e), "type": "timeout_error"}}), 504
        except Exception as e:
            logger.error(f"[Yuanbao] 请求失败: {e}")
            return jsonify({"error": {"message": str(e), "type": "internal_error"}}), 500

    # ── HTTP 上游代理 ──
    if model_idx < 0 or model_idx >= len(RAW_MODELS):
        return jsonify({"error": {"message": f"Unknown model: {model_name}"}}), 400

    api_key, key_idx = key_rotator.get_next_key(model_idx)
    if not api_key:
        return jsonify({"error": {"message": "No available API key for this model"}}), 503

    try:
        result = await proxy_to_upstream(model_idx, messages, api_key, stream, tools)
        if result["streamed"]:
            # 流式响应透传
            import requests
            async def stream_response():
                try:
                    for chunk in result["response"].iter_content(chunk_size=8192):
                        if chunk: yield chunk
                finally:
                    result["response"].close()
            return Response(stream_response(),
                           mimetype="text/event-stream",
                           headers={"Cache-Control": "no-cache"})
        else:
            return jsonify(result["data"])
    except Exception as e:
        return jsonify({"error": {"message": str(e), "type": "upstream_error"}}), 502


# ─────────────────────────────────────────────────────────────
# 11. 文件/媒体发送 API（FYBPAPI 增强）
# ─────────────────────────────────────────────────────────────
@app.route("/api/send/text", methods=["POST"])
@api_key_or_no_auth
async def api_send_text():
    """发送纯文本消息到元宝群"""
    body = await request.get_json()
    text     = body.get("text", "")
    at_user  = body.get("at_user_id", YUANBAO_USER_ID)
    at_nick  = body.get("at_nickname", YUANBAO_NICK)
    if not text:
        return jsonify({"error": "text is required"}), 400
    try:
        client = await ensure_yuanbao()
        ok = await client.send_group_message(text, at_user_id=at_user, at_nickname=at_nick)
        if ok:
            return jsonify({"ok": True, "message": "Text sent"})
        return jsonify({"error": "Send failed, not connected"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send/file", methods=["POST"])
@api_key_or_no_auth
async def api_send_file():
    """发送本地文件（multipart）"""
    try:
        if "file" not in (await request.files):
            return jsonify({"error": "No file in request"}), 400
        file = (await request.files)["file"]
        filename = file.filename or "file.bin"
        data = await file.read()
        client = await ensure_yuanbao()
        ok = await client.send_file_bytes(data, filename)
        if ok:
            return jsonify({"ok": True, "filename": filename, "size": len(data)})
        return jsonify({"error": "Upload or send failed"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send/image", methods=["POST"])
@api_key_or_no_auth
async def api_send_image():
    """发送图片 URL"""
    body = await request.get_json()
    image_url = body.get("url") or body.get("image_url")
    if not image_url:
        return jsonify({"error": "url is required"}), 400
    # 图片走文本消息发送（带上 URL）
    try:
        client = await ensure_yuanbao()
        ok = await client.send_group_message(f"[图片] {image_url}")
        if ok:
            return jsonify({"ok": True, "url": image_url})
        return jsonify({"error": "Send failed"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send/batch", methods=["POST"])
@api_key_or_no_auth
async def api_send_batch():
    """批量发送消息"""
    body = await request.get_json()
    items = body.get("items", [])
    results = []
    try:
        client = await ensure_yuanbao()
        for item in items:
            text = item.get("text", "")
            if text:
                ok = await client.send_group_message(text)
                results.append({"text": text[:30], "ok": ok})
                await asyncio.sleep(0.3)
        return jsonify({"ok": True, "results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
async def api_status():
    """融合系统状态"""
    global yuanbao_client
    status = {
        "yuanbao": {
            "connected": yuanbao_client.connected if yuanbao_client else False,
            "connect_id": yuanbao_client.connect_id if yuanbao_client else None,
            "pending_requests": len(yuanbao_client._pending) if yuanbao_client else 0,
        },
        "models": {
            "yuanbao": True,
            "upstream": [m["name"] for m in RAW_MODELS],
            "total_keys": sum(len(m.get("api_keys",[])) for m in RAW_MODELS),
        },
        "uptime": time.time(),
    }
    return jsonify(status)


# ─────────────────────────────────────────────────────────────
# 12. 管理后台路由（移植自 app.py，改为 async）
# ─────────────────────────────────────────────────────────────
@app.route("/admin", methods=["GET", "POST"])
async def admin_page():
    if request.method == "POST":
        form = await request.form
        pwd = form.get("password", "")
        if pwd == ADMIN_PASSWORD:
            session["admin_logged_in"] = True
            session["login_time"] = time.time()
            return jsonify({"success": True, "redirect": "/"})
        return jsonify({"success": False, "error": "密码错误"}), 400
    tmpl = await render_template("index.html", show_login=True)
    return QuartResponse(tmpl)


@app.route("/logout")
async def logout():
    session.clear()
    return redirect(url_for("admin_page"))


@app.route("/")
def root():
    """根路径重定向到开放平台"""
    return redirect("/portal")


@app.route("/admin")
@login_required
async def admin_index():
    db = get_db()
    now = datetime.now()
    month_start = now.replace(day=1).strftime("%Y-%m-%d")
    today_str = now.strftime("%Y-%m-%d")
    month_stats = db.execute("""SELECT COALESCE(SUM(request_count),0) as cnt,
        COALESCE(SUM(total_tokens),0) as tokens, COALESCE(SUM(success_count),0) as sc,
        COALESCE(SUM(error_count),0) as ec FROM daily_stats WHERE date>=?""", (month_start,)).fetchone()
    today_stats = db.execute("""SELECT COALESCE(SUM(request_count),0) as cnt,
        COALESCE(SUM(total_tokens),0) as tokens FROM daily_stats WHERE date=?""", (today_str,)).fetchone()
    week_rows = db.execute("""SELECT date, SUM(request_count) as cnt,
        SUM(total_tokens) as tokens FROM daily_stats
        WHERE date >= date('now','-7 days') GROUP BY date ORDER BY date""").fetchall()
    recent_logs = db.execute("""SELECT * FROM usage_logs
        ORDER BY created_at DESC LIMIT 5""").fetchall()
    # 余额统计
    month_calls = (month_stats["sc"] or 0) + (month_stats["ec"] or 0)
    balance_spent = (month_stats["sc"] or 0) * 0.001
    context = {
        "balance": balance, "month_calls": month_calls, "month_tokens": month_stats["tokens"] or 0,
        "today_calls": today_stats["cnt"] or 0, "today_tokens": today_stats["tokens"] or 0,
        "week_data": [(r["date"], r["cnt"] or 0, r["tokens"] or 0) for r in week_rows],
        "recent_logs": [dict(r) for r in recent_logs],
        "yuanbao_status": yuanbao_client.connected if yuanbao_client else False,
        "yuanbao_connect_id": yuanbao_client.connect_id if yuanbao_client else None,
    }
    tmpl = await render_template("index.html", **context)
    return QuartResponse(tmpl)


@app.route("/api/logs")
@login_required
async def api_logs():
    db = get_db()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    model_filter = request.args.get("model", "")
    offset = (page - 1) * per_page
    where = "WHERE model LIKE ?" if model_filter else ""
    params = [f"%{model_filter}%"] if model_filter else []
    total = db.execute(f"SELECT COUNT(*) FROM usage_logs {where}", params).fetchone()[0]
    logs = db.execute(f"SELECT * FROM usage_logs {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                      params + [per_page, offset]).fetchall()
    return jsonify({"total": total, "logs": [dict(r) for r in logs]})


@app.route("/api/models")
@login_required
async def api_models_info():
    result = []
    for i, m in enumerate(RAW_MODELS):
        keys = m.get("api_keys", [])
        active = sum(1 for j in range(len(keys)) if key_rotator._status[i][j])
        result.append({"index": i, "name": m["name"], "base_url": m["base_url"],
                       "key_count": len(keys), "active_keys": active,
                       "keys": [{"idx": j, "preview": k[:12]+"...", "active": key_rotator._status[i][j]}
                                for j, k in enumerate(keys)]})
    return jsonify(result)


@app.route("/api/models/<int:mi>/keys/<int:ki>/toggle", methods=["POST"])
@login_required
async def api_toggle_key(mi, ki):
    if mi >= len(RAW_MODELS): return jsonify({"error": "Invalid model index"}), 400
    keys = RAW_MODELS[mi].get("api_keys", [])
    if ki >= len(keys): return jsonify({"error": "Invalid key index"}), 400
    current = key_rotator._status[mi][ki]
    if current: key_rotator.disable_key(mi, ki)
    else: key_rotator.enable_key(mi, ki)
    return jsonify({"active": not current})


@app.route("/api/recharge", methods=["POST"])
@login_required
async def api_recharge():
    data = await request.get_json()
    amount = float(data.get("amount", 0))
    if amount <= 0: return jsonify({"error": "Invalid amount"}), 400
    db = get_db()
    db.execute("UPDATE user_balance SET balance=balance+?, total_recharged=total_recharged+?, updated_at=CURRENT_TIMESTAMP WHERE id=1",
               (amount, amount))
    db.commit()
    balance = db.execute("SELECT balance FROM user_balance WHERE id=1").fetchone()
    return jsonify({"balance": balance["balance"], "recharged": amount})


# ─────────────────────────────────────────────────────────────
# 12.5 配置管理 API
# ─────────────────────────────────────────────────────────────
@app.route("/api/config", methods=["GET"])
@login_required
async def api_get_config():
    """获取当前配置"""
    return jsonify({
        "app_id": APP_ID,
        "app_secret": APP_SECRET,
        "group_code": GROUP_CODE,
        "yuanbao_user_id": YUANBAO_USER_ID,
        "yuanbao_nick": YUANBAO_NICK,
        "port": FYBPAPI_PORT,
        "debug": FYBPAPI_DEBUG,
        "admin_password": ADMIN_PASSWORD,
        "models": RAW_MODELS,
        "default_keys": FYBPAPI_KEYS
    })


@app.route("/api/config/save", methods=["POST"])
@login_required
async def api_save_config():
    """保存配置到config.json和apikey.json"""
    global FYBPAPI_CONFIG, APP_ID, APP_SECRET, GROUP_CODE, YUANBAO_USER_ID
    global YUANBAO_NICK, FYBPAPI_PORT, FYBPAPI_DEBUG, ADMIN_PASSWORD
    global RAW_MODELS, FYBPAPI_KEYS, key_rotator
    
    try:
        data = await request.get_json()
    except Exception as e:
        logger.error(f"JSON解析失败: {e}")
        return jsonify({"error": f"JSON解析失败: {str(e)}"}), 400
    
    # 检查是否提交了模型配置（空对象{}表示不更新模型）
    submitted_models = data.get("models")
    
    # 更新 config.json
    FYBPAPI_CONFIG["APP_ID"] = data.get("app_id", APP_ID)
    FYBPAPI_CONFIG["APP_SECRET"] = data.get("app_secret", APP_SECRET)
    FYBPAPI_CONFIG["GROUP_CODE"] = data.get("group_code", GROUP_CODE)
    FYBPAPI_CONFIG["YUANBAO_USER_ID"] = data.get("yuanbao_user_id", YUANBAO_USER_ID)
    FYBPAPI_CONFIG["YUANBAO_NICK"] = data.get("yuanbao_nick", YUANBAO_NICK)
    FYBPAPI_CONFIG["PORT"] = int(data.get("port", FYBPAPI_PORT))
    FYBPAPI_CONFIG["debug"] = data.get("debug", FYBPAPI_DEBUG)
    FYBPAPI_CONFIG["admin_password"] = data.get("admin_password", ADMIN_PASSWORD)
    # 只有在提供了模型配置时才更新（保留原有模型）
    if submitted_models is not None:
        FYBPAPI_CONFIG["models"] = submitted_models
    
    # 保存 config.json
    try:
        with open(os.path.join(BASE_DIR, "config.json"), "w", encoding="utf-8") as f:
            json.dump(FYBPAPI_CONFIG, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"保存config.json失败: {e}")
        return jsonify({"error": f"保存config.json失败: {str(e)}"}), 500
    
    # 更新 apikey.json (默认keys)
    default_keys = data.get("default_keys", FYBPAPI_KEYS)
    try:
        apikey_data = {"default": default_keys if isinstance(default_keys, list) else [default_keys]}
        with open(os.path.join(BASE_DIR, "apikey.json"), "w", encoding="utf-8") as f:
            json.dump(apikey_data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"保存apikey.json失败: {e}")
        return jsonify({"error": f"保存apikey.json失败: {str(e)}"}), 500
    
    # 更新全局变量
    APP_ID = FYBPAPI_CONFIG["APP_ID"]
    APP_SECRET = FYBPAPI_CONFIG["APP_SECRET"]
    GROUP_CODE = FYBPAPI_CONFIG["GROUP_CODE"]
    YUANBAO_USER_ID = FYBPAPI_CONFIG["YUANBAO_USER_ID"]
    YUANBAO_NICK = FYBPAPI_CONFIG["YUANBAO_NICK"]
    FYBPAPI_PORT = FYBPAPI_CONFIG["PORT"]
    FYBPAPI_DEBUG = FYBPAPI_CONFIG["debug"]
    ADMIN_PASSWORD = FYBPAPI_CONFIG["admin_password"]
    RAW_MODELS = parse_models_from_config(FYBPAPI_CONFIG)
    FYBPAPI_KEYS = default_keys if isinstance(default_keys, list) else [default_keys]
    
    # 重新初始化key_rotator
    key_rotator = KeyRotator()
    
    logger.info("✅ 配置已保存并生效")
    return jsonify({"success": True, "message": "配置已保存"})


# ─────────────────────────────────────────────────────────────
# 13. 启动入口
# ─────────────────────────────────────────────────────────────
async def startup():
    """启动时初始化元宝 WS 连接（后台异步）"""
    logger.info("=" * 55)
    logger.info("FreeYuanBaoProxyAPI 启动中...")
    logger.info(f"  上游模型: {len(RAW_MODELS)} 个，合计 {sum(len(m.get('api_keys',[])) for m in RAW_MODELS)} 个 Key")
    for i, m in enumerate(RAW_MODELS):
        logger.info(f"  [{i}] {m['name']}: {len(m.get('api_keys',[]))} keys -> {m['base_url']}")
    logger.info(f"  元宝:     群 {GROUP_CODE} | user_id {YUANBAO_USER_ID[:20]}...")
    logger.info(f"  HTTP 端口: {PROXY_PORT} (管理后台 + /v1/*)")
    logger.info(f"  FYBPAPI 端口: {FYBPAPI_PORT} (元宝专属 API)")
    logger.info("=" * 55)
    # 后台启动元宝连接（不阻塞启动）
    asyncio.create_task(_background_yuanbao())

# ─────────────────────────────────────────────────────────────
# 开放平台门户
# ─────────────────────────────────────────────────────────────
@app.route("/portal")
async def portal_page():
    """开放平台门户"""
    try:
        tmpl = await render_template("portal.html")
        return QuartResponse(tmpl)
    except Exception as e:
        logger.error(f"渲染portal.html失败: {e}")
        return f"Error: {e}", 500


async def _background_yuanbao():
    """后台异步连接元宝（失败不影响 HTTP 服务）"""
    try:
        await ensure_yuanbao()
    except Exception as e:
        logger.warning(f"[Yuanbao] 后台连接失败（HTTP 服务继续）：{e}")


def main():
    os.makedirs(os.path.join(BASE_DIR, "tmp"), exist_ok=True)
    init_db()

    # 启动时立即初始化元宝（同步签票 + 预热）
    # 注意：这里不等待连接完成，由 startup() 后台处理
    try:
        import requests
        if APP_ID and APP_SECRET:
            # 预热：签票确认凭证有效
            url = f"https://{API_DOMAIN}/api/v5/robotLogic/sign-token"
            nonce = "".join(random.choices(string.hexdigits.lower(), k=32))
            ts = datetime.fromtimestamp(time.time() + 8*3600,
                                       tz=__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+08:00")
            sig = hmac.new(APP_SECRET.encode(),
                           f"{nonce}{ts}{APP_ID}{APP_SECRET}".encode(), hashlib.sha256).hexdigest()
            r = requests.post(url, headers={"Content-Type": "application/json", "X-AppVersion": "1.0.11",
                                            "X-OperationSystem": "linux", "X-Instance-Id": "1", "X-Bot-Version": "2026.3.22"},
                             json={"app_key": APP_ID, "nonce": nonce, "signature": sig, "timestamp": ts}, timeout=15)
            if r.json().get("code") == 0:
                logger.info(f"[Startup] 元宝凭证有效: bot_id={r.json()['data']['bot_id']}")
            else:
                logger.warning(f"[Startup] 元宝凭证无效: {r.json()}")
    except Exception as e:
        logger.warning(f"[Startup] 元宝凭证检查失败: {e}")

    # 注册 startup 钩子
    @app.before_serving
    async def before():
        await startup()

    logger.info(f"HTTP 服务器启动: http://0.0.0.0:{PROXY_PORT}")
    app.run(host="0.0.0.0", port=PROXY_PORT, debug=False, loop=None)


if __name__ == "__main__":
    main()
