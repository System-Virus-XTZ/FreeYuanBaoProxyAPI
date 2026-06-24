# FreeYuanBaoProxyAPI

> 一个基于元宝的 API 代理平台，支持多种大模型，提供 OpenAI 兼容接口。

**版本: 3.0.1** - 新增用户开放平台和管理界面 v3.0.1

一个基于元宝的 API 代理平台，支持多种大模型，提供 OpenAI 兼容接口。

## 功能特性

- 🤖 **多模型支持** - 元宝、Qwen 等多种模型
- 🔄 **Key 轮询** - 支持多个 API Key 自动轮询
- 📊 **用量统计** - 详细的 API 调用统计
- 🔐 **安全认证** - API Key 验证
- 🌐 **OpenAI 兼容** - 支持 `/v1/chat/completions` 等标准接口

## 快速开始

### 安装依赖

```bash
pip install quart aiofiles requests
```

### 配置

编辑 `config.json`:

```json
{
    "APP_ID": "your_app_id",
    "APP_SECRET": "your_app_secret",
    "GROUP_CODE": "your_group_code",
    "PORT": 8000,
    "admin_password": "your_admin_password"
}
```

编辑 `apikey.json`:

```json
{
    "default": [
        "your_api_key_1",
        "your_api_key_2"
    ]
}
```

### 运行

```bash
python app.py
```

### 访问

- 开放平台: http://localhost:8000/portal
- 管理后台: http://localhost:8000/admin
- API 接口: http://localhost:8000/v1/chat/completions

## API 使用

### 发送消息

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "model": "yuanbao",
    "messages": [
      {"role": "user", "content": "你好"}
    ]
  }'
```

### 获取模型列表

```bash
curl http://localhost:8000/v1/models \
  -H "Authorization: Bearer YOUR_API_KEY"
```

## 管理后台

访问 `/admin` 使用管理员密码登录，可管理：

- API Keys 配置
- 模型管理
- 用量统计
- 系统设置

## 项目结构

```
FYBPAPI/
├── app.py          # 主程序
├── index.html      # 管理后台页面
├── portal.html     # 开放平台页面
├── config.json     # 配置文件（不提交）
├── apikey.json     # API Keys（不提交）
├── tmp/            # 日志和数据库目录
│   ├── fused.log
│   └── relay.db
└── .gitignore
```

## License

MIT License
