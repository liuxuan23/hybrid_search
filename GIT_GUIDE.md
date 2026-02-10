# Git 使用指南

本项目已初始化 Git 仓库。以下是常用的 Git 操作指南。

## 当前状态

✅ Git 仓库已初始化
✅ 初始提交已完成
✅ .gitignore 已配置（忽略大文件、虚拟环境等）

## 常用 Git 命令

### 查看状态
```bash
git status                    # 查看工作区状态
git log --oneline            # 查看提交历史
git log --graph --oneline    # 图形化查看提交历史
```

### 添加和提交
```bash
# 添加所有更改
git add .

# 添加特定文件
git add scripts/new_file.py

# 提交更改
git commit -m "描述你的更改"

# 查看即将提交的更改
git diff --staged
```

### 推送到远程仓库

#### 1. 在 GitHub/GitLab/Gitee 创建远程仓库

首先在代码托管平台（GitHub、GitLab、Gitee 等）创建一个新仓库，获取仓库地址。

#### 2. 添加远程仓库

```bash
# GitHub 示例
git remote add origin https://github.com/yourusername/hybrid-search.git

# 或使用 SSH（推荐）
git remote add origin git@github.com:yourusername/hybrid-search.git

# 查看远程仓库
git remote -v
```

#### 3. 推送到远程仓库

```bash
# 首次推送（设置上游分支）
git push -u origin master

# 或如果远程仓库使用 main 分支
git branch -M main
git push -u origin main

# 后续推送
git push
```

### 分支管理

```bash
# 创建新分支
git checkout -b feature/new-feature

# 切换分支
git checkout main

# 查看所有分支
git branch -a

# 合并分支
git checkout main
git merge feature/new-feature
```

### 忽略的文件

根据 `.gitignore` 配置，以下内容不会被提交：
- Python 缓存文件 (`__pycache__/`, `*.pyc`)
- 虚拟环境 (`.venv/`, `venv/`)
- 数据文件 (`data/huggingkg_tiny/*.tsv`, `*.txt`, `*.zip`)
- 数据库存储 (`storage/`, `*.lance/`)
- IDE 配置文件 (`.vscode/`, `.idea/`)

## 推荐的提交信息格式

```
类型: 简短描述

详细说明（可选）

示例：
feat: 添加新的性能测试脚本
fix: 修复路径导入问题
docs: 更新 README 文档
refactor: 重构项目结构
```

## 工作流程建议

1. **开发前**：创建功能分支
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **开发中**：定期提交
   ```bash
   git add .
   git commit -m "描述更改"
   ```

3. **完成后**：推送到远程
   ```bash
   git push origin feature/your-feature-name
   ```

4. **合并**：在代码托管平台创建 Pull Request/Merge Request

## 注意事项

⚠️ **不要提交大文件**
- 数据文件（TSV、TXT、ZIP）已配置为忽略
- 数据库文件（storage/）已配置为忽略
- 如果误提交了大文件，使用 `git rm --cached` 移除

⚠️ **保护敏感信息**
- 不要提交密码、API 密钥等敏感信息
- 使用环境变量或配置文件（添加到 .gitignore）

## 快速开始

```bash
# 1. 查看当前状态
git status

# 2. 添加更改
git add .

# 3. 提交更改
git commit -m "你的提交信息"

# 4. 推送到远程（首次需要添加远程仓库）
git push
```

## 需要帮助？

- Git 官方文档：https://git-scm.com/doc
- GitHub 指南：https://guides.github.com/
- GitLab 指南：https://docs.gitlab.com/ee/gitlab-basics/
