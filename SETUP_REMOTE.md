# 配置远程仓库推送指南

## 问题：Permission denied (publickey)

这是因为 GitHub 需要 SSH 密钥认证。有两种解决方案：

---

## 方案一：使用 SSH 密钥（推荐）

### 步骤 1：生成 SSH 密钥

```bash
# 生成新的 SSH 密钥（替换为你的 GitHub 邮箱）
ssh-keygen -t ed25519 -C "your_email@example.com"

# 按 Enter 使用默认文件位置 (~/.ssh/id_ed25519)
# 设置密码（可选，但推荐）
```

### 步骤 2：启动 SSH 代理并添加密钥

```bash
# 启动 ssh-agent
eval "$(ssh-agent -s)"

# 添加 SSH 私钥到 ssh-agent
ssh-add ~/.ssh/id_ed25519
```

### 步骤 3：复制公钥

```bash
# 显示公钥内容
cat ~/.ssh/id_ed25519.pub

# 复制输出的内容（以 ssh-ed25519 开头）
```

### 步骤 4：添加到 GitHub

1. 登录 GitHub
2. 点击右上角头像 → **Settings**
3. 左侧菜单选择 **SSH and GPG keys**
4. 点击 **New SSH key**
5. **Title**: 填写描述（如 "My Laptop"）
6. **Key**: 粘贴刚才复制的公钥内容
7. 点击 **Add SSH key**

### 步骤 5：测试连接

```bash
ssh -T git@github.com
# 应该看到: Hi username! You've successfully authenticated...
```

### 步骤 6：推送代码

```bash
cd /home/liuxuan/workplace/hybrid_search
git push -u origin master
```

---

## 方案二：使用 HTTPS + Personal Access Token（更简单）

### 步骤 1：切换到 HTTPS 地址

```bash
cd /home/liuxuan/workplace/hybrid_search

# 查看当前远程地址
git remote -v

# 如果使用 SSH，切换到 HTTPS
git remote set-url origin https://github.com/yourusername/hybrid-search.git
```

### 步骤 2：创建 Personal Access Token

1. 登录 GitHub
2. 点击右上角头像 → **Settings**
3. 左侧菜单选择 **Developer settings**
4. 选择 **Personal access tokens** → **Tokens (classic)**
5. 点击 **Generate new token** → **Generate new token (classic)**
6. **Note**: 填写描述（如 "hybrid-search repo"）
7. **Expiration**: 选择过期时间（建议 90 天或 No expiration）
8. **Select scopes**: 勾选 `repo`（完整仓库访问权限）
9. 点击 **Generate token**
10. **重要**：复制生成的 token（只显示一次！）

### 步骤 3：使用 Token 推送

```bash
cd /home/liuxuan/workplace/hybrid_search

# 推送时，用户名使用你的 GitHub 用户名
# 密码使用刚才生成的 Personal Access Token
git push -u origin master

# 提示输入用户名时：输入你的 GitHub 用户名
# 提示输入密码时：粘贴 Personal Access Token（不是 GitHub 密码）
```

### 步骤 4：保存凭据（可选）

```bash
# 使用 Git Credential Helper 保存凭据
git config --global credential.helper store

# 下次推送时会自动使用保存的凭据
```

---

## 方案三：使用 GitHub CLI（最简单）

如果安装了 GitHub CLI：

```bash
# 安装 GitHub CLI（如果未安装）
# Ubuntu/Debian:
sudo apt install gh

# 登录 GitHub
gh auth login

# 选择 GitHub.com
# 选择 HTTPS
# 选择浏览器登录或输入 token

# 然后直接推送
git push -u origin master
```

---

## 推荐方案

- **方案一（SSH）**：适合长期使用，一次配置永久使用
- **方案二（HTTPS + Token）**：适合快速开始，但 token 可能过期
- **方案三（GitHub CLI）**：最简单，但需要安装额外工具

## 常见问题

### Q: SSH 密钥已添加但仍提示 Permission denied？

```bash
# 检查 SSH 密钥是否加载
ssh-add -l

# 如果没有，添加密钥
ssh-add ~/.ssh/id_ed25519

# 测试连接
ssh -T git@github.com
```

### Q: 如何查看当前远程仓库地址？

```bash
git remote -v
```

### Q: 如何切换远程仓库地址？

```bash
# 切换到 HTTPS
git remote set-url origin https://github.com/yourusername/hybrid-search.git

# 切换到 SSH
git remote set-url origin git@github.com:yourusername/hybrid-search.git
```

### Q: Personal Access Token 忘记了怎么办？

重新生成一个新的 token，然后更新 Git 凭据：

```bash
# 清除保存的凭据
git credential reject <<EOF
protocol=https
host=github.com
EOF

# 下次推送时会重新提示输入
```

---

## 快速命令参考

```bash
# 查看远程仓库
git remote -v

# 添加远程仓库（如果还没有）
git remote add origin https://github.com/yourusername/hybrid-search.git

# 推送代码
git push -u origin master

# 如果远程使用 main 分支
git branch -M main
git push -u origin main
```
