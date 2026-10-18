# 参与开发

欢迎修复问题、完善功能、测试和文档。环境依赖、服务配置及部署方式见 [README](README.md)。

## 按任务开发

- 一个分支和一个 PR 聚焦一个问题或完整功能；无关的重构、格式调整单独提交。
- 较大的任务先开 Issue，记录现象、复现步骤和预期行为；小改动可直接在 PR 中说明。
- 在开发分支中提交和推送，完成验证与差异检查后通过 PR 合入 `main`。未完成的工作可先开 Draft PR。

例如，从已同步的主分支创建修复分支：

```bash
git switch main
git pull --ff-only origin main
git switch -c fix/mq-consumer-ack
```

切换前先检查 `git status`，保留已有本地改动。如果无法快进同步，先查明分歧，不要用强制覆盖代替处理。

## 提交内容与说明

每个提交应完成一个可解释、可审查的逻辑改动，相关实现与回归测试可以放在一起。按功能边界提交，不按天数或文件数量拆分。

使用 `类型(模块): 具体说明`，中文或英文均可：

```text
fix(mq): 仅在消息持久化成功后确认消费
feat(client): 显示消息发送状态
docs(deploy): 补充数据库迁移步骤
chore(git): 排除本地构建与临时文件
```

常用类型为 `feat`、`fix`、`test`、`docs`、`refactor`、`build`、`ci` 和 `chore`。复杂改动在正文中解释原因、行为变化及兼容性。

只暂存本次任务涉及的路径。提交前检查：

```bash
git status --short
git diff
git diff --cached --name-only
git diff --cached
git diff --cached --check
```

不要提交真实凭据、运行数据或构建产物。`.gitignore` 不会停止跟踪已经入库的文件；如果发现历史误提交，单独说明并处理。

本地私有配置可使用 `*.local.conf` 等忽略的文件名，但需要启动命令显式指定配置路径；改文件名本身不会让服务自动加载。需要分享配置格式时，提供脱敏示例。

## 构建与验证

按改动范围选择验证，并在 PR 中记录环境、实际命令和结果。没有运行或被环境阻塞的检查应如实说明，不要填写为通过。

### 服务端

推荐在已安装 README 所列依赖的 Linux 环境中，对受影响的服务使用独立构建目录。例如消息服务：

```bash
cmake -S server/message -B server/message/build
cmake --build server/message/build -j
```

其他服务同样按 `server/<service>` 分别构建，不要在源码目录中进行原地构建。公共组件变化应检查使用它的相关服务。

已有联调客户端位于 `server/<service>/test/`，部分数据库测试有独立 Makefile。它们通常依赖运行中的服务或数据库，不能只凭编译成功就认定联调通过。使用隔离的测试数据，记录所需依赖和执行结果。

### Qt 客户端

安装 README 中列出的 Qt 6 模块后运行：

```bash
cmake -S client/ChatClient -B client/ChatClient/build
cmake --build client/ChatClient/build --config Release
```

界面改动附截图或复现步骤；消息状态变化应验证正常发送、失败及乱序回调等受影响场景。

### 协议与数据库

- 修改 Protobuf 时，分别检查服务端和客户端协议，以及受影响的生成代码和调用方；不要将两侧协议目录混作一个编译输入。
- 数据库结构变化同时更新初始化 SQL 和 `server/sql/migrations/` 中的升级脚本，说明新建与已有数据库的验证方式。
- 文档或忽略规则改动，检查链接、示例路径和匹配结果；无需为了纯文档改动启动全部业务服务。

目前仓库尚未配置统一 CI，以上是本地验证约定，不代表已有自动化检查。

## 创建与合并 PR

推送任务分支并向 `main` 创建 PR，按模板填写：问题、改动、验证结果及兼容性影响。修复已有 Issue 时在说明中关联它。

```bash
git push -u origin codex/fix-mq-consumer-ack
```

创建者先检查完整差异；有协作者时邀请评审并处理反馈。个人自查应如实记录，不等同于他人批准。检查完成后再合并，避免直接将未验证功能推入 `main`。

一个 PR 可以包含多个相关提交。若有大量临时修正提交，可用 Squash merge 保留一条清晰的最终记录；合并后删除已完成的远端任务分支。已有公开历史不为整理展示而改写。
