# Contributing

欢迎通过 GitHub Issue 报告问题、讨论功能，并通过 Pull Request 提交改进。

## 本地开发

1. Fork 仓库并克隆到本地。
2. 安装 JDK 8 或更高版本，以及 Maven 3.6.3 或更高版本。
3. 创建分支，例如 `git switch -c fix/parser-empty-fields`。
4. 完成修改后运行：

   ```sh
   mvn --batch-mode --no-transfer-progress verify
   ```

现有测试不需要运行 Kafka、OceanBase 或 TDengine。涉及真实服务的修改，请在 PR 中说明集成验证的环境与结果。

Java 代码使用 4 个空格缩进；YAML 使用 2 个空格缩进。文本文件统一使用 UTF-8 和 LF，具体规则见 [`.editorconfig`](.editorconfig)。

## 提交信息

提交信息与 PR 标题采用 [Conventional Commits 1.0.0](https://www.conventionalcommits.org/zh-hans/v1.0.0/) 规范，这是 GitHub 开源项目常用的约定。

```text
<type>(<scope>): <description>
```

`scope` 可省略，通常填写受影响的模块，例如 `parser`、`config` 或 `writer`。`type` 使用下列小写标识；描述可以使用中文或英文，简洁说明本次改动。

| 类型 | 用途 |
| --- | --- |
| `feat` | 新功能 |
| `fix` | 修复缺陷 |
| `docs` | 文档修改 |
| `style` | 不影响行为的格式调整 |
| `refactor` | 不增加功能或修复缺陷的重构 |
| `perf` | 性能优化 |
| `test` | 测试变更 |
| `build` | 构建流程或依赖变更 |
| `ci` | 持续集成配置变更 |
| `chore` | 其他维护工作 |
| `revert` | 撤销已有修改 |

示例：

```text
feat(mapping): support new timestamp formats
fix(parser): preserve empty message fields
docs: update configuration examples
```

正文与标题之间空一行，用于说明修改原因和影响。破坏兼容性的变更在类型或作用域后添加 `!`，并在正文或 `BREAKING CHANGE:` 页脚中说明迁移方式；关联 Issue 可使用 `Closes #123` 页脚。

```text
feat(config)!: rename the retry configuration key

BREAKING CHANGE: Rename retryCount to maxRetries in application.yaml.
Closes #123
```

仓库提供 [`.gitmessage`](.gitmessage) 模板，可在当前克隆中启用，随后通过不带 `-m` 的 `git commit` 使用：

```sh
git config --local commit.template .gitmessage
```

## Pull Request

- 向 `main` 分支提交 PR，一次聚焦一个问题。
- 使用仓库提供的 PR 模板，说明问题、修改结果和验证方式。
- 涉及配置或行为变化时同步更新文档，并按需要补充测试。
- 示例配置使用占位值；本地账号、密码、运行日志和失败消息不应提交到仓库。
- PR 标题遵循上述提交规范，GitHub Actions 会检查标题，并在 Java 8 和 Java 17 上执行 Maven 构建与测试。

建议维护者使用 **Squash and merge**，并将 PR 标题作为合并后的提交标题。

## 许可证

向本项目提交的原创贡献采用 [MIT 许可证](LICENSE)。引入第三方代码时，保留其原有的版权声明与许可证，并在 PR 中注明来源。
