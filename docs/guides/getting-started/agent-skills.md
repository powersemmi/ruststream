---
description: "Install the RustStream agent skills, so a coding agent writes services, tests and broker crates the way the framework recommends."
---

# Agent skills

The repository ships agent skills: guidance a coding agent loads when a task needs it, so the code
it writes follows the framework's recommended path.
Install them in Claude Code with two commands:

```text
/plugin marketplace add powersemmi/ruststream
/plugin install ruststream@ruststream
```

The plugin brings five skills.
An agent picks a skill by its description, so a request about a RustStream service loads the
service skill without naming it.

| Skill | When an agent uses it |
|---|---|
| `ruststream` | writing a service: handlers, the app, replies, publishing, retries, state, codecs |
| `ruststream-testing` | testing a service with `TestApp` |
| `ruststream-without-macros` | building a service without the `macros` feature |
| `ruststream-broker` | writing a broker crate |
| `ruststream-core` | contributing to the core repository |

## Other agents

Each skill is a directory with a `SKILL.md` and its reference pages, under
[`.agents/skills/`](https://github.com/powersemmi/ruststream/tree/main/.agents/skills) in the
repository.
Cursor and Codex read skills from `.agents/skills/` of the project they work in.
To give an agent the skills in your own service, copy the directories you need into that
directory of your project, or into the skill directory your agent reads.

## Matching the version

The skills describe the framework of the revision they come from.
Install them from the same release line as the `ruststream` your service depends on.
A skill is guidance, not a check: the compiler and the tests remain the judges of the result.
