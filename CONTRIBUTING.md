# Contributing to Apache Iggy Connectors

Apache Iggy Connectors follow the contribution rules of [Apache Iggy](https://github.com/apache/iggy),
which its [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md) describes. This file
repeats the rules that bind every pull request here, and it adds the parts that differ in this repository:
the high-risk areas, the tests, the checks and the commit scopes.

## Issue First

Every new PR that introduces new functionality must link to an approved issue.
A maintainer can close a PR without one.

1. Create an issue or comment under existing
2. Wait for the issue to be assigned to you
    - A maintainer can ask for more details or a different approach
3. Then code

Some connector changes need a change in apache/iggy, for example in the server, the client SDK or the test
crate `integration`. Open the issue for such a change in [apache/iggy](https://github.com/apache/iggy/issues).
That change lands in apache/iggy first. Then a PR here moves the pinned apache/iggy commit, as the
[README](README.md#the-core-pin) describes.

### Good First Issues

The `good first issue` label marks issues for new contributors.

Take only one good first issue. After that, pick issues without the label, so that the
next new contributor can find one.

## High-Risk Areas

These require design discussion in the issue before coding:

- The FFI between the runtime and the plugins, and the public API of `iggy_connector_sdk`
- Source state and its save protocol (`runtime/src/state.rs`)
- Delivery guarantees (at-least-once sinks, exactly-once sources)
- The runtime HTTP API and the configuration format
- Operator-facing names: the image `apache/iggy-connect`, the environment variables, the plugin path rules
  and the default consumer groups
- The pinned apache/iggy commit (the core pin)

## PR Requirements

### Run It Locally

**If you cannot run it, you cannot submit it.**

Authors of PRs must run the code locally. "Relying on CI" is not acceptable.

The integration tests of a connector need Docker and a test server at the pinned apache/iggy commit. The
[README](README.md#tests) shows the commands.

### AI Assistance

You are responsible for the code that you submit. This is also true for code that a tool wrote.

Using an AI assistant to help you code is fine. Submitting code you do not understand is
not. Before you open a PR, you must be able to do these things without the tool:

- Explain what every part of the change does and why.
- Answer review questions about the change.
- Defend the design.

If you cannot, the PR is not ready.

The license rules for AI output are in the
[ASF guidance on generative tooling](https://www.apache.org/legal/generative-tooling.html).
You must follow them.

If an AI assistant helped you write the change, review it with the
[`team-review`](.claude/skills/team-review/SKILL.md) skill before you open the PR.
The skill can report false findings. Fix every finding that is correct and that applies to
your change.

While you are new to the project, please keep to **one open PR at a time**. Review takes
longer than writing, so a queue of changes from one contributor holds up everyone else's.

Some PRs read as a relay between the reviewer and a model, and not as a change that the
author understands and takes responsibility for. Maintainers can close such a PR at the
first review. That decision is about the submission, not about you. You can contribute
again with a change that you take responsibility for.

### Agent-Driven PRs

An AI agent is a tool that writes code by itself. In an agent-driven PR, an
agent does the work from start to end, and a person only approves the result. Maintainers
can close an agent-driven PR at their own discretion, without a code review.

Maintainers can run the same agents. Your PR is useful to us because you understand the
change and can maintain it later. Maintainer review time is limited, so we spend it on people
and not on agent output.

Maintainers look for signs like these:

- The AI Usage section says that an agent did the work and that you approved it.
- The description or a reply to a review describes changes that are not in the diff.
- You cannot explain the change without the agent.

If you follow [Bugs Found by an Agent](#bugs-found-by-an-agent), a bug that an agent found
is the exception. After a maintainer agrees on the approach and assigns the issue to you, an
agent can write the test and the fix.

Your first contribution does not have to be code. A [review of another PR](#reviewing-prs),
an answer on Discord, and an issue that shows that you read the source are all contributions.

### Bugs Found by an Agent

If the bug is a vulnerability, report it in private, as [SECURITY.md](SECURITY.md) tells you.
Do not open a public issue or PR for it.

1. Search open issues. If it is already there, comment on it.
2. Otherwise open an issue that shows how to reproduce the bug.
3. Wait until a maintainer agrees on the approach and assigns the issue to you.
4. Open one PR with the test and the fix, and link the issue with "Closes #123".
5. If the fix is hard, the PR can hold only the test, marked `#[ignore]`. Put any suggested
   fix in a TODO in the test, and link the issue with "Relates to #123".

### Green CI

Maintainers will not start reviewing a PR while its CI is failing. Get the
pipeline green first. A red build, lint, or test means the PR is not ready
for review.

### Single Purpose

One PR = one thing. Bug fix, refactor, feature: separate PRs. Mixed PRs will be closed.

### Review Comments

Respond to every review comment, so that the reviewer knows you read it. Any one of these
is enough:

- Resolve the conversation
- React with a thumbs-up
- Reply to the comment

### Updating Your Branch

Do not rebase your branch or merge `master` into it only to keep it up to date. Each push
runs CI again on GitHub Actions runners that all Apache projects share. A maintainer
updates the branch before the merge.

If GitHub reports a merge conflict, or if a maintainer asks you to, update your branch.

### Quality Checks

Run these from the root of the repository:

```bash
cargo fmt --all
cargo sort --no-format --grouped --workspace
cargo clippy --all-features --all-targets -- -D warnings
cargo machete --with-metadata
cargo build
cargo nextest run -p <package>
```

`cargo build` builds the runtime and every plugin into `target/debug/`, where the integration tests load
them. Before the first integration test, and after each change of the pinned apache/iggy commit, run
`./scripts/build-test-server.sh`.

### Typos Checks

We use [typos](https://github.com/crate-ci/typos):

```bash
cargo install typos-cli --locked
typos
typos --write-changes
```

If it is not a typo, you can set an exception in `.typos.toml`.

### License Header Checks

We use [HawkEye](https://github.com/fast/hawkeye):

```bash
cargo install hawkeye --version "$(cat .github/config/hawkeye.version)" --locked
./scripts/ci/license-headers.sh --check
./scripts/ci/license-headers.sh --fix
```

### Pre-commit Hooks

We use [prek](https://github.com/j178/prek):

```bash
cargo install prek
prek install
```

The hooks require **bash >= 4.2** and refuse to run on anything older. macOS ships bash 3.2, so it needs
the [macOS steps](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md#macos) of apache/iggy.

## Code Style

The code style is the [code style of apache/iggy](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md#code-style).
Comments explain why, not what. [AGENTS.md](AGENTS.md#repo-wide-principles) lists the principles that apply
to every change.

### Commit Messages

Format: `type(scope): subject`

**Good examples from this repo:**

```none
fix(connectors): tighten Iceberg sink redelivery skip and error mapping
feat(connectors): deliver sink messages at least once
fix(connectors): honor Retry-After on every retried status
feat(connectors): add MQTT source connector
```

Keep subject under 72 chars. Put the details in the body. The allowed types and scopes are in
`.github/workflows/pr-title.yml`.

## Reviewing PRs

Anyone can review a PR. You do not need to be a committer, and reviews from contributors
are welcome. Read the change and leave comments or a review. If you can, run the change
locally. Questions count too. If something is not clear to you, it probably is not clear to the next
reader either.

A committer still has to approve and merge. If you are a returning contributor, your
"Request changes" review moves the PR to `S-waiting-on-author`, and you can use `/author`
to do the same.

## PR Triage Commands

A slash command moves a PR in the review queue. Post it on its own line in a
regular PR comment, not in an inline review reply:

| Command                             | Who                                 | Effect                                                       |
| ----------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| `/ready`                            | author or maintainer                | mark `S-waiting-on-review`                                   |
| `/author`                           | maintainer or returning contributor | mark `S-waiting-on-author`                                   |
| `/request-review @user-or-team ...` | author or maintainer                | request review from the listed `@user` / `@org/team` handles |
| `/pin`                              | author or maintainer                | add `pinned`, exempting the PR from the stale bot            |
| `/unpin`                            | author or maintainer                | remove `pinned`                                              |

A returning contributor is anyone with at least one merged commit in apache/iggy-connectors.

Some labels move on their own. Opening a non-draft PR, or marking a draft
ready, sets `S-waiting-on-review`. A "Request changes" review sets
`S-waiting-on-author`, and so does a `/skill` bot review with findings for the
author. Closing a PR, or converting it to a draft, clears both.

When a PR is opened, including a draft, `PR Triage Apply` checks its author once.
If a human author opened PRs in more than 20 visible repositories in the last 30 days,
it adds `Prolific Author`. An author with at least 10 merged PRs in this repository does
not get the label. This follows [Arrow-rs's criteria](https://github.com/apache/arrow-rs/issues/11808).
The label is informational and is not removed automatically. Maintainers can use it to
decide where to spend review time.

Commands take up to about 90 seconds. A thumbs-up reaction means that the command applied, and a confused
reaction means that you lacked permission. If neither shows up, check the `PR Triage Apply` run in the
Actions tab.

## Close Policy

A maintainer can close a PR for these reasons:

- Maintainer feels like proxy between maintainer and LLM
- Agent-driven PR, see [Agent-Driven PRs](#agent-driven-prs)
- No approved issue or no approval from a maintainer
- Code not ran and tested locally
- Mixed purposes or purposes not clear
- Cannot answer questions about the change
- Inactivity, see [Stale PRs](#stale-prs) below

Whoever closes leaves a comment saying why. If the thread holds a finding
that outlives the change, open an issue for it and link it from that
comment. The closed thread is the first place someone looks to find out
whether anything fell through.

### Stale PRs

A bot labels a PR `S-stale` after 7 days without activity and closes it 7
days after that. A push, comment, review, reopen, or ready-for-review
clears the label. Drafts and PRs labeled `pinned` are exempt. Issues are
never labeled or closed by it. A closed PR can be reopened.

## Questions?

[Discussions](https://github.com/apache/iggy-connectors/discussions), [Discord](https://discord.gg/apache-iggy)
or the [dev@iggy.apache.org](https://iggy.apache.org/community/mailing-lists/) mailing list (subscribe by
emailing <dev-subscribe@iggy.apache.org>)

GitHub Discussions are mirrored to dev@. The mirror only runs one way: replies sent to the list do not appear
in Discussions.
