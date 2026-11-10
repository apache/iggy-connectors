## Which issue does this PR address?

<!--
We generally require a GitHub issue for all bug fixes and new features. Link it with GitHub syntax, keep the line that applies and delete the other:
- `Closes #123` links the issue, and the merge of this PR closes it (full fix).
- `Relates to #123` links without closing (partial or related work).
-->

Closes #
Relates to #

## Rationale

<!--
Why is this change needed? If the issue explains it well, a one-liner is fine.
-->

## What changed?

<!--
2-4 sentences. Problem first (before), then solution (after).

GOOD:

"The Iceberg sink committed each append once. When the catalog
restarted, the commit failed, but the runtime had already stored the
offset of the batch. The batch was lost.

The sink now retries the commit with a capped exponential backoff."

GOOD:

"In the Iceberg sink, one row with a wrong type failed the whole batch.
All good rows of that batch were lost with the bad row."

BAD:
- Walls of text
- "This PR adds..." (we can see the diff)
-->

## Local Execution

- Passed / not passed
- Pre-commit hooks ran / not ran

<!--
You must run your code locally before submitting.
"Relying on CI" is not acceptable. We close PRs from authors who did not run the code.

Did you install `prek`? It runs on every commit and covers all project languages.
[CONTRIBUTING.md](https://github.com/apache/iggy-connectors/blob/master/CONTRIBUTING.md) has the setup steps.
-->

## AI Usage

<!--
Did you use AI tools? Then please answer:
1. Which tools? (for example GitHub Copilot, Claude, ChatGPT)
2. Scope of usage? (for example autocomplete, generated functions, entire implementation)
3. How did you verify the generated code works correctly?
4. If someone asks, can you explain every line of the code?

Maintainers can close agent-driven PRs without a code review.
See https://github.com/apache/iggy-connectors/blob/master/CONTRIBUTING.md#agent-driven-prs

If no AI tools were used, write "None" or delete this section.
-->
