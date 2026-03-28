# AGENTS.md

## Scope
- Apply these instructions only within this repository.
- Prefer small, focused changes.

## Workflow
- Check git status before and after changes.
- Avoid modifying unrelated files.
- Do not edit generated files unless explicitly requested.
- For issue work: pull latest `main`, add a repro test first, then fix the issue, run tests to confirm, regenerate `parsetab` as needed, and push to an issue-named branch on `origin`.
- When adding support for new statements, dialect features, or output fields, update user-facing docs (`README.md`) and the active `CHANGELOG.md` entry in the same change.

## Code Style
- Keep edits minimal and consistent with nearby code.
- Use ASCII only unless the file already uses Unicode and it is required.
- Add brief comments only when logic is non-obvious.

## Tests
- If changes affect behavior, run targeted tests when practical.
- Before reporting results, always run tests via `tox`; all tests must be green.
- Report test commands and results; do not fabricate.
- Always run linters before committing (ruff and black).

## Commits
- Make clear, imperative commit messages when asked to commit.
- Do not amend or rewrite history unless explicitly requested.
- Do not commit changes to `AGENTS.md` unless the user explicitly requested an `AGENTS.md` update.
- Always run the code before committing so `parsetab` is refreshed, and include its updates in the commit.
- Always update the changelog for the current version if it is greater than the latest tag. If the current version equals the latest tag, bump the version first, then add changelog entries.

## Tags
- Ignore legacy git tags that start with `v` when deciding changelog/version workflow.
- Treat `v*` tags as older than plain numeric release tags and do not use them to decide whether the current version is newer than the latest release.
- For changelog decisions, prefer the active release line in `CHANGELOG.md` over legacy `v*` tags.
