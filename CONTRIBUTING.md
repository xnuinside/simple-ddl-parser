## How to contribute

Anyone who want to add new statements support or improve library anyhow - can do this.

Please describe issue that you want to solve and open the PR, I will review it as soon as possible.
Please, if you add support for new statements or any new features - don't forget to add tests for them. And run flake8 check before open the PR. Thank you!

Any questions? Ping me in Telegram: https://t.me/xnuinside 

## What can you do?

There is a lot of way how you can contribute to any project (not only in this), you can do it without code:

- Open the issue with new test case - SQL Statements that is not supported yet. What need to remember when you open such PR:

1) it must be valid SQL statement
2) if it is a some dialect - HQL, MySQL, Vertica or any - please mark it in the issue
3) if it totally new statement with no similar fields in output you also can add suggestions that must be in output as result

- Add more statements to the parser

1) Parser is written by lexx&yacc with usage ply library - https://www.dabeaz.com/ply/. If you never work with it - don't be afraid, it's pretty simple, need only to get main idea.

2) All parsers tokens are placed in tokens.py & statements described in ddl_parser.py for common and dialect-specific things I tried to move in dialects package (but not all yet)

3) When you prepare PR with some new statements do not forget create test to them similar as already exists in tests/

4) Before open the PR run flake8 check (it required and will be runs in Actions on PR in GitHub)

5) Do not forget add changes to CHANGELOG.txt

6) And updated SUPPORTED Statements in README.md

- Add more tests to the code

It's always needed, I have only functional tests right now, so if you want to help wiht covering library for example, with unittests - please welcome, open the PR.

For ANY type of contributinon I will really really appritiate. Each of them are important.

### How to run tests

```bash

    git clone https://github.com/xnuinside/simple-ddl-parser.git
    cd simple-ddl-parser
    poetry install # if you use poetry
    # or use `pip install .`
    pytest tests/ -vv

```
