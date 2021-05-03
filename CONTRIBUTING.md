## How to contribute

Any one who want to add new statements support or improve library anyhow - can do this.

Please describe issue that you want to solve and open the PR, I will review it as soon as possible.
Please, if you add support for new statements or any new features - don't forget to add tests for them. And run flake8 check before open the PR. Thank you!

Any questions? Ping me in Telegram: https://t.me/xnuinside 

## What can you do?
There is a lot of way how you can contribute to any project (not only in this), you can do it without code:

- Open the issue with new test case - SQL Statements that is not supported yet. What need to remember when you open such PR:

1) it must be valid SQL statement
2) if it is a some dialect - HQL, MySQL or any - please mark it in the issue
3) if it totally new statement with no similar fields in output you also can add suggestions that must be in output as result

- Create 

For ANY type of contributinon I will really really appritiate. Each of them are important.


### How to run tests

```bash

    git clone https://github.com/xnuinside/simple-ddl-parser.git
    cd simple-ddl-parser
    poetry install # if you use poetry
    # or use `pip install .`
    pytest tests/ -vv

```
