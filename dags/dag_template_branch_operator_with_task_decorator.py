from airflow.decorators import dag, task, setup, teardown
from datetime import datetime
import random

@dag(
    dag_id="dag_template_branch_operator_with_task_decorator",
    start_date=datetime(2024, 11, 15),
    schedule_interval=None,
    catchup=False,
    tags=["Branching", "TaskDecorator"],
)
def init():

    @setup
    def start():
        pass

    @task.branch
    def branch_task():
        choice = random.choice(["path_one", "path_two"])
        print(f"Branching to: {choice}")
        return choice

    @task(
        doc_md="Task executed when the branch leads to `path_one`."
    )
    def path_one():
        print("Executing path one")

    @task(
        doc_md="Task executed when the branch leads to `path_two`."
    )
    def path_two():
        print("Executing path two")

    @teardown
    def end():
        pass

    start() >> branch_task() >> [path_one(), path_two()] >> end()

dag = init()

dag.doc_md = """
#### DAG: **dag_template_branch_operator_with_task_decorator**

*Template for Branch Operator with Task Decorator*

This DAG demonstrates the usage of the `@task.branch` decorator to create branching logic within a task. 

**Task Description**

- `start`: Setup task, executed before branching.
- `branch_task`: The branching task that randomly chooses between two paths.
- `path_one`: Task executed when the branch leads to `path_one`.
- `path_two`: Task executed when the branch leads to `path_two`.
- `end`: Teardown task, executed after all paths.

© Developed by Jefferson Alves - Git Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
