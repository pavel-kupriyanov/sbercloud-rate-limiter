import argparse
from time import sleep
from random import randint
from functools import partial

from .rate_limiter import RateLimitedExecutor


def example_task(id: str, sleeping_time: int = 1):
    print(f'Task {id} started. Sleeping time: {sleeping_time}.')
    sleep(sleeping_time)
    print(f'Task {id} finished.')


def generate_tasks(number_of_tasks: int):
    return (
        partial(example_task, id, randint(1, 10))
        for id in range(number_of_tasks)
    )


def main(workers: int, max_per_minute: int, number_of_tasks):
    executor = RateLimitedExecutor(
        max_workers=workers, max_per_interval=max_per_minute
    )
    with executor:
        for i, task in enumerate(generate_tasks(number_of_tasks)):
            executor.submit(task)
            print(f'Planned task {i}.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rate limiter example')
    parser.add_argument('-n', '--workers', type=int, default=2)
    parser.add_argument('-x', '--max_per_minute', type=int, default=10)
    parser.add_argument('-t', '--number_of_tasks', type=int, default=100)
    args = parser.parse_args()
    try:
        main(args.workers, args.max_per_minute, args.number_of_tasks)
    except KeyboardInterrupt:
        print('Canceled.')
