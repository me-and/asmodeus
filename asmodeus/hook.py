from collections.abc import Iterable, Mapping
from typing import Callable, NoReturn, Optional, TYPE_CHECKING, Union
import datetime
import functools
import sys
import os
import time

if sys.version_info >= (3, 11):
    from typing import TypeAlias
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import TypeAlias

from asmodeus.json import (
        JSONable,
        JSONableDate,
        JSONableDict,
        JSONableDuration,
        JSONableStringList,
)
from asmodeus.types import Task
import asmodeus._utils as _utils

if TYPE_CHECKING:
    from asmodeus.taskwarrior import TaskWarrior

PostHookAction: TypeAlias = Callable[[], None]

TaskHookResult: TypeAlias = tuple[int, Optional[Task], Optional[str],
                                  Optional[PostHookAction]]
BareHookResult: TypeAlias = tuple[int, Optional[str], Optional[PostHookAction]]

OnAddHook: TypeAlias = Callable[['TaskWarrior', Task], TaskHookResult]
OnModifyHook: TypeAlias = Callable[['TaskWarrior', Task, Optional[Task]],
                                   TaskHookResult]
BareHook: TypeAlias = Callable[['TaskWarrior'], BareHookResult]

PID_SLEEP_MIN_INTERVAL = 0.001
PID_SLEEP_MAX_INTERVAL = 0.5


# Based on https://github.com/giampaolo/psutil
def pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # EPERM clearly means there's a process to deny access to.
        return True
    else:
        return True


def wait_for_pid(pid: int) -> None:
    interval = PID_SLEEP_MIN_INTERVAL
    while pid_exists(pid):
        time.sleep(interval)
        interval = min(interval * 2, PID_SLEEP_MAX_INTERVAL)


def due_end_of(tw: 'TaskWarrior', modified_task: Task,
               orig_task: Optional[Task] = None) -> TaskHookResult:
    status = modified_task.get_typed('status', str)
    if status == 'recurring':
        # Don't modify recurring tasks; they'll get fixed when the individual
        # task instances are created.
        return 0, modified_task, None, None

    try:
        due = modified_task.get_typed('due', datetime.datetime)
    except KeyError:
        return 0, modified_task, None, None

    if due.astimezone().time() == datetime.time():
        new_due = due - datetime.timedelta(seconds=1)
        modified_task['due'] = new_due
        return (0, modified_task,
                (f'Changed due from {due} to {new_due}'), None)

    return 0, modified_task, None, None


class Modifications(JSONableDict[JSONable]):
    _key_class_map: Mapping[str,
                            Union[type[JSONable],
                                  tuple[type[JSONable],
                                        Callable[..., JSONable]]]
                            ] = (
            Task._key_map | {
                'add-tags': JSONableStringList,
                'remove-tags': JSONableStringList,
            })


def recur_after(tw: 'TaskWarrior', modified_task: Task,
                orig_task: Optional[Task] = None
                ) -> TaskHookResult:
    if (modified_task.get_typed('status', str) != 'completed' or
            (orig_task is not None and
             orig_task.get_typed('status', str) == 'completed')):
        return 0, modified_task, None, None

    wait_delay = modified_task.get_typed(
            'recurAfterWait', datetime.timedelta, None)
    due_delay = modified_task.get_typed(
            'recurAfterDue', datetime.timedelta, None)

    if wait_delay is None and due_delay is None:
        return 0, modified_task, None, None

    end_date = modified_task.get_typed('end', datetime.datetime)

    new_task = modified_task.duplicate()

    message_parts = [f'Creating new task {new_task["description"]}']
    if wait_delay is not None:
        new_wait = end_date + wait_delay
        new_task['wait'] = new_wait
        message_parts.append(f'waiting until {new_wait.isoformat()}')
    if due_delay is not None:
        new_due = end_date + due_delay
        new_task['due'] = new_due
        message_parts.append(f'due {new_due.isoformat()}')

    modifications = modified_task.get_typed(
            'recurAfterModifications', str, None)
    if modifications is not None:
        try:
            task_modifications = Modifications.from_json_str(modifications)
        except Exception as ex:
            return (1, None,
                    f'Failed to parse recurAfterModifications: {ex}', None)

        try:
            tags = task_modifications.pop('add-tags')
        except KeyError:
            pass
        else:
            assert _utils.is_iterable_str(tags)
            new_task.tag(tags)

        try:
            tags = task_modifications.pop('remove-tags')
        except KeyError:
            pass
        else:
            assert _utils.is_iterable_str(tags)
            new_task.untag(tags)

        for key, value in task_modifications.items():
            if value is None:
                del new_task[key]
            else:
                new_task[key] = value

    return (0, modified_task,
            ', '.join(message_parts),
            functools.partial(tw.to_taskwarrior, new_task))


def child_until(tw: 'TaskWarrior', modified_task: Task,
                orig_task: Optional[Task] = None) -> TaskHookResult:
    if modified_task.get_typed('status', str) == 'recurring':
        return 0, modified_task, None, None

    child_until = modified_task.get_typed(
            'recurTaskUntil', datetime.timedelta, None)

    if child_until is None:
        return 0, modified_task, None, None

    due = modified_task.get_typed('due', datetime.datetime, None)
    if due is None:
        return (1, None,
                f'Task {modified_task["uuid"]} has recurTaskUntil but no due',
                None)

    old_until = modified_task.get_typed('until', datetime.datetime, None)

    new_until = due + child_until
    modified_task['until'] = new_until

    message: Optional[str]
    if old_until is None:
        message = (f'Task {modified_task["description"]} '
                   f'expires {new_until.isoformat()}')
    else:
        if old_until == new_until:
            message = None
        else:
            message = (f'Task {modified_task["description"]} did expire '
                       f'{old_until.isoformat()}, now expires '
                       f'{new_until.isoformat()}')
    return 0, modified_task, message, None


def inbox(tw: 'TaskWarrior', task: Task) -> TaskHookResult:
    if 'tags' in task:
        return 0, task, None, None
    task['tags'] = ['inbox']
    return 0, task, f'Tagged {task["description"]} as inbox', None


def reviewed_to_entry(tw: 'TaskWarrior', modified_task: Task,
                      orig_task: Optional[Task] = None) -> TaskHookResult:
    '''Set the default reviewed value.

    Mark tasks that don't have a reviewed date as having been reviewed on the
    date they were entered.
    '''
    last_reviewed = modified_task.get_typed(
            'reviewed', datetime.datetime, None)
    task_entry = modified_task.get_typed('entry', datetime.datetime)
    if last_reviewed is not None:
        if last_reviewed >= task_entry:
            # This task han an explicit review date and it's more recent than
            # when the task was entered, so nothing to do.
            return 0, modified_task, None, None

    modified_task['reviewed'] = modified_task['entry']
    return 0, modified_task, None, None


def _do_final_jobs(jobs: Iterable[PostHookAction]) -> NoReturn:
    if jobs:
        sys.stdout.flush()
        if (0 < os.fork()):
            sys.exit(0)

        try:
            # TaskWarrior waits for this process to close stdout, so do that.
            os.close(sys.stdout.fileno())
        except OSError as ex:
            # Apparently this sometimes produces an error, possibly because
            # stdout has already been closed!?
            pass

        parent_pid = os.getppid()
        wait_for_pid(parent_pid)

        for job in jobs:
            job()

    sys.exit(0)


def on_add(tw: 'TaskWarrior', hooks: Iterable[OnAddHook]) -> NoReturn:
    task: Optional[Task]
    task = Task.from_json_str(sys.stdin.readline())

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        rc, task, feedback, final = hook(tw, task)

        if task is None or rc != 0:
            assert rc != 0
            assert feedback is not None
            print(feedback)
            sys.exit(rc)

        if feedback is not None:
            feedback_messages.append(feedback)
        if final is not None:
            final_jobs.append(final)

    print(task.to_json_str())
    if feedback_messages:
        print('; '.join(feedback_messages))

    _do_final_jobs(final_jobs)


def on_modify(tw: 'TaskWarrior', hooks: Iterable[OnModifyHook]) -> NoReturn:
    orig_task = Task.from_json_str(sys.stdin.readline())
    modified_task: Optional[Task]
    modified_task = Task.from_json_str(sys.stdin.readline())

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        rc, modified_task, feedback, final = hook(tw, modified_task, orig_task)

        if modified_task is None or rc != 0:
            assert rc != 0
            assert feedback is not None
            print(feedback)
            sys.exit(rc)

        if feedback is not None:
            feedback_messages.append(feedback)
        if final is not None:
            final_jobs.append(final)

    print(modified_task.to_json_str())
    if feedback_messages:
        print('; '.join(feedback_messages))

    _do_final_jobs(final_jobs)