from typing import TypeAlias, Optional, Callable, TYPE_CHECKING, NoReturn
from collections.abc import Iterable
import datetime
import functools
import sys
import os
import time

from asmodeus.columns import Task
from asmodeus._utils import JSONableDuration, JSONableDate, JSONableString, is_iterable_str, load_json, dump_json

if TYPE_CHECKING:
    from asmodeus.taskwarrior import TaskWarrior

PostHookAction: TypeAlias = Callable[[], None]

TaskHookResult: TypeAlias = tuple[int, Optional[Task], Optional[str], Optional[PostHookAction]]
BareHookResult: TypeAlias = tuple[int, Optional[str], Optional[PostHookAction]]

OnAddHook: TypeAlias = Callable[['TaskWarrior', Task], TaskHookResult]
OnModifyHook: TypeAlias = Callable[['TaskWarrior', Task, Optional[Task]], TaskHookResult]
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
    if modified_task['status'] == 'recurring':  # type: ignore[comparison-overlap]  # False warning
        # Don't modify recurring tasks; they'll get fixed when the individual
        # task instances are created.
        return 0, modified_task, None, None

    try:
        due = modified_task['due']
    except KeyError:
        return 0, modified_task, None, None

    assert isinstance(due, datetime.datetime)
    if due.astimezone().time() == datetime.time():
        new_due = due - datetime.timedelta(seconds=1)
        modified_task['due'] = new_due
        return (0, modified_task,
                (f'Changed due from {due} to {new_due}'), None)

    return 0, modified_task, None, None


def recur_after(tw: 'TaskWarrior', modified_task: Task,
                orig_task: Optional[Task] = None
                ) -> TaskHookResult:
    # Disabled warnings because mypy doesn't realise there's a subclass of
    # JSONable that has class overlap with str, and that this gets handled
    # competently.
    if (modified_task['status'] != 'completed' or (  # type: ignore[comparison-overlap]
             orig_task is not None and
              orig_task['status'] == 'completed')):  # type: ignore[comparison-overlap]
        return 0, modified_task, None, None

    wait_delay = modified_task.get('recurAfterWait', None)
    due_delay = modified_task.get('recurAfterDue', None)

    if wait_delay is None and due_delay is None:
        return 0, modified_task, None, None

    end_date = modified_task['end']
    assert isinstance(end_date, JSONableDate)

    new_task = modified_task.duplicate()

    message_parts = [f'Creating new task {new_task["description"]}']
    if wait_delay is not None:
        assert isinstance(wait_delay, JSONableDuration)
        new_wait = end_date + wait_delay
        new_task['wait'] = new_wait
        message_parts.append(f'waiting until {new_wait.isoformat()}')
    if due_delay is not None:
        assert isinstance(due_delay, JSONableDuration)
        new_due = end_date + due_delay
        new_task['due'] = new_due
        message_parts.append(f'due {new_due.isoformat()}')

    modifications = modified_task.get('recurAfterModifications', None)
    if modifications is not None:
        assert isinstance(modifications, JSONableString)
        try:
            task_modifications = load_json(modifications)
        except Exception as ex:
            return 1, None, f'Failed to parse recurAfterModifications: {ex}', None

        assert isinstance(task_modifications, dict)

        try:
            tags = task_modifications.pop('add-tags')
        except KeyError:
            pass
        else:
            assert is_iterable_str(tags)
            new_task.tag(tags)

        try:
            tags = task_modifications.pop('remove-tags')
        except KeyError:
            pass
        else:
            assert is_iterable_str(tags)
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
    if modified_task['status'] == 'recurring':  # type: ignore[comparison-overlap]
        return 0, modified_task, None, None

    child_until = modified_task.get('recurTaskUntil', None)

    if child_until is None:
        return 0, modified_task, None, None

    due = modified_task.get('due', None)
    if due is None:
        return (1, None,
                f'Task {modified_task["uuid"]} has recurTaskUntil but no due',
                None)

    old_until = modified_task.get('until', None)

    assert isinstance(due, JSONableDate)
    assert isinstance(child_until, JSONableDuration)
    new_until = due + child_until
    modified_task['until'] = new_until

    message: Optional[str]
    if old_until is None:
        message = f'Task {modified_task["description"]} expires {new_until.isoformat()}'
    else:
        assert isinstance(old_until, JSONableDate)
        if old_until == new_until:
            message = None
        else:
            message = f'Task {modified_task["description"]} did expire {old_until.isoformat()}, now expires {new_until.isoformat()}'
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
    last_reviewed = modified_task.get('reviewed', None)
    task_entry = modified_task['entry']
    if last_reviewed is not None:
        assert isinstance(last_reviewed, datetime.datetime)
        assert isinstance(task_entry, datetime.datetime)
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
    task: Optional[Task] = Task.from_json_val(load_json(sys.stdin.readline()))

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        assert task is not None
        rc, task, feedback, final = hook(tw, task)
        assert task is not None or (rc != 0 and feedback is not None)
        if rc != 0:
            print(feedback)
            sys.exit(rc)
        if feedback is not None:
            feedback_messages.append(feedback)
        if final is not None:
            final_jobs.append(final)

    print(dump_json(task))
    if feedback_messages:
        print('; '.join(feedback_messages))

    _do_final_jobs(final_jobs)


def on_modify(tw: 'TaskWarrior', hooks: Iterable[OnModifyHook]) -> NoReturn:
    orig_task = Task.from_json_val(load_json(sys.stdin.readline()))
    modified_task: Optional[Task] = Task.from_json_val(load_json(sys.stdin.readline()))

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        assert modified_task is not None
        rc, modified_task, feedback, final = hook(tw, modified_task, orig_task)
        assert modified_task is not None or (rc != 0 and feedback is not None)
        if rc != 0:
            print(feedback)
            sys.exit(rc)
        if feedback is not None:
            feedback_messages.append(feedback)
        if final is not None:
            final_jobs.append(final)

    print(dump_json(modified_task))
    if feedback_messages:
        print('; '.join(feedback_messages))

    _do_final_jobs(final_jobs)
