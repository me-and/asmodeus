from collections.abc import Iterable, Mapping
from typing import Callable, NoReturn, Optional, Protocol, TYPE_CHECKING, Union, assert_never, assert_type
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
from asmodeus.types import Task, TaskProblem, ProblemTestResult
import asmodeus._utils as _utils

if TYPE_CHECKING:
    from asmodeus.taskwarrior import TaskWarrior

PostHookAction: TypeAlias = Callable[[], None]

TaskHookResult: TypeAlias = tuple[int, Optional[Task], Optional[str],
                                  Optional[PostHookAction]]
BareHookResult: TypeAlias = tuple[int, Optional[str], Optional[PostHookAction]]


class OnAddHook(Protocol):
    def __call__(self, tw: 'TaskWarrior', modified_task: Task) -> TaskHookResult: ...


class OnModifyHook(Protocol):
    def __call__(self, tw: 'TaskWarrior', modified_task: Task, orig_task: Task) -> TaskHookResult: ...


class OnAddModifyHook(OnAddHook, OnModifyHook, Protocol):
    def __call__(self, tw: 'TaskWarrior', modified_task: Task, orig_task: Optional[Task] = None) -> TaskHookResult: ...


BareHook: TypeAlias = Callable[['TaskWarrior'], BareHookResult]


PID_SLEEP_MIN_INTERVAL = 0.001
PID_SLEEP_MAX_INTERVAL = 0.5


CONTEXT_TAGS = frozenset((
    'alex',
    'anywhere',
    'audio',
    # 'business'  # Excluded as there should always be another tag.
    'dadford',
    'enfield',
    'home',
    'internet',
    'linaker',
    'multivac',
    'pc',
    'phone',
    'ssh',
    'waitingfor',
    'work',
))


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


def missing_context_tags(task: Task) -> bool:
    tags = task.get_tags()
    return "inbox" not in tags and len(set(tags) & CONTEXT_TAGS) == 0


missing_context_problem = TaskProblem(missing_context_tags, 'no context tags')


def missing_project(task: Task) -> bool:
    return not task.has_tag('inbox') and 'project' not in task


missing_project_problem = TaskProblem(missing_project, 'no project')


def inbox_if_hook_gen(test: Callable[[Task], bool]) -> OnAddModifyHook:
    def hook(tw: 'TaskWarrior', modified_task: Task,
             orig_task: Optional[Task] = None) -> TaskHookResult:
        if not modified_task.has_tag('inbox') and test(modified_task):
            modified_task.tag('inbox')
            return 0, modified_task, f'Added inbox tag to {modified_task.describe()}', None
        return 0, modified_task, None, None
    return hook


def problem_tag_hook_gen(problems: Union[TaskProblem, Iterable[TaskProblem]]) -> OnAddModifyHook:
    def hook(tw: 'TaskWarrior', modified_task: Task,
             orig_task: Optional[Task] = None) -> TaskHookResult:
        if modified_task.has_tag('inbox'):
            # Don't think this task is set up properly yet anyway.
            return 0, modified_task, None, None
        result = modified_task.check_log_problems(problems)
        if result is ProblemTestResult(0):
            return 0, modified_task, None, None
        elif result is ProblemTestResult.ADDED:
            return 0, modified_task, f'Found and tagged problems with {modified_task.describe()}', None
        elif result is ProblemTestResult.REMOVED:
            return 0, modified_task, f'Found and untagged resolved problems with {modified_task.describe()}', None
        elif result is (ProblemTestResult.ADDED | ProblemTestResult.REMOVED):
            # TODO Well that's awkward: mypy apparently thinks this code is
            # unreachable when it _is_ reachable.  Apparently mypy's
            # exhaustiveness checks don't believe in flag enums.
            #
            # Currently demonstrated with assert_type, which is a no-op at
            # runtime and should cause an assertion during type checking.
            assert_type(None, int)
            return 0, modified_task, f'Fonud and tagged new problems, and removed old problems, with {modified_task.describe()}', None
        else:
            reveal_type(result)
            assert_never(modified_task)
    return hook


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


def on_add(tw: 'TaskWarrior',
           hooks: Union[OnAddHook, Iterable[OnAddHook]]) -> NoReturn:
    task: Optional[Task]
    task = Task.from_json_str(sys.stdin.readline())

    if not isinstance(hooks, Iterable):
        hooks = (hooks,)

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


def on_modify(tw: 'TaskWarrior',
              hooks: Union[OnModifyHook, Iterable[OnModifyHook]]) -> NoReturn:
    orig_task = Task.from_json_str(sys.stdin.readline())
    modified_task: Optional[Task]
    modified_task = Task.from_json_str(sys.stdin.readline())

    if not isinstance(hooks, Iterable):
        hooks = (hooks,)

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
