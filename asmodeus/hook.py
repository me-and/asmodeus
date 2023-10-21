from collections.abc import Callable, Iterable, Mapping
from typing import (
        Final,
        Literal,
        NoReturn,
        Optional,
        overload,
        Protocol,
        TYPE_CHECKING,
        Union,
        )
import datetime
import functools
import sys
import os
import time
import uuid
import fcntl
import json
import copy

if sys.version_info >= (3, 11):
    from typing import TypeAlias, assert_type, assert_never
else:
    from typing_extensions import TypeAlias, assert_type, assert_never

from asmodeus.json import (
        JSONable,
        JSONableDict,
        JSONableStringList,
        JSONableUUID,
        JSONValPlus,
        )
from asmodeus.taskwarrior import TaskCountError, TaskWarrior
from asmodeus.types import Task, TaskProblem, ProblemTestResult
import asmodeus._utils as _utils

PostHookAction: TypeAlias = Callable[[], None]

TaskHookResult: TypeAlias = tuple[int, Optional[Task], Optional[str],
                                  Optional[PostHookAction]]
BareHookResult: TypeAlias = tuple[int, Optional[str], Optional[PostHookAction]]


RECUR_AFTER_NAMESPACE: Final = uuid.UUID('3d963a36-2867-4629-a7ae-79533dd8bb2a')

DEBUG: Final = True
DEBUG_PATH: Final = os.path.expanduser("~/.asmodeus-hooks.log")

def log_debug_data(data: dict[str, JSONValPlus]) -> None:
    data["now"] = now_str()
    with open(DEBUG_PATH, "a") as log_file:
        fcntl.flock(log_file, fcntl.LOCK_EX)
        json.dump(data, log_file, separators=(",", ":"), default=JSONable._json_dumper)
        log_file.write('\n')


def now_str() -> str:
    return datetime.datetime.now().astimezone().strftime("%a %d %b %Y %H:%M:%S %Z")


class OnAddHook(Protocol):
    __name__: str
    def __call__(self,
                 tw: 'TaskWarrior',
                 modified_task: Task
                 ) -> TaskHookResult: ...


class OnModifyHook(Protocol):
    __name__: str
    def __call__(self,
                 tw: 'TaskWarrior',
                 modified_task: Task,
                 orig_task: Task
                 ) -> TaskHookResult: ...


class OnAddModifyHook(OnAddHook, OnModifyHook, Protocol):
    def __call__(self,
                 tw: 'TaskWarrior',
                 modified_task: Task,
                 orig_task: Optional[Task] = None
                 ) -> TaskHookResult: ...


@overload
def get_job_data(job: None) -> None: ...
@overload
def get_job_data(job: Union[Callable[[], None], functools.partial[None]]) -> dict[str, JSONValPlus]: ...
def get_job_data(job: Union[None, Callable[[], None], functools.partial[None]]) -> Optional[dict[str, JSONValPlus]]:
    if job is None:
        return None

    r: dict[str, JSONValPlus] = {}
    if isinstance(job, functools.partial):
        r["function"] = job.func.__name__
        args: list[JSONValPlus] = []
        for arg in job.args:
            if isinstance(arg, JSONable):
                args.append(copy.deepcopy(arg))
            else:
                args.append(repr(arg))
        r["args"] = args
    else:
        r["function"] = job.__name__
    return r


BareHook: TypeAlias = Callable[['TaskWarrior'], BareHookResult]


PID_SLEEP_MIN_INTERVAL = 0.001
PID_SLEEP_MAX_INTERVAL = 0.5


CONTEXT_TAGS = frozenset((
    'alex',
    'allotment',
    'anywhere',
    'audio',
    # 'business'  # Excluded as there should always be another tag.
    'dadford',
    'enfield',
    'home',
    'internet',
    'linaker',
    'multivac',
    'nibble',
    'pc',
    'phone',
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


def ensure_unique(tw: TaskWarrior, uuid: uuid.UUID) -> None:
    if not DEBUG:
        return

    try:
        tw.get_task(uuid)
    except TaskCountError as ex:
        count = ex.task_count
    else:
        count = 1
    log_debug_data({"uuid": str(uuid),
                    "count": count,
                    })


def due_end_of(tw: 'TaskWarrior',
               modified_task: Task,
               orig_task: Optional[Task] = None
               ) -> tuple[Literal[0], Task, Optional[str], None]:
    status = modified_task.get_typed('status', str)
    if status == 'recurring' or status == 'deleted':
        # Don't modify recurring tasks; they'll get fixed when the individual
        # task instances are created.  Similarly, don't modify deleted tasks,
        # as they _might_ be deleted recurring tasks, and the due date isn't
        # relevant anyway.
        return 0, modified_task, None, None

    try:
        due = modified_task.get_typed('due', datetime.datetime)
    except KeyError:
        return 0, modified_task, None, None

    if due.astimezone().time() == datetime.time():
        modified_task['due'] = new_due = due - datetime.timedelta(seconds=1)
        return (0, modified_task,
                (f'Changed due from {due} to {new_due}'), None)

    return 0, modified_task, None, None


class Modifications(JSONableDict[JSONable]):
    _key_map: Mapping[str,
                      Union[type[JSONable],
                            tuple[type[JSONable],
                                  Callable[..., JSONable]]]
                      ] = (
            Task._key_map | {
                'addTags': JSONableStringList,
                'removeTags': JSONableStringList,
            })


def recur_after(tw: 'TaskWarrior', modified_task: Task,
                orig_task: Optional[Task] = None
                ) -> tuple[Literal[0, 1], Optional[Task], Optional[str],
                           Optional[PostHookAction]]:
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
        wait_delay_round_down = modified_task.get_typed('recurAfterWaitRoundDown', str, None)
        match wait_delay_round_down:
            case None:
                pass
            case 'P1D':
                new_wait = new_wait.astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
            case _:
                raise ValueError(f"Could not parse recurAfterWaitRoundDown value {wait_delay_round_down!r}")
        new_task['wait'] = new_wait
        message_parts.append(f'waiting until {new_wait.isoformat()}')
    if due_delay is not None:
        new_due = end_date + due_delay
        new_task['due'] = new_due
        message_parts.append(f'due {new_due.isoformat()}')

    # Set UUID here on the basis of the previous UUID.  This means that if a
    # task is marked as completed multiple times, it'll still only create one
    # new task.  That's been intermittently a problem when syncing task
    # completions, even though I wouldn't expect hooks to run after a sync...
    new_task['uuid'] = JSONableUUID.uuid5(
            RECUR_AFTER_NAMESPACE,
            str(modified_task.get_typed('uuid', uuid.UUID)))

    modifications = modified_task.get_typed(
            'recurAfterModifications', str, None)
    if modifications is not None:
        try:
            task_modifications = Modifications.from_json_str(modifications)
        except Exception as ex:
            return (1, None,
                    f'Failed to parse recurAfterModifications: {ex}', None)

        try:
            tags = task_modifications.pop('addTags')
        except KeyError:
            pass
        else:
            assert _utils.is_iterable_str(tags)
            new_task.tag(tags)

        try:
            tags = task_modifications.pop('removeTags')
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


def child_until(tw: 'TaskWarrior',
                modified_task: Task,
                orig_task: Optional[Task] = None
                ) -> tuple[Literal[0, 1], Optional[Task], Optional[str], None]:
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


def waitingfor_adds_due(tw: 'TaskWarrior',
                        modified_task: Task,
                        orig_task: Optional[Task] = None
                        ) -> tuple[Literal[0], Task, Optional[str], None]:
    if modified_task.get_typed('status', str) != 'pending':
        # Don't care about tasks that aren't pending.
        return 0, modified_task, None, None

    if not modified_task.has_tag('waitingfor'):
        return 0, modified_task, None, None

    if 'due' in modified_task:
        return 0, modified_task, None, None

    modified_task['due'] = modified_task['entry']
    return (0, modified_task,
            (f'Due date added to undue {modified_task.describe()}'), None)


def missing_context_tags(task: Task) -> bool:
    tags = task.get_tags()
    status = task.get_typed('status', str)
    return ("inbox" not in tags
            and len(set(tags) & CONTEXT_TAGS) == 0
            and status not in ('completed', 'deleted')
            )


missing_context_problem = TaskProblem(missing_context_tags, 'no context tags')


def missing_project(task: Task) -> bool:
    status = task.get_typed('status', str)
    return (not task.has_tag('inbox')
            and 'project' not in task
            and status not in ('completed', 'deleted')
            )


missing_project_problem = TaskProblem(missing_project, 'no project')


class _ReliableHookWithoutJob(OnAddModifyHook, Protocol):
    def __call__(self,
                 tw: 'TaskWarrior',
                 modified_task: Task,
                 orig_task: Optional[Task] = None,
                 ) -> tuple[Literal[0],
                            Task,
                            Optional[str],
                            None]: ...


def inbox_if_hook_gen(test: Callable[[Task], bool]) -> _ReliableHookWithoutJob:
    def hook(tw: 'TaskWarrior',
             modified_task: Task,
             orig_task: Optional[Task] = None
             ) -> tuple[Literal[0], Task, Optional[str], None]:
        if not modified_task.has_tag('inbox') and test(modified_task):
            modified_task.tag('inbox')
            return (0, modified_task,
                    f'Added inbox tag to {modified_task.describe()}', None)
        return 0, modified_task, None, None
    return hook


def problem_tag_hook_gen(problems: _utils.OneOrMany[TaskProblem],
                         ) -> _ReliableHookWithoutJob:
    def hook(tw: 'TaskWarrior',
             modified_task: Task,
             orig_task: Optional[Task] = None
             ) -> tuple[Literal[0], Task, Optional[str], None]:
        if modified_task.has_tag('inbox'):
            # Don't think this task is set up properly yet anyway.
            return 0, modified_task, None, None
        result = modified_task.check_log_problems(problems)
        if result is ProblemTestResult(0):
            return 0, modified_task, None, None
        elif result is ProblemTestResult.ADDED:
            return (0, modified_task,
                    ('Found and tagged problems '
                     f'with {modified_task.describe()}'),
                    None)
        elif result is ProblemTestResult.REMOVED:
            return (0, modified_task,
                    ('Found and untagged resolved problems '
                     f'with {modified_task.describe()}'),
                    None)
        elif result is (ProblemTestResult.ADDED | ProblemTestResult.REMOVED):
            # TODO Well that's awkward: mypy apparently thinks this code is
            # unreachable when it _is_ reachable.  Apparently mypy's
            # exhaustiveness checks don't believe in flag enums.
            #
            # Currently demonstrated with assert_type, which is a no-op at
            # runtime and should cause an assertion during type checking.
            assert_type(None, int)
            return (0, modified_task,
                    ('Found and tagged new problems, '
                     'and removed old problems, '
                     f'with {modified_task.describe()}'),
                    None)
        else:
            reveal_type(result)
            assert_never(modified_task)
    return hook


def reviewed_to_entry(tw: 'TaskWarrior',
                      modified_task: Task,
                      orig_task: Optional[Task] = None
                      ) -> tuple[Literal[0], Task, None, None]:
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
        if DEBUG:
            job_list: list[JSONValPlus] = []
            debug_data: dict[str, JSONValPlus] = {"job": "final jobs",
                                              "start": now_str(),
                                              "jobs": job_list,
                                              }

        try:
            sys.stdout.flush()
            if (0 < os.fork()):
                sys.exit(0)

            try:
                # Taskwarrior waits for this process to close stdout, so do
                # that, as the follow-up tasks will almost certainly want to
                # call Taskwarrior commands themselves.
                os.close(sys.stdout.fileno())
            except OSError as ex:
                # Apparently this sometimes produces an error, possibly because
                # stdout has already been closed!?
                if DEBUG:
                    debug_data["os.close error"] = str(ex)

            parent_pid = os.getppid()
            wait_for_pid(parent_pid)

            for job in jobs:
                if DEBUG:
                    job_data = get_job_data(job)
                try:
                    job()
                except BaseException as ex:
                    if DEBUG:
                        job_data["exception"] = repr(ex)
                    raise
                finally:
                    job_list.append(job_data)

        finally:
            log_debug_data(debug_data)

    sys.exit(0)


def on_add(tw: 'TaskWarrior',
           hooks: _utils.OneOrMany[OnAddHook]) -> NoReturn:
    task: Optional[Task]
    task = Task.from_json_str(sys.stdin.readline())

    if DEBUG:
        hook_outcomes: list[JSONValPlus] = []
        debug_data: dict[str, JSONValPlus] = {"job": "on-add hook",
                                          "start": now_str(),
                                          "new task": copy.deepcopy(task),
                                          "hook outcomes": hook_outcomes,
                                          }

    if callable(hooks):
        hooks = (hooks,)

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        rc, task, feedback, final = hook(tw, task)

        if DEBUG:
            hook_outcomes.append({
                "hook name": hook.__name__,
                "rc": rc,
                "task": copy.deepcopy(task),
                "feedback": feedback,
                "final": get_job_data(final),
                })

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

    if DEBUG:
        log_debug_data(debug_data)
        if "uuid" in task:
            final_jobs.append(
                    functools.partial(ensure_unique,
                                      tw=tw,
                                      uuid=task.get_typed("uuid", uuid.UUID),
                                      ))

    _do_final_jobs(final_jobs)


def on_modify(tw: 'TaskWarrior',
              hooks: _utils.OneOrMany[OnModifyHook]) -> NoReturn:
    orig_task = Task.from_json_str(sys.stdin.readline())
    modified_task: Optional[Task]
    modified_task = Task.from_json_str(sys.stdin.readline())

    if DEBUG:
        hook_outcomes: list[JSONValPlus] = []
        debug_data: dict[str, JSONValPlus] = {"job": "on-modify hook",
                                          "start": now_str(),
                                          "original task": copy.deepcopy(orig_task),
                                          "modified task": copy.deepcopy(modified_task),
                                          "hook outcomes": hook_outcomes,
                                          }

    if callable(hooks):
        hooks = (hooks,)

    feedback_messages: list[str] = []
    final_jobs: list[PostHookAction] = []
    for hook in hooks:
        rc, modified_task, feedback, final = hook(tw, modified_task, orig_task)

        if DEBUG:
            hook_outcomes.append({
                "hook name": hook.__name__,
                "rc": rc,
                "task": copy.deepcopy(modified_task),
                "feedback": feedback,
                "final": get_job_data(final),
                })

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

    if DEBUG:
        log_debug_data(debug_data)
        if "uuid" in modified_task:
            final_jobs.append(
                    functools.partial(ensure_unique,
                                      tw=tw,
                                      uuid=modified_task.get_typed("uuid", uuid.UUID),
                                      ))

    _do_final_jobs(final_jobs)
