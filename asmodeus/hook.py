from collections.abc import Callable, Iterable, Mapping
from typing import (
        Final,
        Literal,
        NoReturn,
        Optional,
        Protocol,
        TYPE_CHECKING,
        Union,
        cast,
        overload,
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
import re

from dateutil.relativedelta import relativedelta

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
from asmodeus.types import Task, TaskList, TaskProblem, ProblemTestResult
import asmodeus._utils as _utils

PostHookAction: TypeAlias = Callable[[], None]

TaskHookResult: TypeAlias = Union[tuple[Literal[0], Task, Optional[str],
                                        Optional[PostHookAction]],
                                  tuple[int, None, str, None],
                                  ]
BareHookResult: TypeAlias = Union[tuple[Literal[0], Optional[str], Optional[PostHookAction]],
                                  tuple[int, str, None],
                                  ]


RECUR_AFTER_NAMESPACE: Final = uuid.UUID('3d963a36-2867-4629-a7ae-79533dd8bb2a')


_SINGLE_TASK_ID_RE: Final = re.compile(r'\d+')
_TASK_ID_RANGE_RE: Final = re.compile(r'\d+-\d+')
_SHORT_UUID_RE: Final = re.compile(r'[0-9a-f]{8}')
_FULL_UUID_RE: Final = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

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
    'car',
    'dadford',
    'enfield',
    'home',
    'internet',
    'southport',
    'surface',
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


def due_end_of(tw: 'TaskWarrior',
               modified_task: Task,
               orig_task: Optional[Task] = None
               ) -> tuple[Literal[0], Task, None, None]:
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
        return 0, modified_task, None, None

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


def recurrance_is_whole_days(tw: 'TaskWarrior',
                             recur_str: str,
                             ) -> bool:
    if recur_str in (
            'annual', 'biannual', 'bimonthly', 'biweekly',
            'biyearly', 'daily', 'day', 'fortnight', 'monthly',
            'month', 'mth', 'mo', 'quarterly', 'quarter', 'qrtr',
            'qtr', 'semiannual', 'weekdays', 'weekly', 'week',
            'wk', 'yearly', 'year', 'yr'):
        # One of the durations in Duration.cpp that corresponds to
        # a straightforward whole day.
        return True

    if recur_str[0] == 'P' and 'T' not in recur_str:
        # It's an ISO duration, and it doesn't include "T".
        return True

    # Let Taskwarrior's calc work out what it looks like.  This should produce
    # a duration string, and we care about whether there's a "T" in it.
    dur_str = tw.calc(recur_str)
    assert dur_str[0] == 'P'
    return 'T' not in dur_str


# TODO This goes horribly wrong with a parent task waiting until 1 July at
# 00:00 is created as waiting until 23:00 on 29 February, when this fix means
# it ends up waiting until 00:00 on 29 February rather than 00:00 on 1 March.
def fix_recurrance_dst(tw: 'TaskWarrior',
                       modified_task: Task,
                       ) -> tuple[Literal[0], Task, Optional[str], None]:
    parent_uuid = modified_task.get_typed('parent', uuid.UUID, None)
    if parent_uuid is None:
        return 0, modified_task, None, None

    if not recurrance_is_whole_days(tw, modified_task.get_typed('recur', str)):
        return 0, modified_task, None, None

    description = modified_task.describe()
    parent = tw.get_task(parent_uuid)

    parent_due = parent.get_typed('due', datetime.datetime).astimezone()
    child_due = modified_task.get_typed('due', datetime.datetime).astimezone()

    modified_task['due'] = new_due = child_due.replace(hour=parent_due.hour)
    assert parent_due.time() == new_due.time()
    assert (child_due - new_due) <= datetime.timedelta(hours=1)  # TODO This flags when DST has gone wrong due to the above todo

    parent_wait = parent.get_typed('wait', datetime.datetime, None)
    child_wait = modified_task.get_typed('wait', datetime.datetime, None)

    if parent_wait is None:
        assert child_wait is None
        message = f'Task {description} DST fixes: due {child_due.time()} -> {new_due.time()}'
    else:
        assert child_wait is not None
        parent_wait = parent_wait.astimezone()
        child_wait = child_wait.astimezone()
        modified_task['wait'] = new_wait = child_wait.replace(hour=parent_wait.hour)
        assert parent_wait.time() == new_wait.time()
        assert (child_wait - new_wait) <= datetime.timedelta(hours=1)  # TODO Flag when DST has gone wrong per the above todo
        message = f'Task {description} DST fixes: due {child_due.time()} -> {new_due.time()}, wait: {child_wait.time()} -> {new_wait.time()}'

    return 0, modified_task, message, None


def recur_after(tw: 'TaskWarrior',
                modified_task: Task,
                orig_task: Optional[Task] = None,
                ) -> Union[tuple[Literal[0], Task, None, None],
                           tuple[Literal[0], Task, str, PostHookAction],
                           tuple[Literal[1], None, str, None],
                           ]:

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
        if wait_delay_round_down is None:
            pass
        elif wait_delay_round_down == 'P1D':
            new_wait = new_wait.astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f"Could not parse recurAfterWaitRoundDown value {wait_delay_round_down!r}")
        new_task['wait'] = new_wait
        message_parts.append(f'waiting until {new_wait.isoformat()}')

    if due_delay is not None:
        new_due = end_date + due_delay
        due_delay_round_down = modified_task.get_typed('recurAfterDueRoundDown', str, None)
        if due_delay_round_down is None:
            pass
        elif due_delay_round_down == 'P1D':
            new_due = new_due.astimezone().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(seconds=1)
        else:
            raise ValueError(f"Could not parse recurAfterDueRoundDown value {due_delay_round_down!r}")
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
                ) -> Union[tuple[Literal[0], Task, Optional[str], None],
                           tuple[Literal[1], None, str, None],
                           ]:
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
            and "project" not in tags
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


def blocks(tw: 'TaskWarrior',
           modified_task: Task,
           orig_task: Optional[Task] = None,
           ) -> Union[tuple[Literal[0], Task, Optional[str], None],
                      tuple[Literal[1], None, str, None],
                      ]:
    blocks = modified_task.get_typed('blocks', str, None)
    if blocks is None:
        return 0, modified_task, None, None

    del modified_task['blocks']

    all_to_block = TaskList()
    for block_entry in blocks.split(','):
        if (_SINGLE_TASK_ID_RE.fullmatch(block_entry)
                or _TASK_ID_RANGE_RE.fullmatch(block_entry)
                or _SHORT_UUID_RE.fullmatch(block_entry)
                or _FULL_UUID_RE.fullmatch(block_entry)
                ):
            to_block = tw.from_taskwarrior(block_entry)
            if not to_block:
                return 1, None, f'Could not find a task matching {block_entry!r} to block', None
            all_to_block.extend(to_block)
        else:
            return 1, None, f'Could not parse {block_entry!r} as a task to block', None

    task_uuid = modified_task.get_typed('uuid', JSONableUUID)
    if len(all_to_block) == 1:
        message = f'Task {all_to_block[0].describe()} blocked by {modified_task.describe()}'
    else:
        message = f'Tasks {", ".join(t.describe() for t in all_to_block[:-1])} and {all_to_block[-1].describe()} blocked by {modified_task.describe()}'

    for t in all_to_block:
        t.add_dependency(task_uuid)

    # For some reason I've not been able to work out, this works here and
    # _doesn't_ work if run as a post-action hook.
    tw.to_taskwarrior(all_to_block)

    return 0, modified_task, message, None


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


def fix_weekday_due(tw: 'TaskWarrior',
                    modified_task: Task,
                    ) -> Union[tuple[Literal[0], Task, Optional[str], None],
                               tuple[Literal[1], None, str, None]]:
    if (modified_task.get_typed('parent', uuid.UUID, None) is None
            or modified_task.get_typed('recurrence', str, None) != 'weekdays'):
        # Nothing to do.
        return 0, modified_task, None, None

    due = modified_task.get_typed('due', datetime.datetime).astimezone()

    if due.weekday() <= 4:
        # The due date is Monday–Friday, so nothing to do.
        return 0, modified_task, None, None

    # TODO This *might* go horribly wrong with a parent task waiting until 1
    # July at 00:00 is created as waiting until 23:00 on 29 February, when this
    # fix means it ends up waiting until 00:00 on 29 February rather than 00:00
    # on 1 March.  That's definitely the case with the other DST wrangling I'm
    # doing in a similar fashion, but I haven't actually checked the problem
    # exists here too.
    if due.weekday() == 6 and due.hour == 23 and due.minute == 59 and due.second == 59:
        # The due date is a Sunday, despite this being a task that supposedly
        # only recurs on weekdays.  That happens because Taskwarrior will
        # create a task due at 00:00:00 on Monday–Friday, which the due_end_of
        # hook will convert to being due at 23:59:59 on Sunday–Thursday.  To
        # fix that, convert the task that's due at 23:59:59 on Sunday to be due
        # that time on a Friday, and modify the other timestamps to match.
        #
        # This can also run into daylight savings problems if the date change
        # goes over a DST clock change.
        modified_task['due'] = new_due = due + relativedelta(days=-2, hour=due.hour)
        assert (due - new_due) <= datetime.timedelta(hours=1)  # TODO Flag when DST has gone wrong per the above todo

        wait = modified_task.get_typed('wait', datetime.datetime, None)
        if wait is not None:
            wait = wait.astimezone()
            modified_task['wait'] = new_wait = wait + relativedelta(days=-2, hour=wait.hour)
            assert (wait - new_wait) <= datetime.timedelta(hours=1)  # TODO Flag when DST has gone wrong per the above todo

        return 0, modified_task, f'Corrected {modified_task.describe()} dates to fix weekday recurrence', None

    # There's also something going on with daylight savings time, although I
    # haven't got my head around it well enough to characterise, so for now
    # just abort with a warning if we end up at this stage with a task that
    # isn't due at 23:59:59 on the Sunday.
    return 1, None, f'Not sure how to handle this due date for weekday recurrence: {due!r}', None


def _do_final_jobs(jobs: Iterable[PostHookAction]) -> NoReturn:
    if jobs:
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
            pass

        parent_pid = os.getppid()
        wait_for_pid(parent_pid)

        for job in jobs:
            job()

    sys.exit(0)


def on_add(tw: 'TaskWarrior',
           hooks: _utils.OneOrMany[OnAddHook]) -> NoReturn:
    task: Optional[Task]
    task = Task.from_json_str(sys.stdin.readline())

    if callable(hooks):
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
              hooks: _utils.OneOrMany[OnModifyHook]) -> NoReturn:
    orig_task = Task.from_json_str(sys.stdin.readline())
    modified_task: Optional[Task]
    modified_task = Task.from_json_str(sys.stdin.readline())

    if not modified_task:
        # The modified task is empty!?  I've only seen that happen when a new
        # task is being removed with `task undo`.  Handle that as a special
        # case, where the only thing I've found that doesn't produce an error
        # is to return the _original_ task.
        print(orig_task.to_json_str())
        sys.exit(0)

    if callable(hooks):
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
