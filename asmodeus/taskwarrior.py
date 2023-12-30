from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import IO, Literal, Optional, Union, overload
import datetime
import os
import subprocess
import sys
import uuid

if sys.version_info >= (3, 11):
    from typing import TypeAlias
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import TypeAlias

from asmodeus.json import JSONableUUID
from asmodeus.types import Annotation, Task, TaskList
import asmodeus._utils as _utils

StrPath: TypeAlias = Union[str, os.PathLike]


class TaskCountError(RuntimeError):
    def __init__(self, *args: object, task_count: int, **kwargs: object) -> None:
        self.task_count = task_count
        super().__init__(*args, **kwargs)


@dataclass
class TaskWarrior:
    executable: StrPath = 'task'

    @overload
    def exec(self,
             *command: str,
             verbose: Union[None, str, int] = 'nothing',
             gc: Optional[bool] = False,
             date_iso: Optional[bool] = True,
             color: Optional[bool] = False,
             detection: Optional[bool] = False,
             hooks: Optional[bool] = False,
             recurrence: Optional[bool] = False,
             check: bool = True,
             input: Optional[str] = None,
             output: Literal['return'],
             ) -> str: ...
    @overload
    def exec(self,
             *command: str,
             verbose: Union[None, str, int] = 'nothing',
             gc: Optional[bool] = False,
             date_iso: Optional[bool] = True,
             color: Optional[bool] = False,
             detection: Optional[bool] = False,
             hooks: Optional[bool] = False,
             recurrence: Optional[bool] = False,
             check: bool = True,
             input: Optional[str] = None,
             output: Literal['pass'] = 'pass',
             ) -> None: ...
    def exec(self,
             *command: str,
             verbose: Union[None, str, int] = 'nothing',
             gc: Optional[bool] = False,
             date_iso: Optional[bool] = True,
             color: Optional[bool] = False,
             detection: Optional[bool] = False,
             hooks: Optional[bool] = False,
             recurrence: Optional[bool] = False,
             check: bool = True,
             input: Optional[str] = None,
             output: Literal['return', 'pass'] = 'pass',
             ) -> Optional[str]:
        args: list[StrPath] = [self.executable]

        if verbose is not None:
            args.append(f'rc.verbose={verbose}')

        if gc is not None:
            if gc: args.append('rc.gc=1')
            else: args.append('rc.gc=0')

        if date_iso is not None:
            if date_iso: args.append('rc.date.iso=1')
            else: args.append('rc.date.iso=0')

        if color is not None:
            if color: args.append('rc.color=1')
            else: args.append('rc.color=0')

        if detection is not None:
            if detection: args.append('rc.detection=1')
            else: args.append('rc.detection=0')

        if hooks is not None:
            if hooks: args.append('rc.hooks=1')
            else: args.append('rc.hooks=0')

        if recurrence is not None:
            if recurrence: args.append('rc.recurrence=1')
            else: args.append('rc.recurrence=0')

        args.extend(command)

        if output == 'return':
            stdout = subprocess.PIPE
        else:
            stdout = None

        p = subprocess.run(args,
                           stdout=stdout,
                           check=check,
                           encoding='utf-8',
                           input=input,
                           )

        if output == 'return':
            return p.stdout.strip()
        else:
            return None

    def calc(self, statement: str) -> str:
        return self.exec('calc', statement, output='return')

    def calc_datetime(self, statement: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.calc(statement))

    def calc_bool(self, statement: str) -> bool:
        result = self.calc(statement)
        if result == 'true':
            return True
        if result == 'false':
            return False
        raise RuntimeError(f"{result!r} neither 'false' nor 'true'")

    def to_taskwarrior(self, tasks: Union[Task, TaskList, Iterable[Task]]
                       ) -> None:
        if isinstance(tasks, Task) or isinstance(tasks, TaskList):
            json_str = tasks.to_json_str()
        else:
            json_str = ('[' +
                        ','.join(task.to_json_str() for task in tasks) +
                        ']')
        self.exec('import', '-', hooks=True, input=json_str)

    def from_taskwarrior(self, filter_args: _utils.OneOrMany[str] = ()
                         ) -> TaskList:
        args: Sequence[str]
        if isinstance(filter_args, str):
            filter_args = (filter_args,)

        return TaskList.from_json_str(
                    self.exec(*filter_args, 'export',
                              output='return',
                              ))

    def get_task(self, u: uuid.UUID) -> Task:
        task_list = self.from_taskwarrior((str(u),))
        count = len(task_list)
        if count == 0:
            raise TaskCountError(f'Found no tasks with UUID {u}', task_count=0)
        elif count == 1:
            task = task_list[0]
            return task
        else:
            ex = TaskCountError(f'Found {count} tasks with UUID {u}', task_count=count)
            if sys.version_info >= (3, 11):
                for t in islice(task_list, 3):
                    ex.add_note(repr(t))
                if count > 3:
                    ex.add_note('...')
            raise ex

    def get_dom(self, ref: str) -> str:
        return self.exec('_get', ref,
                         output='return',
                         )
