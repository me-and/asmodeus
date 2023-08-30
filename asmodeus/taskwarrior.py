from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import Optional, Union
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

    def calc(self, statement: str) -> str:
        p = subprocess.run((self.executable,
                            'rc.verbose=nothing',
                            'rc.gc=0',
                            'rc.date.iso=yes',
                            'calc',
                            statement,
                            ),
                           stdout=subprocess.PIPE,
                           check=True,
                           encoding='utf-8',
                           )
        return p.stdout.strip()

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
        subprocess.run((self.executable,
                        'rc.verbose=nothing',
                        'rc.gc=0',
                        'import',
                        '-',
                        ),
                       input=json_str,
                       encoding='utf-8',
                       check=True,
                       )

    def from_taskwarrior(self, filter_args: _utils.OneOrMany[str] = ()
                         ) -> TaskList:
        args: Sequence[StrPath]
        if isinstance(filter_args, str):
            filter_args = (filter_args,)

        args = (self.executable,
                'rc.verbose=nothing',
                'rc.gc=0',
                'rc.hooks=0',
                *filter_args,
                'export',
                )
        p = subprocess.run(args,
                           stdout=subprocess.PIPE,
                           encoding='utf-8',
                           check=True,
                           )
        return TaskList.from_json_str(p.stdout)

    def cmdline_add(self, args: Iterable[str]) -> JSONableUUID:
        p = subprocess.run(((self.executable,
                             'rc.verbose=new-uuid',
                             'rc.gc=0',
                             'add',
                             )
                            + tuple(args)
                            ),
                           stdout=subprocess.PIPE,
                           check=True,
                           encoding='utf-8')
        new_uuid: Optional[JSONableUUID] = None
        for line in p.stdout.split('\n'):
            if line.startswith('Created task ') and line.endswith('.'):
                if new_uuid is not None:
                    ex = RuntimeError(
                        'Unexpectedly multiple task UUIDs in '
                        '"task add" output')
                    if sys.version_info >= (3, 11):
                        ex.add_note(p.stdout)
                    raise ex
                uuid_str = line.removeprefix('Created task ').removesuffix('.')
                new_uuid = JSONableUUID(uuid_str)
        if new_uuid is None:
            ex = RuntimeError(
                    'Unexpectedly no task UUIDs in "task add" output')
            if sys.version_info >= (3, 11):
                ex.add_note(p.stdout)
            raise ex

        return new_uuid

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

    def annotate_task(self, u: uuid.UUID, a: Annotation) -> None:
        task = self.get_task(u)
        task.add_annotation(a)
        self.to_taskwarrior(task)

    def get_dom(self, ref: str) -> str:
        p = subprocess.run((self.executable,
                            'rc.gc=0',
                            '_get',
                            ref,
                            ),
                           stdout=subprocess.PIPE,
                           check=True,
                           encoding='utf-8',
                           )
        return p.stdout.strip()
