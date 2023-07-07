from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import chain
from typing import Optional, TypeAlias, Union
import datetime
import os
import subprocess
import uuid

from asmodeus.json import JSONableUUID
from asmodeus.types import Annotation, Task, TaskList

StrPath: TypeAlias = Union[str, os.PathLike]

@dataclass
class TaskWarrior:
    executable: StrPath = 'task'

    def calc(self, statement: str) -> str:
        p = subprocess.run((self.executable, 'rc.verbose=nothing',
                            'rc.date.iso=yes', 'calc', statement),
                           stdout=subprocess.PIPE, check=True, encoding='utf-8')
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

    def to_taskwarrior(self, tasks: Union[Task, TaskList, Iterable[Task]]) -> None:
        if isinstance(tasks, Task) or isinstance(tasks, TaskList):
            json_str = tasks.to_json_str()
        else:
            json_str = '[' + ','.join(task.to_json_str() for task in tasks) + ']'
        subprocess.run((self.executable, 'rc.verbose=nothing', 'import', '-'),
                       input=json_str,
                       encoding='utf-8', check=True)

    def from_taskwarrior(self, filter_args: Optional[Iterable[str]] = None
                         ) -> TaskList:
        args: Sequence[StrPath]
        if filter_args is None:
            args = (self.executable, 'rc.verbose=nothing', 'export')
        else:
            args = tuple(chain((self.executable, 'rc.verbose=nothing'),
                               filter_args, ('export',)))
        p = subprocess.run(args, stdout=subprocess.PIPE, encoding='utf-8',
                           check=True)
        return TaskList.from_json_str(p.stdout)

    def cmdline_add(self, args: Iterable[str]) -> JSONableUUID:
        p = subprocess.run(((self.executable, 'rc.verbose=new-uuid', 'add') +
                            tuple(args)),
                           stdout=subprocess.PIPE, check=True, encoding='utf-8')
        new_uuid: Optional[JSONableUUID] = None
        for line in p.stdout.split('\n'):
            if line.startswith('Created task ') and line.endswith('.'):
                if new_uuid is not None:
                    ex = RuntimeError('Unexpectedly multiple task UUIDs in "task add" output')
                    ex.add_note(p.stdout)
                    raise ex
                new_uuid = JSONableUUID(line.removeprefix('Created task ').removesuffix('.'))
        if new_uuid is None:
            ex = RuntimeError('Unexpectedly now task UUIDs in "task add" output')
            ex.add_note(p.stdout)
            raise ex

        return new_uuid

    def get_task(self, u: uuid.UUID) -> Task:
        task_list = self.from_taskwarrior((str(u),))
        match len(task_list):
            case 0:
                raise RuntimeError(f'Found no tasks with UUID {u}')
            case 1:
                task = task_list[0]
                assert isinstance(task, Task)
                return task
            case n if n <= 3:
                ex = RuntimeError(f'Found {n} tasks with UUID {u}')
                for t in task_list:
                    ex.add_note(repr(t))
                raise ex
            case n:
                ex = RuntimeError(f'Found {n} tasks with UUID {u}')
                for t, _ in zip(task_list, range(3)):
                    ex.add_note(repr(t))
                ex.add_note('...')
                raise ex
        # Needed to avoid mypy raising a "missing return statement" error
        raise RuntimeError('This should be unreachable!')

    def annotate_task(self, u: uuid.UUID, a: Annotation) -> None:
        task = self.get_task(u)
        task.add_annotation(a)
        self.to_taskwarrior(task)
