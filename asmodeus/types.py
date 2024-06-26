from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, NoReturn, Optional, Union, cast, overload, TYPE_CHECKING
import copy
import datetime
import enum
import sys
import uuid

if sys.version_info >= (3, 11):
    from typing import Self
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import Self

from asmodeus.json import (
        JSONable,
        JSONableDict,
        JSONableList,
        JSONableDate,
        JSONableInt,
        JSONableString,
        JSONableNumber,
        JSONableUUID,
        JSONableUUIDList,
        JSONableStringList,
        JSONableFloat,
        JSONableAny,
        JSONableDuration
        )
import asmodeus._utils as _utils

if TYPE_CHECKING:
    from asmodeus._utils import _SupportsKeysAndGetItem

class NoSuchTagError(ValueError):
    pass


class JSONableUUIDPlaceholder(JSONableUUID):
    _factory: Optional[Callable[[], Optional[uuid.UUID]]]

    # Looks like a JSONableUUID, except for the bit where it won't resolve to a
    # UUID yet.
    #
    # factory should be a function that can be called and will either return a
    # UUID that this object should inhabit, or should return None if it's not
    # okay to generate a UUID yet.
    def __init__(self, factory: Callable[[], Optional[uuid.UUID]]) -> None:
        object.__setattr__(self, '_factory', factory)

    def _populate(self) -> None:
        if self._factory is None:
            # This indicates we've already successfully populated the instance,
            # so there's nothing further to do.
            return

        u = self._factory()
        if u is None:
            # This indicates the factory isn't ready to generate a UUID yet,
            # e.g. because some values it depends on haven't yet been set, so
            # there's nothing further to do.
            return

        # Successfully generated a new UUID, so initiate this instance as if it
        # were that UUID.  This also means the instance can have __hash__ and
        # __setattr__ functions from its parent.
        super().__init__(int=u.int)
        object.__setattr__(self, '_factory', None)

    def __getstate__(self) -> NoReturn:
        raise NotImplementedError(
                'JSONableUUIDPlaceholders cannot yet be pickled')

    def __setstate__(self, state: Any) -> NoReturn:
        raise NotImplementedError(
                'JSONableUUIDPlaceholders cannot yet be unpickled')

    def __repr__(self) -> str:
        self._populate()
        if self._factory is None:
            return f"JSONableUUID('{super().__str__()}')"
        return f'{self.__class__.__name__}({self._factory!r})'

    def __str__(self) -> str:
        self._populate()
        if self._factory is None:
            return super().__str__()
        return repr(self)

    def _json_pre_dump(self) -> str:
        self._populate()
        if self._factory is None:
            return str(self)
        raise RuntimeError('UUID not yet populated')

    def __eq__(self, other: object) -> bool:
        self._populate()
        if self._factory is None:
            return super().__eq__(other)
        return NotImplemented


class Annotation(JSONableDict[Union[JSONableString, JSONableDate]]):
    # Would like _key_class_map to be a ClassVar, but PEP526 says
    # that's not supported.  Looks like the issue is that it's
    # difficult to check, rather than that it's a problem to do this
    # at all.
    _key_map: dict[str, type[Union[JSONableString, JSONableDate]]] = {
            'description': JSONableString,
            'entry': JSONableDate}
    _required_keys: ClassVar[tuple[str]] = ('description',)

    def __init__(self,
                 *args: Union[str,
                              '_SupportsKeysAndGetItem[str, object]',
                              Iterable[tuple[str, object]]],
                 **kwargs: object) -> None:
        if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], str):
            # We've been given a single string as the initialisation parameter,
            # so take that as the annotation description.
            super().__init__(description=args[0])
        else:
            super().__init__(*args, **kwargs)


class AnnotationList(JSONableList[Annotation]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class: type[Annotation] = Annotation


def uuid_init(*args: object, **kwargs: object) -> JSONableUUID:
    if len(args) == 1 and len(kwargs) == 0:
        arg = args[0]
        if isinstance(arg, Task):
            return arg.get_typed('uuid', JSONableUUID)
        if isinstance(arg, JSONableUUID):
            return arg
    return JSONableUUID(*args, **kwargs)


def uuid_list_init(*args: object, **kwargs: object) -> JSONableUUIDList:
    if len(args) == 1 and len(kwargs) == 0:
        arg = args[0]
        if isinstance(arg, JSONableUUIDList):
            return arg
        assert isinstance(arg, Iterable)
        return JSONableUUIDList(map(uuid_init, arg))
    # TODO Maybe fix up this ignore; I *think* I want to not care about types
    # here, and instead police at runtime given that the type checking of
    # things passed to (say) Task doesn't work, but I'm not certain.
    return JSONableUUIDList(*args, **kwargs)  # type: ignore[arg-type]


class ProblemTestResult(enum.Flag):
    ADDED = enum.auto()
    REMOVED = enum.auto()


class Task(JSONableDict[JSONable]):
    # Would like _key_class_map and _fallback_class to be ClassVars,
    # but PEP526 says that's not supported.  Looks like the issue is
    # that it's difficult to check, rather than that it's a problem
    # to do this at all.
    generate_uuid: ClassVar[bool] = False
    _key_map: dict[str,
                   Union[type[JSONable],
                         tuple[type[JSONable],
                               Callable[..., JSONable]]]] = {
        'annotations': AnnotationList,
        'depends': (JSONableUUIDList, uuid_list_init),
        'description': JSONableString,
        'due': JSONableDate,
        'dueRandomDelay': JSONableDuration,  # TODO Make this dynamic
        'end': JSONableDate,
        'entry': JSONableDate,
        'id': JSONableInt,
        'imask': JSONableNumber,  # TODO More structure for this?
        'last': JSONableNumber,  # TODO More structure for this?
        'mask': JSONableString,  # TODO More structure for this
        'modified': JSONableDate,
        'parent': (JSONableUUID, uuid_init),
        'priority': JSONableString,  # TODO Make this dynamic
        'project': JSONableString,
        'recur': JSONableString,  # TODO More structure for this
        'recurAfterDue': JSONableDuration,  # TODO Make this dynamic
        'recurAfterDueRandomDelay': JSONableDuration,  # TODO Make this dynamic
        'recurAfterModifications': JSONableString,  # TODO Make this dynamic
        'recurAfterWait': JSONableDuration,  # TODO Make this dynamic
        'recurAfterWaitRoundDown': JSONableString,  # TODO Make this dynamic
        'recurAfterDueRoundDown': JSONableString,  # TODO Make this dynamic
        'recurAfterWaitRandomDelay': JSONableDuration,  # TODO Make this dynamic
        'recurTaskUntil': JSONableDuration,  # TODO Make this dynamic
        'reviewed': JSONableDate,  # TODO Make this dynamic
        'rtype': JSONableString,  # TODO More structure for this
        'problems': JSONableString,  # TODO Make this dynamic
        'scheduled': JSONableDate,
        'start': JSONableDate,
        'status': JSONableString,  # TODO More structure for this
        'source': JSONableString,  # TODO Make this dynamic
        'tags': JSONableStringList,
        'template': (JSONableUUID, uuid_init),
        'until': JSONableDate,
        'urgency': JSONableFloat,
        'uuid': (JSONableUUID, uuid_init),
        'wait': JSONableDate,
        'waitRandomDelay': JSONableDuration,  # TODO Make this dynamic
        'blocks': JSONableString,  # TODO Make this dynamic
    }
    _required_keys: ClassVar[tuple[str]] = ('description',)
    _fallback: type[JSONable] = JSONableAny

    def __init__(self, *args: object, **kwargs: object):
        super().__init__(*args, **kwargs)
        self._maybe_populate_uuid()

    def _maybe_populate_uuid(self) -> None:
        if 'uuid' not in self and self.generate_uuid:
            self['uuid'] = JSONableUUIDPlaceholder(self._gen_uuid)

    def duplicate(self, reset_as_new: bool = True, reset_id: bool = True,
                  reset_uuid: bool = True, reset_deps: bool = True) -> Self:
        if reset_as_new and (not reset_uuid or not reset_deps or not reset_id):
            raise ValueError(
                "Must reset ID, UUID and dependencies "
                "if also resetting as a new task")

        new = copy.deepcopy(self)

        keys_to_reset: Iterable[str]
        if reset_as_new:
            keys_to_reset = ('dependencies', 'end', 'entry', 'id', 'modified',
                             'reviewed', 'start', 'status', 'urgency', 'uuid')
        else:
            keys_to_reset = list()
            if reset_id:
                keys_to_reset.append('id')
            if reset_uuid:
                keys_to_reset.append('uuid')
            if reset_deps:
                keys_to_reset.append('dependencies')

        for key in keys_to_reset:
            try:
                del new[key]
            except KeyError:
                pass

        new._maybe_populate_uuid()

        return new

    def _gen_uuid(self) -> uuid.UUID:
        return JSONableUUID.uuid4()

    @overload
    def add_annotation(self, annotation: Annotation) -> None: ...

    @overload
    def add_annotation(self, annotation: str,
                       dt: Optional[datetime.datetime] = None) -> None: ...

    def add_annotation(self, annotation: Union[Annotation, str],
                       dt: Optional[datetime.datetime] = None) -> None:
        if isinstance(annotation, str):
            if dt is None:
                annotation = Annotation({'description': annotation})
            else:
                annotation = Annotation({'description': annotation,
                                         'entry': dt})

        try:
            self.get_typed('annotations', AnnotationList).append(annotation)
        except KeyError:
            self['annotations'] = [annotation]

    def tag(self, tags: _utils.OneOrMany[str]) -> None:
        if isinstance(tags, str):
            tags = (tags,)

        try:
            self.get_typed('tags', JSONableStringList).extend(tags)
        except KeyError:
            self['tags'] = tags

    def untag(self, tags: _utils.OneOrMany[str]) -> None:
        if isinstance(tags, str):
            tags = (tags,)

        # Default to an empty list, as this will work as expected in the valid
        # no-op case of attempting to remove zero tags from a task with zero
        # tags.
        current_tags = self.get_typed('tags', JSONableStringList, cast(list[str], []))

        for tag in tags:
            try:
                current_tags.remove(tag)
            except ValueError:
                raise NoSuchTagError(tag)

    def add_dependency(self, uuids: _utils.OneOrMany[uuid.UUID]) -> None:
        if isinstance(uuids, uuid.UUID):
            uuids = (uuids,)

        try:
            self.get_typed('depends', JSONableUUIDList).extend(uuids)
        except KeyError:
            self['depends'] = uuids

    def get_tags(self) -> JSONableStringList:
        tags = self.get_typed('tags', JSONableStringList, None)
        if tags is None:
            tags = self['tags'] = JSONableStringList()
        return tags

    def has_tag(self, tag: str) -> bool:
        return tag in self.get_tags()

    def __setitem__(self, key: str, value: object) -> None:
        if key == 'uuid' and 'uuid' in self:
            raise RuntimeError('Task UUID is immutable once created')
        super().__setitem__(key, value)

    def _get_problems(self) -> list[str]:
        current_problem_str = self.get_typed('problems', str, None)
        if current_problem_str is None:
            return []
        return current_problem_str.split(', ')

    def check_log_problems(self,
                           problems: _utils.OneOrMany['TaskProblem'],
                           ) -> ProblemTestResult:
        if isinstance(problems, TaskProblem):
            problems = (problems,)

        result = ProblemTestResult(0)

        for problem in problems:
            has_problem = problem.test(self)
            current_problems = self._get_problems()
            has_problem_tag = self.has_tag('problems')
            if has_problem and problem.description not in current_problems:
                current_problems.append(problem.description)
                self['problems'] = ', '.join(current_problems)
                if not has_problem_tag:
                    self.tag('problems')
                result |= ProblemTestResult.ADDED
            elif not has_problem and problem.description in current_problems:
                current_problems.remove(problem.description)
                self['problems'] = ', '.join(current_problems)
                if has_problem_tag and len(current_problems) == 0:
                    self.untag('problems')
                result |= ProblemTestResult.REMOVED
        return result

    def describe(self) -> str:
        ident: Union[None, str, int]

        ident = self.get_typed('id', int, 0)

        if ident == 0:
            try:
                u = self.get_typed('uuid', uuid.UUID)
            except KeyError:
                ident = None
            else:
                ident = str(u).split('-', maxsplit=1)[0]

        if ident is None:
            return repr(self['description'])
        else:
            return f'{ident} {self["description"]!r}'


@dataclass
class TaskProblem:
    test: Callable[[Task], bool]
    description: str


class TaskList(JSONableList[Task]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class: type[Task] = Task

    def by_id(self, id_num: int) -> Task:
        if id_num <= 0:
            raise ValueError("Task ID must be greater than 0")
        try:
            return next(t for t in self if t.get_typed('id', int) == id_num)
        except StopIteration:
            raise KeyError(f"No task with ID {id_num}")

    def by_uuid(self, uuid_val: Union[uuid.UUID, str]) -> Task:
        if isinstance(uuid_val, uuid.UUID):
            uuid_obj = uuid_val
        else:
            uuid_obj = uuid.UUID(uuid_val)

        try:
            return next(t for t in self if t['uuid'] == uuid_obj)
        except StopIteration:
            raise KeyError(f"No task with UUID {uuid_val}")
