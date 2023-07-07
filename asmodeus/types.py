from collections.abc import Iterable
from typing import ClassVar, Optional, Self, Union, overload
import copy
import datetime
import uuid

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


class Annotation(JSONableDict[JSONable]):
    # Would like _key_class_map to be a ClassVar, but PEP526 says
    # that's not supported.  Looks like the issue is that it's
    # difficult to check, rather than that it's a problem to do this
    # at all.
    _key_class_map: dict[str, type[JSONable]] = {
            'description': JSONableString,
            'entry': JSONableDate}
    _required_keys: ClassVar[tuple[str]] = ('description',)


class AnnotationList(JSONableList[Annotation]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class: type[Annotation] = Annotation


class Task(JSONableDict[JSONable]):
    # Would like _key_class_map and _fallback_class to be ClassVars,
    # but PEP526 says that's not supported.  Looks like the issue is
    # that it's difficult to check, rather than that it's a problem
    # to do this at all.
    _key_class_map: dict[str, type[JSONable]] = {
        'annotations': AnnotationList,
        'depends': JSONableUUIDList,
        'description': JSONableString,
        'due': JSONableDate,
        'end': JSONableDate,
        'entry': JSONableDate,
        'id': JSONableInt,
        'imask': JSONableNumber,  # TODO More structure for this?
        'last': JSONableNumber,  # TODO More structure for this?
        'mask': JSONableString,  # TODO More structure for this
        'modified': JSONableDate,
        'parent': JSONableUUID,
        'priority': JSONableString,  # TODO Make this dynamic
        'project': JSONableString,
        'recur': JSONableString,  # TODO More structure for this
        'recurAfterDue': JSONableDuration,  # TODO Make this dynamic
        'recurAfterModifications': JSONableString,  # TODO Make this dynamic
        'recurAfterWait': JSONableDuration,  # TODO Make this dynamic
        'recurTaskUntil': JSONableDuration,  # TODO Make this dynamic
        'reviewed': JSONableDate,  # TODO Make this dynamic
        'rtype': JSONableString,  # TODO More structure for this
        'scheduled': JSONableDate,
        'start': JSONableDate,
        'status': JSONableString,  # TODO More structure for this
        'source': JSONableString,  # TODO Make this dynamic
        'tags': JSONableStringList,
        'template': JSONableUUID,
        'until': JSONableDate,
        'urgency': JSONableFloat,
        'uuid': JSONableUUID,
        'wait': JSONableDate,
    }
    _required_keys: ClassVar[tuple[str]] = ('description',)
    _fallback_class: type[JSONable] = JSONableAny

    def duplicate(self, reset_as_new: bool = True,
                  reset_uuid: bool = True, reset_deps: bool = True) -> Self:
        if reset_as_new and (not reset_uuid or not reset_deps):
            raise ValueError(
                "Must reset UUID and dependencies "
                "if also resetting as a new task")

        new = copy.deepcopy(self)

        keys_to_reset: Iterable[str]
        if reset_as_new:
            keys_to_reset = ('dependencies', 'end', 'entry', 'modified',
                             'reviewed', 'start', 'status', 'uuid')
        else:
            keys_to_reset = list()
            if reset_uuid:
                keys_to_reset.append('uuid')
            if reset_deps:
                keys_to_reset.append('dependencies')

        for key in keys_to_reset:
            try:
                del new[key]
            except KeyError:
                pass

        return new

    def _gen_uuid(self) -> JSONableUUID:
        return JSONableUUID.uuid4()

    def __getitem__(self, key: str) -> JSONable:
        if key == 'uuid':
            try:
                return super().__getitem__('uuid')
            except KeyError:
                self['uuid'] = new_uuid = self._gen_uuid()
                return new_uuid
        return super().__getitem__(key)

    @overload
    def add_annotation(self, annotation: Annotation) -> None: ...
    @overload
    def add_annotation(self, annotation: str,
                       dt: Optional[datetime.datetime] = None) -> None: ...
    def add_annotation(self, annotation: Annotation | str,
                       dt: Optional[datetime.datetime] = None) -> None:
        if isinstance(annotation, str):
            if dt is None:
                annotation = Annotation({'description': annotation})
            else:
                annotation = Annotation({'description': annotation,
                                         'entry': dt})
        try:
            annotations = self['annotations']
        except KeyError:
            annotations = self['annotations'] = AnnotationList()
        else:
            assert isinstance(annotations, AnnotationList)
        annotations.append(annotation)

    def tag(self, tags: Union[str, Iterable[str]]) -> None:
        try:
            current_tags = self['tags']
        except KeyError:
            self['tags'] = current_tags = JSONableStringList()
        else:
            assert isinstance(current_tags, JSONableStringList)

        if isinstance(tags, str):
            current_tags.append(tags)
        else:
            current_tags.extend(tags)

    def untag(self, tags: Union[str, Iterable[str]]) -> None:
        try:
            current_tags = self['tags']
        except KeyError:
            # If we've been asked to remove zero tags, this is a safe
            # no-op, otherwise raise the KeyError.
            if not isinstance(tags, str) and _utils.is_empty_iter(tags):
                return
            else:
                raise

        assert isinstance(current_tags, JSONableList)

        if isinstance(tags, str):
            current_tags.remove(tags)
        else:
            for tag in tags:
                current_tags.remove(tag)
        if len(current_tags) == 0:
            del self['tags']

    def __setitem__(self, key: str, value: object) -> None:
        if key == 'uuid' and 'uuid' in self:
            raise RuntimeError('Task UUID is immutable once created')
        super().__setitem__(key, value)



class TaskList(JSONableList[Task]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class: type[Task] = Task

    def by_id(self, id_num: int) -> Task:
        if id_num <= 0:
            raise ValueError("Task ID must be greater than 0")
        try:
            return next(t for t in self if t['id'] == id_num)  # type: ignore[comparison-overlap]
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
