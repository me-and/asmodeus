from typing import Self, Optional, Union, ClassVar
from collections.abc import Iterable
import datetime
import uuid
import copy

from asmodeus._utils import JSONable, JSONableDict, JSONableList, JSONableDate, JSONableInt, JSONableString, JSONableNumber, JSONableUUID, JSONableUUIDList, JSONableStringList, JSONableFloat, JSONableAny, JSONableDuration, is_empty_iter

class Annotation(JSONableDict[JSONable]):
    _key_class_map: dict[str, type[JSONable]] = {  # Would like this to be a ClassVar, but PEP526 says that's not (yet) supported
            'description': JSONableString, 'entry': JSONableDate}
    _required_keys: ClassVar[tuple[str]] = ('description',)


class AnnotationList(JSONableList[Annotation]):
    _class: type[Annotation] = Annotation  # Would like this to be a ClassVar, but PEP526 says that's not (yet) supported


class Task(JSONableDict[JSONable]):
    _key_class_map: dict[str, type[JSONable]] = {  # Would like this to be a ClassVar, but PEP526 says that's not (yet) supported
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
    _fallback_class: type[JSONable] = JSONableAny  # Would like this to be a ClassVar, but PEP526 says that's not (yet) supported

    def duplicate(self, reset_as_new: bool = True,
                  reset_uuid: bool = True, reset_deps: bool = True) -> Self:
        if reset_as_new and (not reset_uuid or not reset_deps):
            raise ValueError("Must reset UUID and dependencies if also resetting as a new task")

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

    def add_annotation(self, annotation: Annotation) -> None:
        try:
            annotations = self['annotations']
        except KeyError:
            annotations = self['annotations'] = AnnotationList()
        else:
            assert isinstance(annotations, AnnotationList)
        annotations.append(annotation)

    def add_annotation_string(self, annotation_str: str,
                              dt: Optional[datetime.datetime] = None) -> None:
        annotation = Annotation(description=annotation_str)
        if dt is not None:
            annotation['entry'] = dt
        self.add_annotation(annotation)

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
            # If we've been asked to remove zero tags, this is a safe no-op,
            # otherwise raise the KeyError.
            if not isinstance(tags, str) and is_empty_iter(tags):
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
    _class: type[Task] = Task  # Would like this to be a ClassVar, but PEP526 says that's not (yet) supported

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
