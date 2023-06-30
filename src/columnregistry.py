"""Representing TaskWarrior task columns.

Broadly based on the stdlib email.headerregistry module.
"""

from dataclasses import dataclass, InitVar, field
from typing import Optional, Self, Literal, ClassVar, runtime_checkable, overload, NoReturn, TypeVar, Iterable, TypeAlias, Union, reveal_type, Any, TypedDict, Protocol, NotRequired, Generic, get_type_hints, TypeGuard
from collections.abc import MutableSequence
import datetime
import re
from itertools import chain
import uuid
import inspect
import enum
import json

T = TypeVar('T')

BaseJson: TypeAlias = Union[str, int, float, bool, None,
                            list['BaseJson'],
                            dict[str, str],  # TODO Shouldn't be necessary
                            dict[str, 'BaseJson']]
J = TypeVar('J', bound=BaseJson)


class BaseColumn:
    def __new__(cls, name: str, *args: Any, **kwargs: Any) -> Self:
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self._name = name
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, {super().__repr__()})'


class StringColumn(BaseColumn, str):
    def __init__(self, name: str, value: str, *args: Any, **kwargs: Any) -> None:
        return super().__init__(name, *args, **kwargs)

class UDAColumn(BaseColumn):
    pass


class UDAStringColumn(UDAColumn, StringColumn):
    pass


class UDAEnumColumn(UDAStringColumn):
    # TODO How do we add validation etc here?
    pass


class DateColumn(BaseColumn, datetime.datetime):
    def __new__(cls, name: str, datestr: str, *args: Any, **kwargs: Any) -> Self:
        dt = datetime.datetime.fromisoformat(datestr)
        return super().__new__(cls, name, dt.year, dt.month, dt.day, dt.hour,
                               dt.minute, dt.second, tzinfo=dt.tzinfo,
                               fold=dt.fold)

    def __init__(self, name: str, datestr: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(name, *args, **kwargs)

    def __str__(self) -> str:
        return self.isoformat()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, {str(self)!r})'


class UDADateColumn(UDAColumn, DateColumn):
    pass


class UDADurationColumn(UDAColumn, datetime.timedelta):
    # Durations are as defined in src/libshared/src/Duration.cpp.  Notably
    # everything gets normalised to something that resembles a Python timedelta
    # object, e.g. P1M gets normalised to P30D, even though that loses
    # information.

    _norm_re = re.compile(
        r'^P(?:(?P<d>\d+)D)?'
        r'(?:T(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+)S)?)?$')

    def __new__(cls, name: str, durstr: str, *args: Any, **kwargs: Any) -> Self:
        match = cls._norm_re.match(durstr)
        if match is None:
            raise ValueError('Cannot interpret {durstr!r} as a TaskWarrior duration')
        days = int(match['d']) if match['d'] else 0
        hours = int(match['h']) if match['h'] else 0
        minutes = int(match['m']) if match['m'] else 0
        seconds = int(match['s']) if match['s'] else 0
        return super().__new__(cls, name, days=days, hours=hours,
                               minutes=minutes, seconds=seconds, *args,
                               **kwargs)

    def __init__(self, name: str, durstr: str, *args: Any, **kwargs: Any) -> None:
        return super().__init__(name, *args, **kwargs)

    def __str__(self) -> str:
        seconds = int(self.total_seconds())
        if seconds == 0:
            return 'PT0S'
        elif seconds < 0:
            positive = False
            seconds *= -1
        else:
            positive = True
        days, seconds = divmod(seconds, 60 * 60 * 24)
        hours, seconds = divmod(seconds, 60 * 60)
        minutes, seconds = divmod(seconds, 60)
        parts = ['P']
        if days:
            if positive:
                parts.append(f'{days}D')
            else:
                parts.append(f'-{days}D')
        if hours or minutes or seconds:
            parts.append('T')
            if hours:
                if positive:
                    parts.append(f'{hours}H')
                else:
                    parts.append(f'-{hours}H')
            if minutes:
                if positive:
                    parts.append(f'{minutes}M')
                else:
                    parts.append(f'-{minutes}M')
            if seconds:
                if positive:
                    parts.append(f'{seconds}S')
                else:
                    parts.append(f'-{seconds}S')
        return ''.join(parts)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, {str(self)!r})'


class NumericColumn(BaseColumn, float):
    def __init__(self, name: str, num: float, *args: Any, **kwargs: Any) -> None:
        self._int: Optional[int]
        if isinstance(num, int):
            self._int = num
        else:
            self._int = None
        super().__init__(name, *args, **kwargs)

    def __str__(self) -> str:
        if self._int is not None:
            return str(self._int)
        return str(float(self))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, {self})'


# Would like to inherit from NumericColumn, but I can't work out a sensible way
# to do that.  But it should duck-type just the same at least.
class IntColumn(BaseColumn, int):
    def __init__(self, name: str, num: int, *args: Any, **kwargs: Any) -> None:
        self._int = num
        super().__init__(name, *args, **kwargs)

    __str__ = NumericColumn.__str__
    __repr__ = NumericColumn.__repr__


class FloatColumn(NumericColumn):
    def __init__(self, name: str, num: float, *args: Any, **kwargs: Any) -> None:
        self._int = None
        super().__init__(name, num, *args, **kwargs)


class UDANumericColumn(NumericColumn, UDAColumn):
    pass


class UDAOrphanColumn(UDAStringColumn):
    def __new__(cls, name: str, value: BaseJson, *args: Any, **kwargs: Any) -> Self:
        return super().__new__(cls, name, str(value), *args, **kwargs)

    def __init__(self, name: str, value: BaseJson, *args: Any, **kwargs: Any) -> None:
        return super().__init__(name, *args, **kwargs)


@dataclass
class Annotation:
    description: str
    entry: Optional[datetime.datetime] = None

    def as_dict(self) -> dict[str, str]:
        d = {'description': self.description}
        if self.entry is not None:
            d['entry'] = self.entry.isoformat()
        return d

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}.from_dict({self.as_dict()!r})'

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> Self:
        if 'entry' in d:
            return cls(d['description'],
                       datetime.datetime.fromisoformat(d['entry']))
        return cls(d['description'])


# TODO ABC with the mappping function from J to T as a method that needs to be
# overridden?
class BaseListColumn(BaseColumn, list[T], Generic[T, J]):
    @staticmethod
    def parse(j: J) -> T:
        raise NotImplementedError

    def __new__(cls, name: str, value: Iterable[J] = (), *args: Any, **kwargs: Any) -> Self:
        return super().__new__(cls, name, *args, **kwargs)

    def __init__(self, name: str, value: Iterable[J] = (), *args: Any, **kwargs: Any) -> None:
        super().__init__(name, map(self.parse, value), *args, **kwargs)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, [{", ".join(repr(i) for i in self)}])'


class IdentityListColumn(BaseListColumn[J, J], Generic[J]):
    @staticmethod
    def parse(obj: J) -> J:
        return obj


class StringListColumn(IdentityListColumn[str]):
    pass


class AnnotationListColumn(BaseListColumn[Annotation, dict[str, str]]):
    parse = staticmethod(Annotation.from_dict)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, [{", ".join(repr(a.as_dict()) for a in self)}])'



class UUIDColumn(BaseColumn, uuid.UUID):
    def __new__(cls, name: str, uuidstr: str, *args: Any, **kwargs: Any) -> Self:
        return super().__new__(cls, name, *args, **kwargs)

    # Override uuid.UUID.__setattr__ because not all parts of the UUID need to
    # be immutable.
    def __setattr__(self, attr: str, value: object) -> None:
        if attr == '_name':
            BaseColumn.__setattr__(self, attr, value)
        else:
            uuid.UUID.__setattr__(self, attr, value)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, {str(self)!r})'


class UUIDListColumn(BaseListColumn[uuid.UUID, str]):
    parse = staticmethod(uuid.UUID)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.name!r}, [{", ".join(repr(str(u)) for u in self)}])'


class RecurrencePeriod(str):
    # Recurrence periods include all durations, but they don't get normalised;
    # e.g. "P1M" or "monthly" get stored exactly like that, and get used for
    # actual month-by-month calculations rather than assuming a month is always
    # exactly 30 days as happens with UDA duration values.
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({super().__repr__()})'


_default_column_map: dict[str, type[BaseColumn]] = {
    'annotations': AnnotationListColumn,
    'depends': UUIDListColumn,
    'description': StringColumn,
    'due': DateColumn,
    'end': DateColumn,
    'entry': DateColumn,
    'id': IntColumn,
    'imask': NumericColumn,  # TODO More structure for this?
    'last': NumericColumn,  # TODO More structure for this?
    'mask': StringColumn,  # TODO More structure for this
    'modified': DateColumn,
    'parent': UUIDColumn,
    'priority': UDAEnumColumn,  # TODO Make this dynamic
    'project': StringColumn,
    'recur': StringColumn,  # TODO More structure for this
    'rtype': StringColumn,  # TODO More structure for this
    'scheduled': DateColumn,
    'start': DateColumn,
    'status': StringColumn,  # TODO More structure for this
    'tags': StringListColumn,
    'template': UUIDColumn,
    'until': DateColumn,
    'urgency': FloatColumn,
    'uuid': UUIDColumn,
    'wait': DateColumn,
}

@dataclass
class ColumnRegistry:
    """Column factory and registry."""

    column_map: dict[str, type[BaseColumn]] = field(default_factory=dict)
    default_class: type[BaseColumn] = UDAOrphanColumn
    use_default_map: InitVar[bool] = True
    fetch_udas: InitVar[bool] = True

    def __init__(self,
                 column_map: Optional[dict[str, type[BaseColumn]]] = None,
                 default_class: type[BaseColumn] = UDAOrphanColumn,
                 use_default_map: bool = True, fetch_udas: bool = True
                 ) -> None:
        self.default_class = default_class

        self.column_map: dict[str, type[BaseColumn]] = {}
        if use_default_map:
            self.column_map |= _default_column_map
        if fetch_udas:
            self.column_map |= self._fetch_udas()
        if column_map is not None:
            self.column_map |= column_map

    def __getitem__(self, name: str) -> type[BaseColumn]:
        cls = self.column_map.get(name, self.default_class)
        return type('_'.join(('', cls.__name__, name)), (cls,), {})

    def __call__(self, name: str, value: BaseJson) -> BaseColumn:
        """Create a column instance for column "name" with value "value".
        """
        return self[name](name, value)

    @classmethod
    def _fetch_udas(cls) -> dict[str, type[UDAColumn]]:
        d: dict[str, type[UDAColumn]] = {}
        raise NotImplementedError("Need to add call to `task _show`, + parsing")
        for name, kind in ():
            if kind == 'string':
                # TODO Handle UDAEnumColumn if uda.<name>.values exists
                d[name] = UDAStringColumn
            elif kind == 'duration':
                d[name] = UDADurationColumn
            elif kind == 'date':
                d[name] = UDADateColumn
            elif kind == 'numeric':
                d[name] = UDANumericColumn
            else:
                raise ValueError(f'Unexpected UDA type {kind}')
