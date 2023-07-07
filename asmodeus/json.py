from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import (Any, ClassVar, Generic, Optional, SupportsIndex,
                    TYPE_CHECKING, TypeVar, Union, cast, overload)
import datetime
import json
import re
import subprocess
import sys
import uuid

if sys.version_info >= (3, 11):
    from typing import Self, TypeAlias, TypeGuard
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import Self, TypeAlias, TypeGuard

if TYPE_CHECKING:
    from asmodeus._utils import _SupportsKeysAndGetItem

JSONVal: TypeAlias = Union[None, bool, str, int, float,
                           list['JSONVal'], dict[str, 'JSONVal']]

T = TypeVar('T')
Va = TypeVar('Va')
Vb = TypeVar('Vb')

SEMAPHORE = object()


def is_json_val(obj: Any) -> TypeGuard[JSONVal]:
    if (obj is None or
            isinstance(obj, int) or
            isinstance(obj, float) or
            isinstance(obj, bool)):
        return True
    if isinstance(obj, dict):
        return all(isinstance(key, str) and is_json_val(value)
                   for key, value in obj.items())
    if isinstance(obj, list):
        return all(is_json_val(item) for item in obj)
    return False


JSONValImmut: TypeAlias = Union[None, bool, str, int, float,
                                Sequence['JSONValImmut'],
                                Mapping[str, 'JSONValImmut']]


def is_json_val_immut(obj: Any) -> TypeGuard[JSONValImmut]:
    if (obj is None or
            isinstance(obj, int) or
            isinstance(obj, float) or
            isinstance(obj, bool)):
        return True
    if isinstance(obj, Mapping):
        return all(isinstance(key, str) and is_json_val_immut(value)
                   for key, value in obj.items())
    if isinstance(obj, Sequence):
        return all(is_json_val_immut(item) for item in obj)
    return False


class JSONable(ABC):
    @abstractmethod
    def _json_pre_dump(self) -> JSONValImmut: ...
    @classmethod
    @abstractmethod
    def from_json_val(cls, j: JSONValImmut) -> Self: ...

    @staticmethod
    def _json_dumper(obj: object) -> JSONValImmut:
        if isinstance(obj, JSONable):
            return obj._json_pre_dump()
        raise TypeError(f"Can't convert {obj!r} to json")

    def to_json_str(self) -> str:
        return json.dumps(self, default=self._json_dumper)

    @classmethod
    def from_json_str(cls, string: str) -> Self:
        # Safe to use cast as json.loads can only return these limited
        # types; we know that's true because we know what parser the
        # json library is using, which mypy isn't able to infer.
        return cls.from_json_val(cast(JSONVal, json.loads(string)))


# TODO Could this be changed to actually allow base string/int/float
# values?
Jb = TypeVar('Jb', bound=JSONable)


class JSONableString(str, JSONable):
    def __init__(self, value: str,
                 *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _json_pre_dump(self) -> str:
        return self

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, str)
        return cls(j)


class JSONableDate(datetime.datetime, JSONable):

    @overload
    def __new__(cls, year_or_str_or_dt: datetime.datetime) -> Self: ...

    @overload
    def __new__(cls, year_or_str_or_dt: str) -> Self: ...

    @overload
    def __new__(cls, year_or_str_or_dt: int, month: int, day: int, hour: int,
                minute: int, second: int, microsecond: int,
                tzinfo: Optional[datetime.tzinfo], *, fold: int
                ) -> Self: ...

    def __new__(cls, year_or_str_or_dt: Union[int, str, datetime.datetime],
                month: Optional[int] = None, day: Optional[int] = None,
                hour: int = 0, minute: int = 0, second: int = 0,
                microsecond: int = 0,
                tzinfo: Optional[datetime.tzinfo] = None,
                *, fold: int = 0) -> Self:
        if isinstance(year_or_str_or_dt, int):
            # Looks like a year, so emulate the normal
            # datetime.datetime interface.
            assert month is not None
            assert day is not None
            return super().__new__(cls, year_or_str_or_dt, month, day, hour,
                                   minute, second, microsecond, tzinfo=tzinfo,
                                   fold=fold)
        if isinstance(year_or_str_or_dt, str):
            # It's a string.  If it's in ISO format, injest it
            # directly, otherwise see what TaskWarrior's calc function
            # makes of it.
            try:
                dt = datetime.datetime.fromisoformat(year_or_str_or_dt)
            except ValueError:
                # TODO This shouldn't be duplicating code in
                # TaskWarrior.calc_datetime, but I wanted a quick
                # solution rather than a good one...
                p = subprocess.run(('task', 'rc.verbose=nothing',
                                    'rc.date.iso=yes', 'calc',
                                    year_or_str_or_dt),
                                   stdout=subprocess.PIPE,
                                   check=True, encoding='utf-8')
                dt = datetime.datetime.fromisoformat(p.stdout.strip())
        else:
            # Must already be a datetime.datetime.
            dt = year_or_str_or_dt

        return super().__new__(cls, dt.year, dt.month, dt.day, dt.hour,
                               dt.minute, dt.second, dt.microsecond,
                               tzinfo=dt.tzinfo, fold=dt.fold)

    def _json_pre_dump(self) -> str:
        as_utc = self.astimezone(datetime.timezone.utc)
        return as_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    def __str__(self) -> str:
        as_local = self.astimezone()
        return as_local.strftime('%d %b %Y %H:%M:%S %Z')

    def __copy__(self) -> Self:
        return self.__class__(self.year, self.month, self.day, self.hour,
                              self.minute, self.second, self.microsecond,
                              tzinfo=self.tzinfo, fold=self.fold)

    def __deepcopy__(self, memo: Any) -> Self:
        return self.__copy__()

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, str)
        return cls(j)


class JSONableDuration(datetime.timedelta, JSONable):
    # Durations are as defined in src/libshared/src/Duration.cpp.
    # Notably everything gets normalised to something that resembles a
    # Python timedelta object, e.g. P1M gets normalised to P30D, even
    # though that loses information.

    @overload
    def __new__(cls, days_or_str_or_td: datetime.timedelta) -> Self: ...

    @overload
    def __new__(cls, days_or_str_or_td: str) -> Self: ...

    @overload
    def __new__(cls, days_or_str_or_td: int, seconds: int, microseconds: int,
                milliseconds: int, minutes: int, hours: int, weeks: int
                ) -> Self: ...

    def __new__(cls,
                days_or_str_or_td: Union[int, str, datetime.timedelta] = 0,
                seconds: int = 0, microseconds: int = 0,
                milliseconds: int = 0, minutes: int = 0, hours: int = 0,
                weeks: int = 0) -> Self:
        if isinstance(days_or_str_or_td, int):
            # Emulate the normal datetime.timedelta interface.
            return super().__new__(cls, days_or_str_or_td, seconds,
                                   microseconds, milliseconds, minutes, hours,
                                   weeks)
        if isinstance(days_or_str_or_td, str):
            match = cls._norm_re.match(days_or_str_or_td)
            if match is None:
                raise ValueError('Cannot interpret {durstr!r}')
            days = int(match['d']) if match['d'] else 0
            hours = int(match['h']) if match['h'] else 0
            minutes = int(match['m']) if match['m'] else 0
            seconds = int(match['s']) if match['s'] else 0
            return super().__new__(cls, days=days, hours=hours,
                                   minutes=minutes, seconds=seconds)
        # Must be a datetime.timedelta.
        return super().__new__(cls, days=days_or_str_or_td.days,
                               seconds=days_or_str_or_td.seconds,
                               microseconds=days_or_str_or_td.microseconds)

    def __str__(self) -> str:
        seconds = int(self.total_seconds())

        if seconds == 0:
            return 'PT0S'

        if seconds < 0:
            negative = True
            seconds = abs(seconds)
        else:
            negative = False

        days, seconds = divmod(seconds, 60 * 60 * 24)
        hours, seconds = divmod(seconds, 60 * 60)
        minutes, seconds = divmod(seconds, 60)
        parts = ['P']

        if days:
            if negative:
                parts.append('-')
            parts.append(str(days))
            parts.append('D')

        if hours or minutes or seconds:
            parts.append('T')
            for name, num in (('H', hours), ('M', minutes), ('S', seconds)):
                if num:
                    if negative:
                        parts.append('-')
                    parts.append(str(num))
                    parts.append(name)

        return ''.join(parts)

    def _json_pre_dump(self) -> str:
        return str(self)

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, str)
        return cls(j)

    _norm_re = re.compile(
        r'^P(?:(?P<d>\d+)D)?'
        r'(?:T(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+)S)?)?$')


class JSONableNumber(float, JSONable):
    _int: Optional[int]

    def __init__(self, num: float, *args: object, **kwargs: object) -> None:
        if isinstance(num, int):
            self._int = num
        else:
            self._int = None
        super().__init__(*args, **kwargs)

    def _json_pre_dump(self) -> Union[int, float]:
        if self._int is not None:
            return self._int
        return self

    def __repr__(self) -> str:
        if self._int is not None:
            return str(self._int)
        return super().__repr__()

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, float) or isinstance(j, int)
        return cls(j)


class JSONableInt(int, JSONable):
    def _json_pre_dump(self) -> int:
        return self

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, int)
        return cls(j)


class JSONableFloat(float, JSONable):
    def __init__(self, num: float,
                 *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def _json_pre_dump(self) -> float:
        return self

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, float)
        return cls(j)


class JSONableDict(dict[str, Jb], JSONable, Generic[Jb], ABC):
    # Would like _key_class_map and _fallback_class to be ClassVars,
    # but PEP526 says that's not supported.  Looks like the issue is
    # that it's difficult to check, rather than that it's a problem
    # to do this at all.
    _key_class_map: Mapping[str, type[Jb]]
    _required_keys: ClassVar[Iterable[str]] = ()
    _fallback_class: Optional[type[Jb]] = None

    @overload
    def __init__(self) -> None: ...

    @overload
    def __init__(self, **kwargs: object) -> None: ...

    @overload
    def __init__(self, __map: '_SupportsKeysAndGetItem[str, object]'
                 ) -> None: ...

    @overload
    def __init__(self, __map: '_SupportsKeysAndGetItem[str, object]',
                 **kwargs: object) -> None: ...

    @overload
    def __init__(self, __it: Iterable[tuple[str, object]]) -> None: ...

    @overload
    def __init__(self, __it: Iterable[tuple[str, object]],
                 **kwargs: object) -> None: ...

    def __init__(self, *args: Union['_SupportsKeysAndGetItem[str, object]',
                                    Iterable[tuple[str, object]]],
                 **kwargs: object) -> None:
        processed_kwargs: dict[str, Jb] = {
            key: self._parse_if_needed(key, value)
            for key, value in kwargs.items()}
        if len(args) == 0:
            super().__init__(**processed_kwargs)
        elif len(args) == 1:
            arg = args[0]
            try:
                keys = arg.keys()  # type: ignore[union-attr]
            except AttributeError:
                # Must be an iterable of tuples.
                arg = cast(Iterable[tuple[str, object]], arg)
                processed_arg = ((key, self._parse_if_needed(key, value))
                                 for key, value in arg)
            else:
                arg = cast('_SupportsKeysAndGetItem[str, object]', arg)
                processed_arg = ((key, self._parse_if_needed(key, arg[key]))
                                 for key in keys)
            super().__init__(processed_arg, **processed_kwargs)
        else:
            raise TypeError(
                f'{self.__class__.__name__} accepts at most 1 positional '
                'argument')

    @classmethod
    def _get_class(cls, key: str) -> type[Jb]:
        if cls._fallback_class is None:
            return cls._key_class_map[key]
        return cls._key_class_map.get(key, cls._fallback_class)

    @classmethod
    def _parse_if_needed(cls, key: str,
                         *args: object, **kwargs: object) -> Jb:
        if (len(args) == 1 and len(kwargs) == 0 and
                isinstance((arg := args[0]), cls._get_class(key))):
            return arg
        return cls._get_class(key)(*args, **kwargs)

    def _json_pre_dump(self) -> Mapping[str, JSONValImmut]:
        return {k: v._json_pre_dump() for k, v in self.items()}

    def _has_required_keys(self) -> bool:
        return all(k in self for k in self._required_keys)

    def __setitem__(self, key: str, value: object) -> None:
        super().__setitem__(key, self._parse_if_needed(key, value))

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, Mapping)
        return cls(j)

    @overload
    def get_typed(self, key: str, kind: type[Va]) -> Va: ...

    @overload
    def get_typed(self, key: str, kind: type[Va],
                  default: Vb) -> Union[Va, Vb]: ...

    def get_typed(self, key: str, kind: type[Va],
                  default: object = SEMAPHORE) -> Union[Va, object]:
        try:
            value = self[key]
        except KeyError:
            if default is SEMAPHORE:
                raise
            return default
        assert isinstance(value, kind), f"{key!r} isn't a {kind.__qualname__}"
        return value


class JSONableList(list[Jb], JSONable, Generic[Jb], ABC):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class: type[Jb]

    def __init__(self, it: Optional[Iterable[object]] = None) -> None:
        if it is None:
            super().__init__()
        else:
            super().__init__(map(self._parse_if_needed, it))

    @classmethod
    def _parse_if_needed(cls, *args: object, **kwargs: object) -> Jb:
        if (len(args) == 1 and len(kwargs) == 0 and
                isinstance((arg := args[0]), cls._class)):
            return cast(Jb, arg)
        return cast(Jb, cls._class(*args, **kwargs))

    def _json_pre_dump(self) -> Sequence[JSONValImmut]:
        return [v._json_pre_dump() for v in self]

    @overload
    def __setitem__(self, key: SupportsIndex,
                    value: object) -> None: ...

    @overload
    def __setitem__(self, key: slice,
                    value: Iterable[object]) -> None: ...

    def __setitem__(self, key: Union[SupportsIndex, slice],
                    value: Union[object, Iterable[object]]
                    ) -> None:
        if isinstance(key, SupportsIndex):
            value = cast(object, value)
            super().__setitem__(key, self._parse_if_needed(value))
        else:
            value = cast(Iterable[object], value)
            super().__setitem__(key, map(self._parse_if_needed, value))

    def append(self, value: object) -> None:
        super().append(self._parse_if_needed(value))

    def extend(self, value: Iterable[object]) -> None:
        super().extend(map(self._parse_if_needed, value))

    def remove(self, obj: object) -> None:
        super().remove(self._parse_if_needed(obj))

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, Sequence)
        return cls(j)


class JSONableUUID(uuid.UUID, JSONable):
    def _json_pre_dump(self) -> str:
        return str(self)

    @classmethod
    def uuid1(cls, node: Optional[int], clock_seq: Optional[int]) -> Self:
        return cls(int=uuid.uuid1(node, clock_seq).int)

    @classmethod
    def uuid3(cls, namespace: uuid.UUID, name: str) -> Self:
        return cls(int=uuid.uuid3(namespace, name).int)

    @classmethod
    def uuid4(cls) -> Self:
        return cls(int=uuid.uuid4().int)

    @classmethod
    def uuid5(cls, namespace: uuid.UUID, name: str) -> Self:
        return cls(int=uuid.uuid5(namespace, name).int)

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        assert isinstance(j, str)
        return cls(j)


class JSONableUUIDList(JSONableList[JSONableUUID]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class = JSONableUUID


class JSONableStringList(JSONableList[JSONableString]):
    # Would like _class to be a ClassVar, but PEP526 says that's not
    # supported.  Looks like the issue is that it's difficult to check,
    # rather than that it's a problem to do this at all.
    _class = JSONableString


@dataclass
class JSONableAny(JSONable):
    _data: JSONValImmut

    def _json_pre_dump(self) -> JSONValImmut:
        return self._data

    @classmethod
    def from_json_val(cls, j: JSONValImmut) -> Self:
        return cls(j)
