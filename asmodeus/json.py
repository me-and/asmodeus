from abc import ABC, abstractmethod
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import (Any, Callable, ClassVar, Generic, Optional, NewType,
                    SupportsIndex, TYPE_CHECKING, TypeVar, Union, cast,
                    overload)
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

Va = TypeVar('Va')
Vb = TypeVar('Vb')

Semaphore = NewType('Semaphore', object)
SEMAPHORE = Semaphore(object())


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


# TODO See if I can find some way to add typechecking to the things that
# JSONable subclasses will injest; at the moment there's no checking in the
# type checker to prevent, e.g., Task({'description': object()})
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
        return json.dumps(self, default=self._json_dumper, ensure_ascii=False)

    @classmethod
    def from_json_str(cls, string: str) -> Self:
        # Safe to use cast as json.loads can only return these limited
        # types; we know that's true because we know what parser the
        # json library is using, which mypy isn't able to infer.
        return cls.from_json_val(cast(JSONVal, json.loads(string)))


# TODO Could this be changed to actually allow base string/int/float
# values?
Jb = TypeVar('Jb', bound=JSONable)

JSONValPlus: TypeAlias = Union[None, bool, str, int, float, list['JSONValPlus'], dict[str, 'JSONValPlus'], JSONable]


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


def _parse_datetime_string(dt: str) -> datetime.datetime:
    try:
        if sys.version_info >= (3, 11):
            # Try using datetime.datetime.fromisoformat, as it's reasonably
            # comprehensive in this version.
            return datetime.datetime.fromisoformat(dt)
        else:
            # Try using the standard format in which Taskwarrior outputs
            # timestamps in JSON.
            return datetime.datetime.strptime(dt, "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.timezone.utc)

    except ValueError:
        # TODO This shouldn't be duplicating code in
        # TaskWarrior.calc_datetime, but I wanted a quick
        # solution rather than a good one...
        p = subprocess.run(('task',
                            'rc.verbose=nothing',
                            'rc.gc=0',
                            'rc.recurrence=0',
                            'rc.date.iso=yes',
                            'calc',
                            dt),
                           stdout=subprocess.PIPE,
                           check=True, encoding='utf-8')
        try:
            return datetime.datetime.fromisoformat(p.stdout.strip())
        except ValueError:
            # Maybe this isn't a datetime but a duration relative to
            # now.  Let's try that; JSONAbleDuration will parse such
            # values competently.
            dur = JSONableDuration(p.stdout.strip())
            return datetime.datetime.now() + dur


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
            dt = _parse_datetime_string(year_or_str_or_dt)
        elif isinstance(year_or_str_or_dt, datetime.datetime):
            dt = year_or_str_or_dt
        elif isinstance(year_or_str_or_dt, datetime.date):
            dt = datetime.datetime.combine(year_or_str_or_dt, datetime.time())
        else:
            raise ValueError(f'Unexpected input {year_or_str_or_dt!r}')

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
            if match:
                days = int(match['d']) if match['d'] else 0
                hours = int(match['h']) if match['h'] else 0
                minutes = int(match['m']) if match['m'] else 0
                seconds = int(match['s']) if match['s'] else 0
                return super().__new__(cls, days=days, hours=hours,
                                       minutes=minutes, seconds=seconds)
            if days_or_str_or_td.isnumeric():
                # Have seen this when using `task edit`: it seems to be trying
                # to "help" by converting the duration string to a number of
                # seconds, which gets brought in as a string.
                return super().__new__(cls, seconds=int(days_or_str_or_td))
            raise ValueError(f'Cannot interpret {days_or_str_or_td!r}')

        if isinstance(days_or_str_or_td, datetime.timedelta):
            return super().__new__(cls, days=days_or_str_or_td.days,
                                   seconds=days_or_str_or_td.seconds,
                                   microseconds=days_or_str_or_td.microseconds)

        raise ValueError(
                f'Cannot interpret {days_or_str_or_td!r} as a timedelta')

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
    _key_map: Mapping[str, Union[type[Jb],
                                 tuple[type[Jb], Callable[..., Jb]]]
                      ]
    _required_keys: ClassVar[Iterable[str]] = ()
    _fallback: Union[None, type[Jb], tuple[type[Jb], Callable[..., Jb]]] = None

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
    def _get_class_type(cls, key: str) -> type[Jb]:
        if cls._fallback is None:
            r = cls._key_map[key]
        else:
            r = cls._key_map.get(key, cls._fallback)

        # r is either a tuple of the type and the callable to convert things to
        # the type, or it's just the type.  In the first case, return the first
        # value in the tuple, in the second case, just return the value.
        if isinstance(r, tuple):
            return r[0]
        else:
            return r

    @classmethod
    def _get_class_fn(cls, key: str) -> tuple[type[Jb], Callable[..., Jb]]:
        if cls._fallback is None:
            r = cls._key_map[key]
        else:
            r = cls._key_map.get(key, cls._fallback)

        # r is either a tuple of the type and the callable to convert things to
        # the type, or it's just the type.  In the first case, that's what we
        # want to return anyway.  In the second case, the type itself is the
        # callable to create instances of the type.
        if isinstance(r, tuple):
            return r
        else:
            return r, r

    @classmethod
    def _parse_if_needed(cls, key: str,
                         *args: object, **kwargs: object) -> Jb:
        _type, fn = cls._get_class_fn(key)
        if (len(args) == 1 and len(kwargs) == 0 and
                isinstance((arg := args[0]), _type)):
            return arg
        return fn(*args, **kwargs)

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
                  default: Union[Vb, Semaphore] = SEMAPHORE) -> Union[Va, Vb]:
        try:
            value = self[key]
        except KeyError:
            if default is SEMAPHORE:
                raise
            return cast(Vb, default)
        assert isinstance(value, kind), f"{key!r} is {value!r}: {type(value).__qualname__} not {kind.__qualname__}"
        return value

    @classmethod
    def all_keys(cls) -> Collection[str]:
        return cls._key_map.keys()

    @classmethod
    def key_is_list(cls, key: str) -> bool:
        t = cls._get_class_type(key)
        return issubclass(t, list)


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
    def __init__(self, *args: object, **kwargs: object) -> None:
        if len(args) == 1 and len(kwargs) == 0:
            # Allow initialisation by just passing a different uuid.UUID
            # object.
            arg = args[0]
            if isinstance(arg, uuid.UUID):
                super().__init__(int=arg.int)
                return
        # TODO Remove and fix this ignore.
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    def _json_pre_dump(self) -> str:
        return str(self)

    @classmethod
    def uuid1(cls, node: Optional[int], clock_seq: Optional[int]) -> Self:
        return cls(uuid.uuid1(node, clock_seq))

    @classmethod
    def uuid3(cls, namespace: uuid.UUID, name: str) -> Self:
        return cls(uuid.uuid3(namespace, name))

    @classmethod
    def uuid4(cls) -> Self:
        return cls(uuid.uuid4())

    @classmethod
    def uuid5(cls, namespace: uuid.UUID, name: str) -> Self:
        return cls(uuid.uuid5(namespace, name))

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
