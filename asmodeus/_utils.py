from typing import TypeAlias, Union, Self, Optional, overload, SupportsIndex, Any, cast, Protocol, TypeVar, runtime_checkable, Generic, ClassVar, TypeGuard
from collections.abc import Mapping, Sequence, Iterable
from abc import ABC, abstractmethod
import datetime
import re
import uuid
from dataclasses import dataclass
import subprocess

from asmodeus.json import *

# Copied from _typeshed
K = TypeVar('K')
V_co = TypeVar('V_co', covariant=True)
@runtime_checkable
class _SupportsKeysAndGetItem(Protocol[K, V_co]):
    def keys(self) -> Iterable[K]: ...
    def __getitem__(self, key: K) -> V_co: ...


def is_empty_iter(it: Iterable[Any]) -> bool:
    it = iter(it)
    try:
        next(it)
    except StopIteration:
        return True
    return False


def is_iterable_str(obj: Any) -> TypeGuard[Iterable[str]]:
    return isinstance(obj, Iterable) and all(isinstance(i, str) for i in obj)
