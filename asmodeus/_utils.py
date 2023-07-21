from collections.abc import Iterable
from typing import Any, Protocol, TypeVar, Union
import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias, TypeGuard
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import TypeAlias, TypeGuard

T = TypeVar('T')
OneOrMany: TypeAlias = Union[T, Iterable[T]]


K = TypeVar('K')
V_co = TypeVar('V_co', covariant=True)


# Copied from _typeshed
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
