from collections.abc import Iterable
from typing import Any, Protocol, TypeVar, runtime_checkable
import sys

if sys.version_info >= (3, 11):
    from typing import TypeGuard
else:
    # The package requires typing_extensions, so just import from there
    # to keep things simple, even though the imports might exist in the
    # stdlib typing module.
    from typing_extensions import TypeGuard

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
