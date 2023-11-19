import collections
from keyword import iskeyword
from typing import Any, Dict, Tuple, Type, Union, get_args, get_origin

NoValue = object()


class PoeOptions:
    __annotations: Dict[str, Type]

    def __init__(self, options_dict: Dict[str, Any], strict: bool = False):
        for key in self.get_fields().keys():
            if key in options_dict:
                setattr(self, key, options_dict[key])

        # TODO: in strict mode raise appropraitely if options_dict
        # misses required keys or includes unknown keys

    def get(self, key: str, default: Any = NoValue) -> Any:
        if iskeyword(key):
            key = f"{key}_"

        if not hasattr(self, key) and key not in self.get_fields():
            raise KeyError(f"{self.__class__.__name__} has no such attribute {key!r}")

        result = getattr(self, key, default)
        if result is NoValue:
            type_of_attr = self.type_of(key)
            if isinstance(type_of_attr, tuple):
                return type_of_attr[0]()
            return type_of_attr()
        return result

    def is_set(self, key: str):
        return getattr(self, key, NoValue) != NoValue

    def update(self, options_dict: Dict[str, Any]):
        new_options_dict = {}
        for key in self.get_fields().keys():
            if key in options_dict:
                new_options_dict[key] = options_dict[key]
            elif hasattr(self, key):
                new_options_dict[key] = getattr(self, key)

    @classmethod
    def type_of(cls, key: str) -> Union[Type, Tuple[Type, ...]]:
        annotations = cls.get_fields()

        if iskeyword(key):
            key = f"{key}_"

        result = annotations[key]
        if get_origin(result) is Union:
            return get_args(result)
        if get_origin(result) in (
            dict,
            collections.abc.Mapping,
            collections.abc.MutableMapping,
        ):
            return dict
        if get_origin(result) in (
            list,
            collections.abc.Sequence,
        ):
            return list

        return result

    @classmethod
    def get_fields(cls) -> Dict[str, Any]:
        """
        Recent python versions removed inheritance for __annotations__
        so we have to implement it explicitly
        """
        if not hasattr(cls, "__annotations"):
            annotations = {}
            for base_cls in cls.__bases__:
                annotations.update(base_cls.__annotations__)
            annotations.update(cls.__annotations__)
            cls.__annotations = {
                key: type_
                for key, type_ in annotations.items()
                if not key.startswith("_")
            }
        return cls.__annotations
