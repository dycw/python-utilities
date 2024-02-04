from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, TypeVar

from frozendict import frozendict
from pydantic import BaseModel, GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic_core.core_schema import (
    CoreSchema,
    chain_schema,
    dict_schema,
    is_instance_schema,
    json_or_python_schema,
    no_info_plain_validator_function,
    plain_serializer_function_ser_schema,
    union_schema,
)
from typing_extensions import override

from utilities.pathlib import ensure_path

if TYPE_CHECKING:
    from pydantic.json_schema import JsonSchemaValue

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import PathLike

_BM = TypeVar("_BM", bound=BaseModel)


class HashableBaseModel(BaseModel):
    """Subclass of BaseModel which is hashable."""

    @override
    def __hash__(self) -> int:
        return hash((type(self), *self.__dict__.values()))


def load_model(model: type[_BM], path: PathLike, /) -> _BM:
    path = ensure_path(path)
    try:
        with path.open() as fh:
            return model.model_validate_json(fh.read())
    except FileNotFoundError:
        raise _LoadModelFileNotFoundError(model=model, path=path) from None
    except IsADirectoryError:  # pragma: os-ne-windows
        raise _LoadModelIsADirectoryError(model=model, path=path) from None


@dataclass(kw_only=True)
class LoadModelError(Exception):
    model: type[BaseModel]
    path: Path


@dataclass(kw_only=True)
class _LoadModelFileNotFoundError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model}; path {str(self.path)!r} must exist."


@dataclass(kw_only=True)
class _LoadModelIsADirectoryError(LoadModelError):
    @override
    def __str__(self) -> str:
        return f"Unable to load {self.model  }; path {str(self.path)!r} must not be a directory."  # pragma: os-ne-windows


class _PydanticFrozenDictAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Any, _handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        from_dict_schema = chain_schema(
            [dict_schema(), no_info_plain_validator_function(frozendict)]
        )
        return json_or_python_schema(
            json_schema=from_dict_schema,
            python_schema=union_schema(
                [is_instance_schema(frozendict), from_dict_schema]
            ),
            serialization=plain_serializer_function_ser_schema(dict),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        return handler(dict_schema())


_K = TypeVar("_K")
_V = TypeVar("_V")
PydanticFrozenDict = Annotated[frozendict[_K, _V], _PydanticFrozenDictAnnotation]


def save_model(
    model: BaseModel,
    path: PathLike,
    /,
    *,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
    overwrite: bool = False,
) -> None:
    with writer(path, overwrite=overwrite) as temp, temp.open(mode="w") as fh:
        _ = fh.write(
            model.model_dump_json(
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            )
        )


__all__ = ["HashableBaseModel", "LoadModelError", "load_model", "save_model"]
