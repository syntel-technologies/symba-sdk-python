"""Pydantic validate/serialize glue + the process-local schema index (spec 16).

When a task declares schemas the SDK validates payload-in and result-out, and
registers the output type in a process-local index so a *consumer task in the
same process* deserializes ``ctx.output[...]`` into the producer's model. Cross-
process consumers get plain dicts — schema knowledge is process-local by design;
the engine stays schema-blind.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .errors import OutputValidationError, PayloadValidationError

if TYPE_CHECKING:
    from pydantic import BaseModel

#: task_name -> output model, populated at registration (spec 16 step 3).
_OUTPUT_INDEX: dict[str, type[BaseModel]] = {}


def register_output_schema(task_name: str, model: type[BaseModel] | None) -> None:
    if model is not None:
        _OUTPUT_INDEX[task_name] = model


def output_schema_for(task_name: str) -> type[BaseModel] | None:
    return _OUTPUT_INDEX.get(task_name)


def clear_index() -> None:
    """Test helper — reset the process-local index."""
    _OUTPUT_INDEX.clear()


def validate_payload(payload: Any, model: type[BaseModel] | None) -> Any:
    """Validate an incoming payload; return the model instance or the raw dict."""
    if model is None:
        return payload
    try:
        return model.model_validate(payload)
    except Exception as exc:
        raise PayloadValidationError(f"payload failed input_schema validation: {exc}") from exc


def serialize_result(result: Any, model: type[BaseModel] | None) -> dict[str, Any]:
    """Validate + serialize a handler return value to a JSON-able dict (spec 16 step 2)."""
    if hasattr(result, "model_dump"):
        if model is not None and not isinstance(result, model):
            try:
                result = model.model_validate(result.model_dump())
            except Exception as exc:
                raise OutputValidationError(f"result failed output_schema: {exc}") from exc
        return result.model_dump(mode="json")
    if model is not None:
        try:
            validated = model.model_validate(result)
        except Exception as exc:
            raise OutputValidationError(f"result failed output_schema validation: {exc}") from exc
        return validated.model_dump(mode="json")
    if result is None:
        return {}
    if not isinstance(result, dict):
        raise OutputValidationError(
            f"handler returned {type(result).__name__}; must return dict | BaseModel | "
            f"ctx.stop_chain(...) | ctx.skip()"
        )
    return result


__all__ = [
    "register_output_schema",
    "output_schema_for",
    "clear_index",
    "validate_payload",
    "serialize_result",
]
