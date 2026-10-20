"""Every module's logger carries its real module name, so pipeline lines get a stage (DEF-034).

A module run with `python -m` is named `__main__`, so `getLogger(__name__)` there logs as
`__main__`, which no stage default matches; a name written out by hand drifts from the module's.
Modules that run as scripts name their logger `__spec__.name`, the module name either way.
"""

import ast
import pkgutil
from pathlib import Path

import pytest

import earth_hub
from earth_hub.logging_config import default_stage

PIPELINE = (
    "earth_hub.ingestion",
    "earth_hub.consumers",
    "earth_hub.database",
    "earth_hub.validation",
    "earth_hub.direct",
)


def modules() -> list[tuple[str, Path]]:
    root = Path(earth_hub.__file__).parent
    found = []
    for info in pkgutil.walk_packages([str(root)], "earth_hub."):
        relative = info.name.removeprefix("earth_hub.").replace(".", "/")
        path = root / (f"{relative}/__init__.py" if info.ispkg else f"{relative}.py")
        found.append((info.name, path))
    return found


def logger_names(tree: ast.Module) -> list[str]:
    """The source of every module-level `logging.getLogger(...)` argument."""
    return [
        ast.unparse(node.value.args[0])
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and ast.unparse(node.value.func) == "logging.getLogger"
        and node.value.args
    ]


def runs_as_script(name: str, tree: ast.Module) -> bool:
    guard = ast.parse('__name__ == "__main__"', mode="eval").body
    return name.endswith(".__main__") or any(
        isinstance(node, ast.If) and ast.dump(node.test) == ast.dump(guard) for node in tree.body
    )


@pytest.mark.req("REQ-OBS-001")
@pytest.mark.parametrize(("name", "path"), modules(), ids=[name for name, _ in modules()])
def test_the_logger_has_the_modules_name(name, path):
    tree = ast.parse(path.read_text())
    expected = "__spec__.name" if runs_as_script(name, tree) else "__name__"
    assert all(arg == expected for arg in logger_names(tree)), f"{name}: use {expected}"


@pytest.mark.req("REQ-OBS-001")
def test_every_pipeline_module_that_logs_has_a_default_stage():
    pipeline = [
        name
        for name, path in modules()
        if name.startswith(PIPELINE) and logger_names(ast.parse(path.read_text()))
    ]
    assert pipeline and [name for name in pipeline if default_stage(name) is None] == []
