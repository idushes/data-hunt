import ast
import shlex
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _dockerfile_copy_sources(dockerfile: Path) -> set[str]:
    sources: set[str] = set()
    for line in dockerfile.read_text().splitlines():
        if not line.startswith("COPY "):
            continue
        arguments = shlex.split(line)[1:]
        while arguments and arguments[0].startswith("--"):
            arguments.pop(0)
        if len(arguments) > 1:
            sources.update(arguments[:-1])
    return sources


def _local_server_imports(repository_root: Path) -> set[str]:
    server = ast.parse((repository_root / "server.py").read_text())
    imports = {"server.py"}
    for node in server.body:
        if not isinstance(node, ast.ImportFrom) or node.level or not node.module:
            continue
        module_path = repository_root / f"{node.module}.py"
        if module_path.is_file():
            imports.add(module_path.name)
    return imports


class DockerfilePackagingTest(unittest.TestCase):
    def test_packages_all_top_level_modules_imported_at_server_startup(self):
        copied_sources = _dockerfile_copy_sources(REPOSITORY_ROOT / "Dockerfile")
        required_modules = _local_server_imports(REPOSITORY_ROOT)

        self.assertFalse(
            required_modules - copied_sources,
            f"Dockerfile omits startup modules: {sorted(required_modules - copied_sources)}",
        )


if __name__ == "__main__":
    unittest.main()
