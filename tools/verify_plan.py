"""Check implementation task references and dependency cycles."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "implementation"
TASK = re.compile(r"M\d{2}\.\d{2}")


def main():
    tasks = {}
    errors = []
    for filename in ("foundation.md", "availability.md", "production.md"):
        for line in (DOCS / filename).read_text(encoding="utf-8").splitlines():
            if not re.match(r"\| M\d{2}\.\d{2} \|", line):
                continue
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            task = cells[0]
            if task in tasks:
                errors.append(f"duplicate task {task}")
            tasks[task] = TASK.findall(cells[1])
            if len(cells) != 4 or not cells[2] or not cells[3]:
                errors.append(f"missing goal or acceptance evidence for {task}")

    for task, dependencies in tasks.items():
        for dependency in dependencies:
            if dependency not in tasks:
                errors.append(f"{task} references unknown dependency {dependency}")

    visiting, visited = set(), set()

    def visit(task):
        if task in visiting:
            errors.append(f"dependency cycle at {task}")
            return
        if task in visited or task not in tasks:
            return
        visiting.add(task)
        for dependency in tasks[task]:
            visit(dependency)
        visiting.remove(task)
        visited.add(task)

    for task in tasks:
        visit(task)
    coverage = set(TASK.findall((DOCS / "coverage.md").read_text(encoding="utf-8")))
    for task in sorted(coverage - tasks.keys()):
        errors.append(f"coverage references unknown task {task}")
    for task in sorted(tasks.keys() - coverage):
        errors.append(f"task {task} has no coverage mapping")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Validated {len(tasks)} tasks, dependencies and coverage references.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
