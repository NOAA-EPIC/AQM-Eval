import subprocess

subprocess.check_call(["black", "."])
subprocess.check_call(
    [
        "mypy",
        "--disallow-untyped-defs",
        "--disallow-incomplete-defs",
        "--check-untyped-defs",
        ".",
    ]
)
subprocess.check_call(["pytest", "src/test"])
