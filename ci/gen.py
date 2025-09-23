import json
import pathlib

CONTAINER_IMAGE = "ghcr.io/informatica-na-presales-ops/process-street-tools"
DEFAULT_BRANCH = "main"
PUSH_OR_DISPATCH = (
    "github.event_name == 'push' || github.event_name == 'workflow_dispatch'"
)
THIS_FILE = pathlib.PurePosixPath(
    pathlib.Path(__file__).relative_to(pathlib.Path.cwd())
)


def gen(content: dict, target: str) -> None:
    pathlib.Path(target).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(target).write_text(
        json.dumps(content, indent=2, sort_keys=True), newline="\n"
    )


def gen_dependabot() -> None:
    target = ".github/dependabot.yaml"
    content = {
        "version": 2,
        "updates": [
            {
                "package-ecosystem": e,
                "allow": [{"dependency-type": "all"}],
                "directory": "/",
                "schedule": {"interval": "daily"},
            }
            for e in ["docker", "github-actions", "uv"]
        ],
    }
    gen(content, target)


def gen_workflow_build() -> None:
    target = ".github/workflows/build.yaml"
    content = {
        "env": {
            "description": f"This workflow ({target}) was generated from {THIS_FILE}"
        },
        "name": "Build the container image",
        "on": {
            "pull_request": {"branches": [DEFAULT_BRANCH]},
            "push": {"branches": [DEFAULT_BRANCH]},
            "workflow_dispatch": {},
        },
        "permissions": {},
        "jobs": {
            "build": {
                "name": "Build the container image",
                "permissions": {"packages": "write"},
                "runs-on": "ubuntu-latest",
                "steps": [
                    {
                        "name": "Set up Docker Buildx",
                        "uses": "docker/setup-buildx-action@v3",
                    },
                    {
                        "name": "Build the container image",
                        "uses": "docker/build-push-action@v6",
                        "with": {
                            "cache-from": "type=gha",
                            "cache-to": "type=gha,mode=max",
                            "tags": f"{CONTAINER_IMAGE}:latest",
                        },
                    },
                    {
                        "name": "Log in to GitHub container registry",
                        "if": PUSH_OR_DISPATCH,
                        "uses": "docker/login-action@v3",
                        "with": {
                            "password": "${{ github.token }}",
                            "registry": "ghcr.io",
                            "username": "${{ github.actor }}",
                        },
                    },
                    {
                        "name": "Push latest image to registry",
                        "if": PUSH_OR_DISPATCH,
                        "uses": "docker/build-push-action@v6",
                        "with": {
                            "cache-from": "type=gha",
                            "push": True,
                            "tags": f"{CONTAINER_IMAGE}:latest",
                        },
                    },
                ],
            }
        },
    }
    gen(content, target)


def main() -> None:
    gen_dependabot()
    gen_workflow_build()


if __name__ == "__main__":
    main()
