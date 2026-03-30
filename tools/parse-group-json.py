import argparse
import json
import pathlib


class Args:
    groups_file: pathlib.Path


def parse_args() -> Args:
    parser = argparse.ArgumentParser()
    parser.add_argument("groups_file", type=pathlib.Path)
    ns = Args()
    return parser.parse_args(namespace=ns)


def main() -> None:
    args = parse_args()
    data = json.loads(args.groups_file.read_text())
    print("id,group_email,display_name")
    for g in data:
        id_ = g.get("id")
        email = g.get("user").get("email")
        group_name = g.get("user").get("username")
        print(f"{id_},{email},{group_name}")


if __name__ == "__main__":
    main()
