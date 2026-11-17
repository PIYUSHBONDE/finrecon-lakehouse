"""Offline Docker-volume archives with byte and metadata preservation proof.

Run inside the pinned Spark image as root with /data and /backup mounts.
The caller must stop every writer before backup and keep it stopped throughout.
Archives contain only regular files and directories, never links or devices.
GNU tar preserves sparse Kafka indexes and PostgreSQL file ownership.
"""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import subprocess
import tarfile


def sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def inventory(root):
    root = Path(root)
    result = {}
    for path in [root, *sorted(root.rglob("*"))]:
        info = path.lstat()
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise ValueError("Unsupported link or special file: " + str(path))
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise ValueError("Hard-linked files are not supported: " + str(path))
        key = path.relative_to(root).as_posix()
        row = {
            "kind": "directory" if path.is_dir() else "file",
            "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid,
            "gid": info.st_gid,
            "mtime_ns": info.st_mtime_ns,
        }
        if row["kind"] == "file":
            row.update(size=info.st_size, sha256=sha256(path))
        result[key] = row
    return result


def validate_archive(archive, expected):
    seen = set()
    with tarfile.open(archive, "r:") as stream:
        for member in stream:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or "\\" in member.name:
                raise ValueError("Unsafe archive path: " + member.name)
            key = path.as_posix()
            if key in seen or key not in expected:
                raise ValueError("Unexpected or duplicate archive entry: " + key)
            seen.add(key)
            row = expected[key]
            if not (member.isdir() if row["kind"] == "directory" else member.isfile()):
                raise ValueError("Archive entry type changed: " + key)
            if row["kind"] == "file" and member.size != row["size"]:
                raise ValueError("Archive entry size changed: " + key)
    if seen != set(expected):
        raise ValueError("Archive entries do not match manifest")


def backup(root, directory):
    root, directory = Path(root), Path(directory)
    archive, manifest = directory / "data.tar", directory / "manifest.json"
    if archive.exists() or manifest.exists():
        raise FileExistsError("Refusing to replace an existing backup")
    before = inventory(root)
    subprocess.run(
        ["tar", "--sparse", "--format=pax", "-cf", str(archive), "-C", str(root), "."],
        check=True,
    )
    if inventory(root) != before:
        raise RuntimeError("Volume changed during offline backup")
    validate_archive(archive, before)
    record = {
        "version": 1,
        "archive_sha256": sha256(archive),
        "archive_bytes": archive.stat().st_size,
        "logical_bytes": sum(row.get("size", 0) for row in before.values()),
        "entries": before,
    }
    manifest.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


def restore(root, directory):
    root, directory = Path(root), Path(directory)
    if any(root.iterdir()):
        raise ValueError("Restore requires an empty target volume")
    record = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    archive = directory / "data.tar"
    if record["version"] != 1 or sha256(archive) != record["archive_sha256"]:
        raise ValueError("Backup hash or version mismatch")
    validate_archive(archive, record["entries"])
    subprocess.run(
        [
            "tar",
            "--same-owner",
            "--same-permissions",
            "-xf",
            str(archive),
            "-C",
            str(root),
        ],
        check=True,
    )
    if inventory(root) != record["entries"]:
        raise RuntimeError("Restored bytes or filesystem metadata differ")
    return {
        "passed": True,
        "entries": len(record["entries"]),
        "archive_sha256": record["archive_sha256"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("backup", "restore"))
    parser.add_argument("--root", type=Path, default=Path("/data"))
    parser.add_argument("--directory", type=Path, default=Path("/backup"))
    args = parser.parse_args()
    result = globals()[args.action](args.root, args.directory)
    print(json.dumps({key: value for key, value in result.items() if key != "entries"}))


if __name__ == "__main__":
    main()
