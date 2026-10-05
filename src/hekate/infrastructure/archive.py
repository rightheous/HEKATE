from __future__ import annotations

import hashlib
import fcntl
import os
import re
import secrets
import stat
from pathlib import Path

from hekate.domain.errors import Conflict
from hekate.domain.models import ArtifactRef


_REF = re.compile(r"sha256:([0-9a-f]{64})\Z")
_LOCKS = ".locks"


def acquire_lock(root: Path, ref: ArtifactRef) -> int:
    match = _REF.fullmatch(ref)
    if match is None:
        raise ValueError("invalid archive reference")
    root.mkdir(parents=True, exist_ok=True)
    root_fd = os.open(root.resolve(strict=True), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        try:
            os.mkdir(_LOCKS, mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        locks_fd = os.open(_LOCKS, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0), dir_fd=root_fd)
        try:
            fd = os.open(
                f"{match.group(1)}.lock",
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=locks_fd,
            )
        finally:
            os.close(locks_fd)
    finally:
        os.close(root_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("archive lock is not a regular file")
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd
    except BaseException:
        os.close(fd)
        raise


def release_lock(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _directory(root: Path, ref: ArtifactRef, *, create: bool = False) -> tuple[int, int, str]:
    match = _REF.fullmatch(ref)
    if match is None:
        raise ValueError("invalid archive reference")
    root.mkdir(parents=True, exist_ok=True)
    root_fd = os.open(root.resolve(strict=True), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    name = match.group(1)[:2]
    try:
        if create:
            try:
                os.mkdir(name, mode=0o700, dir_fd=root_fd)
            except FileExistsError:
                pass
        directory_fd = os.open(name, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0), dir_fd=root_fd)
    except BaseException:
        os.close(root_fd)
        raise
    return root_fd, directory_fd, match.group(1)


def put(root: Path, content: bytes) -> ArtifactRef:
    digest = hashlib.sha256(content).hexdigest()
    ref = ArtifactRef(f"sha256:{digest}")
    lock_fd = acquire_lock(root, ref)
    try:
        return _put_locked(root, content, ref, digest)
    finally:
        release_lock(lock_fd)


def _put_locked(root: Path, content: bytes, ref: ArtifactRef, digest: str) -> ArtifactRef:
    root_fd, directory_fd, name = _directory(root, ref, create=True)
    temporary = f".stage-{secrets.token_hex(16)}"
    try:
        try:
            info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("archive object is not a regular file")
            fd = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd)
            try:
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    existing = stream.read(1_048_577)
            finally:
                os.close(fd)
            if existing != content:
                raise Conflict("content-addressed archive object conflicts")
            return ref
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=directory_fd)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
        fd = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd)
        try:
            with os.fdopen(fd, "rb", closefd=False) as stream:
                stored = stream.read(1_048_577)
        finally:
            os.close(fd)
        if hashlib.sha256(stored).hexdigest() != digest:
            raise IOError("archive digest verification failed")
        return ref
    finally:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)
        os.close(root_fd)


def read(root: Path, ref: ArtifactRef, limit: int) -> bytes:
    if limit < 0:
        raise ValueError("archive read limit must be nonnegative")
    root_fd, directory_fd, name = _directory(root, ref)
    try:
        fd = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 1_048_576:
                raise ValueError("archive object is not a bounded regular file")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                full = stream.read(1_048_577)
        finally:
            os.close(fd)
    finally:
        os.close(directory_fd)
        os.close(root_fd)
    digest = _REF.fullmatch(ref).group(1)  # type: ignore[union-attr]
    if len(full) > 1_048_576 or hashlib.sha256(full).hexdigest() != digest:
        raise IOError("archive digest verification failed")
    return full[:limit]


def delete(root: Path, ref: ArtifactRef) -> bool:
    lock_fd = acquire_lock(root, ref)
    try:
        return delete_locked(root, ref)
    finally:
        release_lock(lock_fd)


def delete_locked(root: Path, ref: ArtifactRef) -> bool:
    try:
        root_fd, directory_fd, name = _directory(root, ref)
    except FileNotFoundError:
        return True
    try:
        try:
            info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return True
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("archive object is not a regular file")
        os.unlink(name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        return True
    finally:
        os.close(directory_fd)
        os.close(root_fd)


def verify_hash(content: bytes, digest: str) -> bool:
    return hashlib.sha256(content).hexdigest() == digest
