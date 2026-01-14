"""
File Utility Functions for Supreme Court Judgments
Handles file operations, TAR archive management, etc.
"""

import io
import logging
import tarfile
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def create_tar_archive(output_path: Path, files: dict) -> int:
    """
    Create a TAR archive from a dictionary of files

    Args:
        output_path: Path to create the TAR file
        files: Dictionary of {filename: content} where content is bytes or str

    Returns:
        Size of the created archive in bytes
    """
    with tarfile.open(output_path, "w") as tf:
        for filename, content in files.items():
            data = content if isinstance(content, bytes) else content.encode("utf-8")
            info = tarfile.TarInfo(name=filename)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

    return output_path.stat().st_size


def add_to_tar_archive(archive_path: Path, filename: str, content: bytes | str):
    """
    Add a single file to an existing TAR archive

    Args:
        archive_path: Path to the TAR file
        filename: Name of the file to add
        content: Content of the file (bytes or str)
    """
    data = content if isinstance(content, bytes) else content.encode("utf-8")
    info = tarfile.TarInfo(name=filename)
    info.size = len(data)

    with tarfile.open(archive_path, "a") as tf:
        tf.addfile(info, io.BytesIO(data))


def list_tar_contents(archive_path: Path) -> List[str]:
    """
    List all files in a TAR archive

    Args:
        archive_path: Path to the TAR file

    Returns:
        List of filenames in the archive
    """
    with tarfile.open(archive_path, "r") as tf:
        return tf.getnames()


def extract_file_from_tar(archive_path: Path, filename: str) -> Optional[bytes]:
    """
    Extract a single file from a TAR archive

    Args:
        archive_path: Path to the TAR file
        filename: Name of the file to extract

    Returns:
        Content of the file as bytes, or None if not found
    """
    try:
        with tarfile.open(archive_path, "r") as tf:
            member = tf.getmember(filename)
            f = tf.extractfile(member)
            if f:
                return f.read()
    except KeyError:
        logger.debug(f"File {filename} not found in {archive_path}")
    except Exception as e:
        logger.error(f"Error extracting {filename} from {archive_path}: {e}")

    return None


def get_tar_size(archive_path: Path) -> int:
    """Get the size of a TAR archive in bytes"""
    return archive_path.stat().st_size if archive_path.exists() else 0
