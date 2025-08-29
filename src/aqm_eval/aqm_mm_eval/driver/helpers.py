import platform
from pathlib import Path


def create_symlinks(
    src_dir: Path,
    dst_dir: Path,
    src_dir_template: tuple[str, ...],
    src_fn_template: tuple[str, ...],
):
    """
    Create symlinks in dst_dir for files in src_dir that match templates.

    Args:
        src_dir: Source directory to search for files
        dst_dir: Destination directory where symlinks will be created
        src_dir_template: Directory name patterns to match
        src_fn_template: Filename patterns to match
    """
    if not dst_dir.exists():
        raise ValueError(f"destination directory does not exist: {dst_dir}")
    # Find directories matching src_dir_template
    for dir_pattern in src_dir_template:
        for subdir in src_dir.glob(dir_pattern):
            if not subdir.is_dir():
                continue
            # Find files in matching directories that match src_fn_template
            for fn_pattern in src_fn_template:
                for src_file in subdir.glob(fn_pattern):
                    if not src_file.is_file():
                        continue
                    # Create symlink if it doesn't already exist
                    dst_file = dst_dir / f"{subdir.name}_{src_file.name}"
                    if not dst_file.exists():
                        if platform.system() == "Windows":  # Here for testing
                            dst_file.hardlink_to(src_file)
                        else:
                            dst_file.symlink_to(src_file)
