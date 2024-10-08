"""
This script finds all SQL files in the given project directory that use the source function
and replaces all occurrences of the source function with a ref to a model named source_<source_name>__<table_name>.

Usage:
    python replace_source_func_with_ref.py <project_directory>

Example:
    python replace_source_func_with_ref.py ./models/marts

This script will find all SQL files in the given project directory that use the source function
and replace all occurrences of the source function with a ref to a model named source_<source_name>__<table_name>.
"""

import os
import re
import sys
import logging


def setup_logging():
    """
    Sets up logging for the script.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def find_files_with_source_refs(project_dir):
    """
    Finds all SQL files in the given project directory that use the source function.

    Args:
        project_dir (str): The root directory to search for SQL files.

    Returns:
        list: A list of file paths that use the source function.
    """
    source_pattern = re.compile(r"{{\s*source\s*\(")
    files_with_sources = []

    for root, _, files in os.walk(project_dir):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    content = f.read()
                if source_pattern.search(content):
                    files_with_sources.append(file_path)

    return files_with_sources


def replace_sources_with_refs(file_path):
    """
    Replaces all occurrences of the source function with a ref to a model named source_<source_name>__<table_name>
    in the given file path.

    Args:
        file_path (str): The path to the SQL file to be processed.
    """
    with open(file_path, "r") as f:
        content = f.read()

    matches = re.finditer(
        r"{{\s*source\s*\(\s*(?:\"?\'?([^\"\']+)\"?\'?\s*,\s*\"?\'?([^\"\']+)\"?\'?)\s*\)\s*}}",
        content,
        re.DOTALL,
    )
    if matches:
        for match in matches:
            source_name = match.group(1)
            table_name = match.group(2)
            new_ref = f"{{{{ ref('source_{source_name}__{table_name}') }}}}"
            content = content.replace(match.group(0), new_ref)

    with open(file_path, "w") as f:
        f.write(content)

    logging.info(f"Processed: {file_path}")


if __name__ == "__main__":
    setup_logging()

    project_directory = sys.argv[1] if len(sys.argv) > 1 else "./models/marts"

    files_to_process = find_files_with_source_refs(project_directory)

    for file_path in files_to_process:
        replace_sources_with_refs(file_path)

    logging.info(f"Processed {len(files_to_process)} files with source references.")
