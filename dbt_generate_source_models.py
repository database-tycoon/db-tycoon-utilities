"""
This script generates SQL files for tables used in a dbt project for a given source.

Usage:
    python generate_source_models.py <mode> <source_name> [additional arguments]

Examples:
    Use case 1. Generate SQL files for tables used in the project with the source function:
        python generate_source_models.py used_sources supermetrics ./models ./models/sources/supermetrics

    Use case 2. Generate SQL files for all tables defined in a YAML file:
        python generate_source_models.py yaml supermetrics ./models/sources/supermetrics/supermetrics.yml

Arguments:
    mode: Mode to use. "used_sources" or "yaml".
    source_name: Name of the source to generate files for.

Additional arguments for "used_sources" mode:
    project_directory: Path to the dbt project directory.
    output_directory: Directory to save the generated SQL files.

Additional argument for "yaml" mode:
    yaml_file_path: Path to the YAML file containing source definitions.

Limitations:
    This script won't catch used sources that are not using the source function. e.g. if a downstream model is using a source table directly, this script won't catch it.

Note: This script requires dbt to be installed and configured in your environment.
"""

import os
import subprocess
import sys
import logging
import re
from collections import defaultdict
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_sources_used_with_source_func(directory, source_name):
    """
    Finds all SQL files in the given directory that use the specified source name
    and returns a dictionary of source tables and the files that use them.

    Args:
        directory (str): The root directory to search for SQL files.
        source_name (str): The name of the source to search for.

    Returns:
        dict: A dictionary where keys are source table names and values are sets
              of file paths that use those source tables.

    Note:
        This function excludes the 'sources' directory from the search to avoid
        unnecessary traversal.
    """
    used_sources = defaultdict(set)
    pattern = re.compile(
        rf"source\s*\(\s*['\"]({re.escape(source_name)})['\"]?\s*,\s*['\"]([^'\"]+)['\"]?\s*\)"
    )

    for root, dirs, files in os.walk(directory):
        if "sources" in dirs:
            dirs.remove("sources")  # Don't traverse into the sources directory
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    content = f.read()
                    matches = pattern.findall(content)
                    for match in matches:
                        if match[0] == source_name:
                            used_sources[match[1]].add(file_path)

    return used_sources


def build_sql_query(source_name, table_name):
    """
    Builds a SQL query for a given source and table using dbt's generate_base_model operation.

    Args:
        source_name (str): The name of the source in the dbt project.
        table_name (str): The name of the table in the specified source.

    Returns:
        str: The generated SQL query, or None if extraction failed.
    """
    command = f"dbt run-operation generate_base_model --args '{{'source_name': '{source_name}', 'table_name': '{table_name}'}}'"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    full_output = result.stdout
    start_index = full_output.find("with source")

    if start_index != -1:
        return full_output[start_index:].strip()
    else:
        logging.warning(
            f"Could not extract SQL content for {table_name}. Check the dbt output."
        )
        return None


def generate_source_models_from_yml(yaml_path, source_name):
    """
    Generates SQL files for all tables in a given source defined in a YAML file.

    Args:
        yaml_path (str): Path to the YAML file containing source definitions.
        source_name (str): Name of the source to generate files for.

    Returns:
        None
    """
    with open(yaml_path, "r") as file:
        yaml_content = yaml.safe_load(file)

    output_dir = os.path.dirname(yaml_path)
    os.makedirs(output_dir, exist_ok=True)

    for source in yaml_content["sources"]:
        if source["name"] == source_name:
            for table in source["tables"]:
                table_name = table["name"]
                file_name = f"source_{source_name}__{table_name}.sql"
                file_path = os.path.join(output_dir, file_name)

                sql_query = build_sql_query(source_name, table_name)
                if sql_query:
                    with open(file_path, "w") as f:
                        f.write(sql_query)
                    logging.info(f"Created {file_path}")


def generate_sql_files_from_used_sources(directory, source_name, output_dir):
    """
    Generates SQL files for tables used in the project for a given source.

    Args:
        directory (str): Path to the dbt project directory.
        source_name (str): Name of the source to generate files for.
        output_dir (str): Directory to save the generated SQL files.

    Returns:
        None
    """
    used_sources = get_sources_used_with_source_func(directory, source_name)
    os.makedirs(output_dir, exist_ok=True)

    for table_name in used_sources.keys():
        file_name = f"source_{source_name}__{table_name}.sql"
        file_path = os.path.join(output_dir, file_name)

        sql_query = build_sql_query(source_name, table_name)
        if sql_query:
            with open(file_path, "w") as f:
                f.write(sql_query)
            logging.info(f"Created {file_path}")

    logging.info(
        f"Generated SQL files for {len(used_sources)} tables used in the project."
    )


def main():
    """
    Main function to execute the script based on command-line arguments.

    Usage:
        python generate_source_models.py <mode> <source_name> [additional arguments]

    Args:
        mode (str): Mode to use. "used_sources" or "yaml".
        source_name (str): Name of the source to generate files for.
        
    Additional arguments for "used_sources" mode:
        project_directory (str): Path to the dbt project directory.
        output_directory (str): Directory to save the generated SQL files.
    
    Additional argument for "yaml" mode:
        yaml_file_path (str): Path to the YAML file containing source definitions.

    Returns:
        None
    """
    if len(sys.argv) < 3:
        logging.error(
            "Usage: python script_name.py <mode> <source_name> [additional arguments]"
        )
        sys.exit(1)

    mode = sys.argv[1]
    source_name = sys.argv[2]

    if mode == "used_sources":
        if len(sys.argv) != 5:
            logging.error(
                "Usage for used_sources mode: python script_name.py used_sources <source_name> <project_directory> <output_directory>"
            )
            sys.exit(1)
        project_directory = sys.argv[3]
        output_directory = sys.argv[4]
        generate_sql_files_from_used_sources(
            project_directory, source_name, output_directory
        )
        logging.info("All SQL files have been generated from used sources.")
    elif mode in ["yaml", "yml"]:
        if len(sys.argv) != 4:
            logging.error(
                "Usage for yaml mode: python script_name.py yaml <source_name> <yaml_file_path>"
            )
            sys.exit(1)
        yaml_file_path = sys.argv[3]
        generate_source_models_from_yml(yaml_file_path, source_name)
        logging.info("All SQL files have been generated from YAML.")
    else:
        logging.error("Invalid mode. Use 'used_sources' or 'yaml'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
