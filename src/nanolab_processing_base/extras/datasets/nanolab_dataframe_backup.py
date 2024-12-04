import re
import warnings
import re
import warnings
import logging
import pandas as pd
from typing import Dict, List, Tuple
from kedro.framework.session import KedroSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_catalog_item(dataset_name: str) -> Dict:
    """
    Load a dataset from Kedro's catalog.
    """
    with KedroSession.create() as session:
        context = session.load_context()
        catalog = context.catalog
        try:
            dataset = catalog.load(dataset_name)
            logger.info(f"Loaded dataset: {dataset_name}")
            return dataset
        except KeyError:
            logger.error(f"Dataset '{dataset_name}' not found in catalog.")
            raise


procedures = load_catalog_item("params:procedures")

def unix_time_to_datetime(unix_time_str: str) -> pd.Timestamp:
    """
    Converts a Unix timestamp string into a pandas datetime object.

    Args:
        unix_time_str (str): Unix timestamp as a string (e.g., "1731364225.4285064").

    Returns:
        pd.Timestamp: Corresponding pandas datetime object.
    """
    try:
        # Convert string to float, then to pandas datetime
        unix_time = float(unix_time_str)
        return pd.to_datetime(unix_time, unit='s')
    except ValueError as e:
        raise ValueError(f"Invalid Unix timestamp: {unix_time_str}") from e


def make_dict_from_parsed_data(list_of_lines: List[str]) -> Dict[str, str]:
    """
    Converts a list of lines with key-value pairs separated by ': ' into a dictionary.

    Args:
        list_of_lines (list): A list of strings, each containing a key and a value
                              separated by ': ' (e.g., 'key: value').

    Returns:
        dict: A dictionary with keys and values extracted from the input list.

    Example:
        Input: ['key1: value1', 'key2: value2']
        Output: {'key1': 'value1', 'key2': 'value2'}
    """
    try:
        return {k: i for k, i in (pair.split(": ", 1) for pair in list_of_lines)}
    except ValueError:
        raise ValueError("Each line must contain a key and a value separated by ': '.")

def read_comment_lines(file_path: str) -> Tuple[Dict[str, str], int]:
    """
    Reads and processes comment lines from the beginning of a file.

    This function reads a file line by line, extracting lines that start with
    the prefix `#\t` (indicating a comment with tab-indented data). It stops
    reading as soon as it encounters a line that does not begin with `#`.
    The extracted lines are cleaned (prefix removed and whitespace stripped)
    and returned as a dictionary.

    Args:
        file_path (str): The path to the file to be read.

    Returns:
        Tuple[Dict[str, str], int]:
            - A dictionary where each key-value pair corresponds to a processed
              comment line in the format `key:value`.
            - The total number of processed comment lines.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If any other error occurs during file processing.

    Example:
        Given a file `example.txt` with the following content:

        ```
        #\tkey1: value1
        #\tkey2: value2
        Normal line
        ```

        >>> read_comment_lines("example.txt")
        ({'key1': 'value1', 'key2': 'value2'}, 2)

    Notes:
        - Comment lines must start with `#\t` and contain a tab character (`\t`)
          to be processed.
        - Lines not starting with `#` (or that lack a tab character) will terminate
          comment processing.

    """
    comment_lines = []
    current_line = 0
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if line.strip().startswith('#'):
                    current_line += 1
                if line.strip().startswith('#\t'):
                    # Remove the prefix "#\t" and strip whitespace
                    comment_lines.append(line.strip().replace("#\t", ""))

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except Exception as e:
        raise Exception(f"An error occurred while processing the file: {e}")

    if comment_lines:
        return make_dict_from_parsed_data(comment_lines), current_line
    else:
        return {}, 0
def determine_procedure(path: str) -> str:
    """
    Extracts and processes content enclosed within angle brackets ("<" and ">")
    from the first line of a csv file corresponding to an experiment.

    The function reads the first line of the specified file and uses a
    regular expression to extract the content inside the angle brackets.
    If content is found, it splits the string at each period (".") and
    returns the last segment. If no content is found within the angle brackets,
    it raises a warning.

    Parameters:
        path (str): The path to the text file to be processed.

    Returns:
        str or None: The last segment of the content within angle brackets
                     (after splitting by ".") if found; otherwise, None.

    Raises:
        UserWarning: If no content is found between angle brackets.

    Example:
        Suppose `example.txt` contains the line:
        "Procedure: <module.submodule.method>"

        >>> determine_procedure("example.txt")
        'method'

        If the file contains no angle brackets, it will raise a warning:
        >>> determine_procedure("empty.txt")
        UserWarning: No content found between < and >

        Returns None in such cases.
    """
    if not path.endswith(".csv"):
        raise ValueError(f"The file '{path}' is not a CSV. Please provide a valid .csv file.")

    with open(path, "r") as path:
        first_line = path.readline().strip()  # Read the first line and strip whitespace

    # Use a regular expression to extract content between < and >
    match = re.search(r"<(.*?)>", first_line)
    if match:
        content = match.group(1)  # Extract the content between < and >
        return content.split(".")[-1]
    else:
        warnings.warn("No content found between < and >", UserWarning)
        return ""


def string_to_bool(string_bool: str) -> bool:
    return string_bool == "True"


def parse_metadata(key, value, how_to_process):
    """
    Parses a metadata value based on the specified processing type.

    The function processes the metadata value according to the type specified
    in `how_to_process`. It supports various types, including numerical,
    boolean, string, and datetime conversions. If an unsupported type is
    provided, the function raises an error.

    Parameters:
        key (str): The metadata key associated with the value, used for error reporting.
        value (str): The metadata value to be parsed.
        how_to_process (str): Specifies the type of processing to apply to the value.
                              Supported values are:
                              - "float": Converts the value to a float. Assumes the value contains
                                a number followed by optional text.
                              - "int": Converts the value to an integer.
                              - "bool": Converts the value to a boolean using `string_to_bool(value)`.
                              - "str": Returns the value as a string.
                              - "datetime": Converts the value to a datetime object using
                                `unix_time_to_datetime(value)`.

    Returns:
        float | int | bool | str | datetime: The parsed metadata value in the specified format.

    Raises:
        ValueError: If `how_to_process` is not one of the supported types.

    Example:
        Suppose you have the following metadata:

        >>> parse_metadata("temperature", "36.5 °C", "float")
        36.5

        >>> parse_metadata("is_active", "true", "bool")
        True

        >>> parse_metadata("timestamp", "1609459200", "datetime")
        datetime.datetime(2021, 1, 1, 0, 0)

        If an unsupported type is specified:
        >>> parse_metadata("unknown_key", "value", "unsupported")
        ValueError: Unhandled metadata key: unknown_key
    """
    # Parse based on key group
    if how_to_process == "float":
        return float(value.split(" ")[0])
    if how_to_process == "int":
        return int(value)
    if how_to_process == "bool":
        return string_to_bool(value)
    if how_to_process == "str":
        return value
    if how_to_process == "datetime":
        return unix_time_to_datetime(value)

    # Explicitly handle unhandled keys
    raise ValueError(f"Unhandled metadata key: {key}")


def get_keys(procedure: str) -> List[str]:
    """
    Retrieves a list of keys associated with a specific procedure.

    This function accesses the global `procedures` dictionary to retrieve
    the "Parameters" and "Metadata" associated with the given procedure name.
    It combines these two lists and returns the result.

    Note: The `procedures` dictionary is expected to be defined globally
    outside the scope of this function, with the following structure:
    ```
    procedures = {
        "procedure_name": {
            "Parameters": [...],
            "Metadata": [...]
        }
    }
    ```

    Args:
        procedure (str): The name of the procedure to retrieve keys for.

    Returns:
        List[str]: A combined list of keys from the "Parameters" and "Metadata"
                   of the specified procedure.

    Raises:
        KeyError: If the specified procedure is not found in `procedures`.
        TypeError: If the "Parameters" or "Metadata" for the procedure are not lists.

    Example:
        Global `procedures` dictionary:
        ```
        procedures = {
            "example_procedure": {
                "Parameters": ["param1", "param2"],
                "Metadata": ["meta1", "meta2"]
            }
        }
        ```

        Usage:
        >>> get_keys("example_procedure")
        ['param1', 'param2', 'meta1', 'meta2']

        Invalid Procedure:
        >>> get_keys("unknown_procedure")
        KeyError: "Procedure 'unknown_procedure' not found in procedures."

        Invalid Parameters/Metadata Type:
        If "Parameters" or "Metadata" are not lists:
        >>> get_keys("invalid_procedure")
        TypeError: "Expected 'Parameters' and 'Metadata' to be lists in procedure 'invalid_procedure'."
    """
    if procedure not in procedures:
        raise KeyError(f"Procedure '{procedure}' not found in procedures.")

    parameters = list(procedures[procedure]["Parameters"].keys())
    metadata = list(procedures[procedure]["Metadata"].keys())

    if not isinstance(parameters, list) or not isinstance(metadata, list):
        raise TypeError(f"Expected 'Parameters' and 'Metadata' to be lists in procedure '{procedure}'.")

    return parameters + metadata


def make_props_data(path: str) -> Tuple[pd.Series, pd.DataFrame]:
    """
    Parses properties and reads data from a file based on the procedure definition.

    This function extracts properties from a file using `read_comment_lines`,
    retrieves the relevant keys for the procedure with `get_keys`, and parses
    the metadata into a pandas Series. It also reads the data portion of the
    file into a DataFrame, applying the appropriate data types.

    Additionally, it adds a column in the properties Series containing the file path.

    Args:
        path (str): The file path to parse.

    Returns:
        Tuple[pd.Series, pd.DataFrame]:
            - A pandas Series containing the parsed properties, including the file path.
            - A pandas DataFrame containing the data portion of the file with
              appropriate data types applied.

    Raises:
        KeyError: If a required key is missing in the parsed properties.
        ValueError: If there is an issue reading the data portion of the file.
    """
    # Extract properties and header
    dictionary_found_properties, header = read_comment_lines(path)

    # Determine the procedure
    procedure = determine_procedure(path)

    # Handle properties
    keys = get_keys(procedure)
    procedure_dict = procedures[procedure]["Parameters"] | procedures[procedure]["Metadata"]
    props_dict = {}

    for key in keys:
        if key in dictionary_found_properties:
            props_dict[key] = parse_metadata(key, dictionary_found_properties[key], procedure_dict[key])
        else:
            raise KeyError(f"Key '{key}' is missing in the parsed properties from the file.")

    # Add the path to the properties dictionary
    props_dict["file_path"] = path

    # Convert the dictionary to a pandas Series
    props_series = pd.Series(props_dict)

    # Handle data
    dtype_mapping = procedures[procedure]["Data"]  # This is already pandas-compatible.
    try:
        data = pd.read_csv(path, header=header, dtype=dtype_mapping)
    except ValueError as e:
        raise ValueError(f"Error in reading {path} with dtype mapping: {dtype_mapping}") from e

    return props_series, data
