import json

def pretty_print_json(object_to_serialize: dict[str, str]) -> None:
    """Helper for more readable printing in JSON format."""
    print(json.dumps(object_to_serialize, indent=4, sort_keys=True))

def get_counts_in_dict_list(dict_list: dict) -> dict:
    """Given a dictionary of lists (or other iterable), return a new dictionary where the values are the lengths of each list."""
    counts = {}
    for key in dict_list:
        counts[key] = len(dict_list[key])
    return counts