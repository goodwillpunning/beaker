import re

def parse_sql_file(file_path):
    """
    Parses a SQL file and returns a list of tuples with the query_id and the query text.

    Parameters:
    file_path (str): The path to the SQL file.

    Returns:
    list: A list of tuples, where each tuple contains a query_id and a query text.
    """
    with open(file_path, 'r') as file:
        content = file.read()

    pattern = r'-- {"query_id":"(.*?)"}\n(.*?);'
    matches = re.findall(pattern, content, re.DOTALL)

    return matches


queries = parse_sql_file('q1.sql')
print(queries)