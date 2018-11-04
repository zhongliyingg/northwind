import re

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')

def camel_to_snake(input_string):
    """
    Converts CamelCase strings to snake_case
    """
    s0 = re.sub('\s+', '', input_string)
    s1 = first_cap_re.sub(r'\1_\2', s0)
    return all_cap_re.sub(r'\1_\2', s1).lower()

def camel_to_snake_columns(df):
    """
    Convert CamelCase column names to snake_case  
    """      
    snake_column_name_list = list(map(lambda x: camel_to_snake(x), df.columns))
    return df.toDF(*snake_column_name_list)
    