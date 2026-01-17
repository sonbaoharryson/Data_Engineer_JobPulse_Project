from .ge_runner import run_ge_validation

def expectations(df):
    df.expect_table_row_count_to_be_between(1, 500)
    df.expect_column_values_to_not_be_null("url")
    df.expect_column_values_to_be_unique("url")
    df.expect_column_values_to_not_be_null("descriptions")
    df.expect_column_values_to_not_be_null("requirements")
    df.expect_column_values_to_match_regex(
        "url",
        r"^https://"
    )