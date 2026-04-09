def validate_columns(df, required_columns: set):
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")