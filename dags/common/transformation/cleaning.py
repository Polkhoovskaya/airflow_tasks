def drop_nulls(df, column: str):
    return df.dropna(subset=[column])