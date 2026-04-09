def users_summary(df):
    return {
        "total_rows_users": len(df),
        "unique_users": df["user_id"].nunique(),
        "unique_countries": df["country"].nunique()
    }