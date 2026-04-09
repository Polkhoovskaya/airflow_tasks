def load_to_mssql(df, table_name, conn_id="mssql_local"):
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    hook = MsSqlHook(mssql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
    )