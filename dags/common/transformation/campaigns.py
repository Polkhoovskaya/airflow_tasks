import logging

def campaigns_summary(df):
    logging.info("Calculating CTR...")
    df["ctr"] = df["clicks"] / df["impressions"]
    
    logging.info("Calculating summary statistics...")
    return {
        "total_rows_campaigns": len(df),
        "total_spend": df["spend"].sum(),
        "avg_ctr": df["ctr"].mean()
    }