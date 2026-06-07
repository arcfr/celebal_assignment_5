from data_ingestion import (
    create_spark_session,
    load_data
)

from data_inspection import (
    inspect_data
)

from data_cleaning import (
    clean_data
)

from data_queries import (
    run_analysis
)


spark = create_spark_session()

df = load_data(spark)

inspect_data(df)

cleaned_df = clean_data(df)

run_analysis(cleaned_df)

spark.stop()