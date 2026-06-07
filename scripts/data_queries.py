from pyspark.sql.functions import *


def run_analysis(df):

    print("\nFiltering Operations")

    print("\nEmployees Age 25–35")

    df.filter(
        col("Age").between(25, 35)
    ).show()

    print("\nTexas Employees")

    df.filter(
        col("Region") == "Texas"
    ).show()

    print("\nCloud Tech Employees")

    df.filter(
        col("Department")
        == "Cloud Tech"
    ).show()

    print("\nAggregation Operations")

    df.select(
        count("*").alias("Count"),
        sum("Monthly_Salary")
        .alias("Total Salary"),
        avg("Monthly_Salary")
        .alias("Average Salary"),
        min("Monthly_Salary")
        .alias("Minimum Salary"),
        max("Monthly_Salary")
        .alias("Maximum Salary")
    ).show()

    print("\nGroup by operations")

    df.groupBy("Region") \
        .agg(
            avg("Monthly_Salary")
            .alias(
                "Average Salary"
            )
        ).show()

    print(
        "\nRegions with avg salary > 85000"
    )

    df.groupBy("Region") \
        .agg(
            avg(
                "Monthly_Salary"
            ).alias(
                "Average Salary"
            )
        ) \
        .filter(
            col(
                "Average Salary"
            ) > 85000
        ) \
        .show()