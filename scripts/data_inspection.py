from pyspark.sql.functions import *


def inspect_data(df):

    print("\nFirst 10 Rows")
    df.show(10)

    print("\nSchema")
    df.printSchema()

    print("\nTotal Rows")
    print(df.count())

    print("\nColumns")
    print(df.columns)

    #null value check 

    print("\nNull Values")

    df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).show()

    #duplicates check

    print("\nDuplicate Count")

    duplicate_count = (
        df.count()
        - df.dropDuplicates().count()
    )

    print(duplicate_count)

    #categorical discrepancy anomaly check

    print("\nStatus Values")

    df.select("Status") \
        .distinct() \
        .show(truncate=False)

    print("\nPerformance Values")

    df.select("Performance_Score") \
        .distinct() \
        .show(truncate=False)

    print("\nDepartment Region Values")

    df.select("Department_Region") \
        .distinct() \
        .show(50, truncate=False)

    # check for age related discrepancy

    print("\nAge Range")

    df.select(
        min("Age"),
        max("Age")
    ).show()

    #check for data related discrepancy 

    print("\nDate Samples")

    df.select("Join_Date") \
        .distinct() \
        .show(30, truncate=False)

    #check for salary realted discrepancy

    print("\nSalary Samples")

    df.select("Salary") \
        .distinct() \
        .show(30, truncate=False)
    
print("Successfully inspected data");    