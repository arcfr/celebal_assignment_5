from pyspark.sql.functions import *
import builtins


def clean_data(df):

    print("Data Cleaning")

    cleaning_report = {}

    # ==================================
    # REMOVE DUPLICATES
    # ==================================

    print("\nRemoving Duplicates")

    initial = df.count()

    df = df.dropDuplicates()

    final = df.count()

    duplicates_removed = (
        initial - final
    )

    cleaning_report[
        "Duplicates Removed"
    ] = duplicates_removed

    print(
        "Duplicates Removed:",
        duplicates_removed
    )

    # ==================================
    # REPLACE EMPTY VALUES
    # ==================================

    df = df.replace("", None)

    # ==================================
    # INVALID CATEGORICAL VALUES
    # ==================================

    invalid_values = [
        "nan",
        "null",
        "NAN",
        "NULL",
        ""
    ]

    # Count before cleaning
    status_invalid_count = df.filter(
        lower(
            trim(col("Status"))
        ).isin(
            [x.lower()
             for x in invalid_values]
        )
    ).count()

    performance_invalid_count = (
        df.filter(
            lower(
                trim(
                    col(
                        "Performance_Score"
                    )
                )
            ).isin(
                [x.lower()
                 for x in invalid_values]
            )
        ).count()
    )

    df = df.withColumn(
        "Status",
        when(
            lower(
                trim(
                    col("Status")
                )
            ).isin(
                [x.lower()
                 for x in invalid_values]
            ),
            None
        ).otherwise(
            col("Status")
        )
    )

    df = df.withColumn(
        "Performance_Score",
        when(
            lower(
                trim(
                    col(
                        "Performance_Score"
                    )
                )
            ).isin(
                [x.lower()
                 for x in invalid_values]
            ),
            None
        ).otherwise(
            col(
                "Performance_Score"
            )
        )
    )

    cleaning_report[
        "Status Cleaned"
    ] = status_invalid_count

    cleaning_report[
        "Performance Cleaned"
    ] = (
        performance_invalid_count
    )

    # ==================================
    # SALARY CLEANING
    # ==================================

    invalid_salary = [
        "n/a",
        "unknown",
        "null",
        ""
    ]

    salary_invalid_count = (
        df.filter(
            lower(
                trim(
                    col("Salary")
                )
            ).isin(
                [x.lower()
                 for x in invalid_salary]
            )
        ).count()
    )

    df = df.withColumn(
        "Salary",
        when(
            lower(
                trim(
                    col("Salary")
                )
            ).isin(
                [x.lower()
                 for x in invalid_salary]
            ),
            None
        ).otherwise(
            col("Salary")
        )
    )

    df = df.withColumn(
        "Salary",
        col("Salary")
        .cast("double")
    )

    cleaning_report[
        "Salary Cleaned"
    ] = salary_invalid_count

    # ==================================
    # TEXT STANDARDIZATION
    # ==================================

    df = df.withColumn(
        "Status",
        lower(
            trim(
                col("Status")
            )
        )
    )

    df = df.withColumn(
        "Performance_Score",
        lower(
            trim(
                col(
                    "Performance_Score"
                )
            )
        )
    )

    # ==================================
    # DATE CLEANING
    # ==================================

    df = df.withColumn(
        "Join_Date",
        regexp_replace(
            col("Join_Date"),
            r"[.-]",
            "/"
        )
    )

    df = df.withColumn(
        "Join_Date",
        coalesce(
            try_to_timestamp(
                col("Join_Date"),
                lit("M/d/yyyy")
            ),

            try_to_timestamp(
                col("Join_Date"),
                lit("d/M/yyyy")
            ),

            try_to_timestamp(
                col("Join_Date"),
                lit("yyyy/MM/dd")
            )
        ).cast("date")
    )

    # ==================================
    # SPLIT COLUMN
    # ==================================

    df = df.withColumn(
        "Department",
        when(
            col(
                "Department_Region"
            ).isNotNull(),
            split(
                col(
                    "Department_Region"
                ),
                "-"
            )[0]
        )
    )

    df = df.withColumn(
        "Region",
        when(
            col(
                "Department_Region"
            ).isNotNull(),
            split(
                col(
                    "Department_Region"
                ),
                "-"
            )[1]
        )
    )

    # ==================================
    # MEAN IMPUTATION
    # ==================================

    age_missing_before = (
        df.filter(
            col("Age")
            .isNull()
        ).count()
    )

    salary_missing_before = (
        df.filter(
            col("Salary")
            .isNull()
        ).count()
    )

    avg_age = df.select(
        avg("Age")
    ).collect()[0][0]

    avg_salary = df.select(
        avg("Salary")
    ).collect()[0][0]

    df = df.na.fill({
        "Age":
        builtins.round(
            avg_age
        ),

        "Salary":
        builtins.round(
            avg_salary,
            2
        )
    })

    cleaning_report[
        "Age Imputed"
    ] = age_missing_before

    cleaning_report[
        "Salary Imputed"
    ] = (
        salary_missing_before
    )

    # ==================================
    # MODE IMPUTATION
    # ==================================

    status_missing_before = (
        df.filter(
            col("Status")
            .isNull()
        ).count()
    )

    performance_missing_before = (
        df.filter(
            col(
                "Performance_Score"
            ).isNull()
        ).count()
    )

    department_missing_before = (
        df.filter(
            col("Department")
            .isNull()
        ).count()
    )

    region_missing_before = (
        df.filter(
            col("Region")
            .isNull()
        ).count()
    )

    status_mode = (
        df.groupBy("Status")
        .count()
        .filter(
            col("Status")
            .isNotNull()
        )
        .orderBy(
            desc("count")
        )
        .first()[0]
    )

    performance_mode = (
        df.groupBy(
            "Performance_Score"
        )
        .count()
        .filter(
            col(
                "Performance_Score"
            ).isNotNull()
        )
        .orderBy(
            desc("count")
        )
        .first()[0]
    )

    department_mode = (
        df.groupBy(
            "Department"
        )
        .count()
        .filter(
            col(
                "Department"
            ).isNotNull()
        )
        .orderBy(
            desc("count")
        )
        .first()[0]
    )

    region_mode = (
        df.groupBy("Region")
        .count()
        .filter(
            col("Region")
            .isNotNull()
        )
        .orderBy(
            desc("count")
        )
        .first()[0]
    )

    df = df.na.fill({
        "Status":
        status_mode,

        "Performance_Score":
        performance_mode,

        "Department":
        department_mode,

        "Region":
        region_mode
    })

    cleaning_report[
        "Status Imputed"
    ] = status_missing_before

    cleaning_report[
        "Performance Imputed"
    ] = (
        performance_missing_before
    )

    cleaning_report[
        "Department Imputed"
    ] = (
        department_missing_before
    )

    cleaning_report[
        "Region Imputed"
    ] = region_missing_before

    # ==================================
    # RENAME COLUMN
    # ==================================

    df = df.withColumnRenamed(
        "Salary",
        "Monthly_Salary"
    )

    # ==================================
    # CLEANING SUMMARY
    # ==================================

    print("\nCleaning Summary")

    for key, value in (
        cleaning_report.items()
    ):
        print(
            f"{key}: {value}"
        )

    print(
        "\nCleaning Complete"
    )

    return df