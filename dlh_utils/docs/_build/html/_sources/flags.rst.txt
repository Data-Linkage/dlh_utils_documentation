**Flags**
=====================

'''
Flag functions, used to quickly highlight anomalous values within data
'''
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import pandas as pd
from functools import reduce

###############################################################################


def flag(df, ref_col, condition, condition_value=None, condition_col=None,
         alias=None, prefix='FLAG', fill_null=None):
    """
    Adds True or False flags to supplied dataframe that can then be used for
    quality checks.

    Conditions can be set in comparison to columns or specific values
    (e.g. == column, ==1).  Conditions covered are equals, not equals,
    greater/less than, is/is not null. Optional TRUE/FALSE
    fill for null outputs of comparision. Designed for use in conjunction with
    flag_summary() and flag_check() functions.

    This function creates a column filled with TRUE or FALSE values
    based on whether a condition has been met.

    NOTE: If an alias is not specified, a Flag column name is automatically
    generated.

    Parameters
    ----------
    df : dataframe
      The dataframe the function is applied to.
    ref_col : string
      The column title that the conditions are
      performing checks upon.
    condition : {'==','!=','>','>=',<=','<','isNull','isNotNull'}
      Conditional statements used to compare values to the
      ref_col.
    condition_value : data-types, default = None
      The value the ref_col is being compared against.
    condition_col : data-types, default = None
      Comparison column for flag condition
    alias : string, default = None
      Alias for flag column.
    prefix : string, default = 'FLAG'
      Default alias flag column prefix.
    fill_null : bool, default = False
      True or False fill where condition operations
      return null.

    Returns
    -------
    dataframe
      Dataframe with additional window column.

    Raises
    ------
    None at present.

    Example
    -------

    > df.show()

    +---+--------+----------+-------+----------+---+--------+
    | ID|Forename|Middlename|Surname|       DoB|Sex|Postcode|
    +---+--------+----------+-------+----------+---+--------+
    |  1|   Homer|       Jay|Simpson|1983-05-12|  M|ET74 2SP|
    |  2|   Marge|    Juliet|Simpson|1983-03-19|  F|ET74 2SP|
    |  3|    Bart|     Jo-Jo|Simpson|2012-04-01|  M|ET74 2SP|
    |  3|    Bart|     Jo-Jo|Simpson|2012-04-01|  M|ET74 2SP|
    |  4|    Lisa|     Marie|Simpson|2014-05-09|  F|ET74 2SP|
    |  5|  Maggie|      null|Simpson|2021-01-12|  F|ET74 2SP|
    +---+--------+----------+-------+----------+---+--------+

    > flag(df,
           ref_col = 'Middlename',
           condition = 'isNotNull',
           condition_value=None,
           condition_col=None,
           alias = None,
           prefix='FLAG',
           fill_null = None).show()

    +---+--------+----------+-------+----------+---+--------+------------------------+
    | ID|Forename|Middlename|Surname|       DoB|Sex|Postcode|FLAG_MiddlenameisNotNull|
    +---+--------+----------+-------+----------+---+--------+------------------------+
    |  1|   Homer|       Jay|Simpson|1983-05-12|  M|ET74 2SP|                    true|
    |  2|   Marge|    Juliet|Simpson|1983-03-19|  F|ET74 2SP|                    true|
    |  3|    Bart|     Jo-Jo|Simpson|2012-04-01|  M|ET74 2SP|                    true|
    |  3|    Bart|     Jo-Jo|Simpson|2012-04-01|  M|ET74 2SP|                    true|
    |  4|    Lisa|     Marie|Simpson|2014-05-09|  F|ET74 2SP|                    true|
    |  5|  Maggie|      null|Simpson|2021-01-12|  F|ET74 2SP|                   false|
    +---+--------+----------+-------+----------+---+--------+------------------------+
    """

    if (alias is None
            and condition_value is not None):

        alias_value = str(condition_value)
        alias = f"{prefix}_{ref_col}{condition}{alias_value}"

    if (alias is None
            and condition_col is not None):
        alias = f"{prefix}_{ref_col}{condition}_{condition_col}"

    if (alias is None
        and condition_col is None
            and condition_value is None):
        alias = f"{prefix}_{ref_col}{condition}"

    if (condition == '=='
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) == F.col(condition_col))

    if (condition == '=='
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) == condition_value)

    if (condition == '>'
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) > F.col(condition_col))

    if (condition == '>'
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) > condition_value)

    if (condition == '>='
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) >= F.col(condition_col))

    if (condition == '>='
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) >= condition_value)

    if (condition == '<'
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) < F.col(condition_col))

    if (condition == '<'
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) < condition_value)

    if (condition == '<='
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) <= F.col(condition_col))

    if (condition == '<='
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) <= condition_value)

    if (condition == '!='
            and condition_col is not None):
        df = df.withColumn(alias,
                           F.col(ref_col) != F.col(condition_col))

    if (condition == '!='
            and condition_col is None):
        df = df.withColumn(alias,
                           F.col(ref_col) != condition_value)

    if condition == 'isNull':
        df = df.withColumn(alias,
                           (F.col(ref_col).isNull()) | (
                               F.isnan(F.col(ref_col)))
                           )

    if condition == 'isNotNull':
        df = df.withColumn(alias,
                           (F.col(ref_col).isNotNull()) & (
                               F.isnan(F.col(ref_col)) == False)
                           )

    if fill_null is not None:
        df = (df
              .withColumn(alias,
                          F.when(F.col(alias).isNull(),
                                 fill_null)
                          .otherwise(F.col(alias)))
              )

    return df
###############################################################################