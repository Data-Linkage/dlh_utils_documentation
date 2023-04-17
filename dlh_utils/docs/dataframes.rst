**Dataframes**
=====================

import re
import pyspark.sql.functions as F
from pyspark.sql import Window
from dlh_utils import standardisation as st

##################################################################################

**dropColumns()**
------------------------

def select(df, columns=None, startswith=None, endswith=None, contains=None,
           regex=None, drop_duplicates=True):
    """
    Retains only specified list of columns or columns meeting startswith,
    endswith, contains or regex arguments.

    Parameters
    ----------
    df : dataframe
      Dataframe to which the function is applied.

    columns : string or list of strings, default = None
      This argument can be entered as a list of column headers
      that are the columns to be selected. If a single string
      that is a name of a column is entered, it will select
      only that column.

    startswith : string, default = None
      This parameter takes a string value and selects
      columns from the dataframe if the column
      title starts with the string value.

    endswith : string, default = None
      This parameter takes a string value and selects
      columns from the dataframe if the column
      title ends with the string value.

    contains : string, default = None
      This parameter takes a string value and selects
      columns from the dataframe if the column
      title contains the string value.

    regex : string, default = None
      This parameter takes a string value in
      regex format and selects columns from the
      dataframe if the column title matches
      the conditions of the regex string.

    drop_duplicates : bool, default = True
      This parameter drops duplicated columns.

    Returns
    -------
    dataframe
      Dataframe with columns limited to those
      specified by the parameters.

    Raises
    ------
    None at present.

    Example
    -------

    data = [("1","6","1","Simpson","1983-05-12","M","ET74 2SP"),
            ("2","8","2","Simpson","1983-03-19","F","ET74 2SP"),
            ("3","7","3","Simpson","2012-04-01","M","ET74 2SP"),
            ("3","9","3","Simpson","2012-04-01","M","ET74 2SP"),
            ("4","9","4","Simpson","2014-05-09","F","ET74 2SP"),
            ("5","6","4","Simpson","2021-01-12","F","ET74 2SP")]
    df=spark.createDataFrame(data=data,schema=["ID","ID2","clust","ROWNUM","DoB","Sex","Postcode"])

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

    > select(df,columns = None, startswith = 'F').show()
      +--------+
      |Forename|
      +--------+
      |   Homer|
      |   Marge|
      |  Maggie|
      |    Bart|
      |    Lisa|
      +--------+

    > select(df,columns = None, endswith = 'e',drop_duplicates = False).show()
     +--------+----------+-------+--------+
     |Forename|Middlename|Surname|Postcode|
     +--------+----------+-------+--------+
     |   Homer|       Jay|Simpson|ET74 2SP|
     |   Marge|    Juliet|Simpson|ET74 2SP|
     |    Bart|     Jo-Jo|Simpson|ET74 2SP|
     |    Bart|     Jo-Jo|Simpson|ET74 2SP|
     |    Lisa|     Marie|Simpson|ET74 2SP|
     |  Maggie|      null|Simpson|ET74 2SP|
     +--------+----------+-------+--------+

    > select(df,columns = None, contains = 'name').show()
     +--------+----------+-------+
     |Forename|Middlename|Surname|
     +--------+----------+-------+
     |    Bart|     Jo-Jo|Simpson|
     |   Marge|    Juliet|Simpson|
     |   Homer|       Jay|Simpson|
     |    Lisa|     Marie|Simpson|
     |  Maggie|      null|Simpson|
     +--------+----------+-------+

     > select(df,columns = None, regex = '^[A-Z]{2}$').show()
     +---+
     | ID|
     +---+
     |  3|
     |  5|
     |  1|
     |  4|
     |  2|
     +---+

    """
    if columns is not None:
        df = df.select(columns)

    if startswith is not None:
        df = df.select(
            [x for x in df.columns if x.startswith(startswith)]
        )

    if endswith is not None:
        df = df.select(
            [x for x in df.columns if x.endswith(endswith)]
        )

    if contains is not None:
        df = df.select(
            [x for x in df.columns if contains in x]
        )

    if regex is not None:
        df = df.select(
            [x for x in df.columns if re.search(regex, x)]
        )

    if drop_duplicates:
        df = df.dropDuplicates()

    return df