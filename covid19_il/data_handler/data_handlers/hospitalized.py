import inspect
from collections import defaultdict
from functools import lru_cache
from pandas.core.series import Series
from pandas import DataFrame
from typing import Tuple, Dict, DefaultDict, Generator

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Hospitalized(DataHandler):
    """ Covid19_IL Hospitalized Data Handler.

    Attributes:
        None.

    Methods:
        _arrange_data_before_processing(self, df, method_name: str): Get df columns name list for group by then return
            a series with unique items
        hospitalized_total_stats(self): Yields Hospitalized Total Stats data.
        hospitalized_stats_by_date(self, date: str): Yields Hospitalized statistics by given date.

    """

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _arrange_data_before_processing(self, df: DataFrame, method_name: str) -> Tuple[Series, list]:
        """ Get df columns name list for group by then return a series with unique items.

        Args:
            df(DataFrame): the data frame itself.
            method_name(str): the name of the method which called this helper method.

        Returns:
            _(Tuple[Series, list]): unique values of series grouped by & data frame's columns.

        """

        df_columns = df.columns.tolist()
        if method_name == "hospitalized_total_stats":
            ser = df.groupby([*df_columns])['תאריך']
            df_columns.remove('תאריך')
        else:
            df_columns.remove('תאריך')
            ser = df.groupby([*df_columns])['תאריך']

        return ser.unique(), df_columns

    def hospitalized_total_stats(self) -> Generator[DefaultDict[str, Dict[str, float or int]], None, None] or \
                                          Generator[Tuple[str, str], None, None]:
        """ Yields Hospitalized Total Stats data.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, float or int]] or Tuple[str, str]: hospitalized_total_stats's data or "No Data" as
                bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            data, df_columns = \
                self._arrange_data_before_processing(df, method_name=inspect.currentframe().f_code.co_name)
            data_dict = defaultdict(lambda: dict())

            for key, _ in data.items():
                # key[0] - date, key[1:] - data/values
                date_as_clean_key = key[0].split('T')[0]
                data_dict[date_as_clean_key] =\
                    {column_name: data for (column_name, data) in zip(df_columns, key[1:])}

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    @lru_cache
    def hospitalized_stats_by_date(self, date: str) \
            -> Generator[Dict[str, float or int or str], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields Hospitalized statistics by given date.

        Args:
            date(str): required date for data processing.

        Yields:
            Tuple[str, float or int or str] or Tuple[str, str]: hospitalized_stats_by_date's data or "No Data" as bad
                result.

        Raises:
            ValueError: exception raises when date string isn't in a valid pattern like: "2020-10-03".

        """

        self._date_validation(pattern='(202[0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$',
                              date=date)

        data_dict = None
        try:
            date += "T00:00:00"
            df = self._get_clean_copy_df_data()
            df = df[df['תאריך'] == date]
            data, df_columns = \
                self._arrange_data_before_processing(df, method_name=inspect.currentframe().f_code.co_name)
            data_dict = {column_name: data for (column_name, data) in zip(df_columns, *data.keys())}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""
