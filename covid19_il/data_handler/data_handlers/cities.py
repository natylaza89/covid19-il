from collections import namedtuple, defaultdict
from datetime import datetime as dt
from functools import lru_cache
from typing import Dict, NamedTuple, Tuple, DefaultDict, AnyStr, Generator

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Cities(DataHandler):
    """ Covid19_IL Cities Data Handler.

    Attributes:
        None.

    Methods:
        cities_by_date(self, date: str = dt.strftime(dt.now(), format="%Y-%m-%d")): Returns calculated cities as a
            generator of namedtuple with city's data props via given date in format ike: '2020-10-03'. if it has no
            data, it yields "No Data" string as bad result.
        _get_top_cases_statistics(self, cities_fields: Tuple[AnyStr]): Helper Method of other class's method for
            calculation.
        top_cases_in_cities(self): Returns top cities with 5 calculated properties or "No Data" as bad result.
        cases_statistics(self): returns cases statistics.

    """

    fields = ("City_name", "City_code", "Date", "Cumulative_verified_cases", "Cumulated_recovered",
              "Cumulated_deaths", "Cumulated_number_of_tests", "Cumulated_number_of_diagnostic_tests")
    city = namedtuple("City", fields, defaults=(None,) * len(fields))

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache(maxsize=None)
    def cities_by_date(self, date: str = dt.strftime(dt.now(), format="%Y-%m-%d")) \
            -> Generator[NamedTuple, None, None] or Generator[str, None, None]:
        """ Returns calculated cities as a generator of namedtuple with city's data props via given date in format
            like: '2020-10-03'. if it has no data, it yields "No Data" string as bad result.

        Args:
            date(str): today's date as a string.

        Returns:
            data_dict(dict or none): cities by date data stored in a dictionary.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df["Date"] == date]
            data = df.groupby([*df.columns])["Date"].unique()
            data_dict = defaultdict(NamedTuple)

            for key in data.keys():
                data_dict[key[0]] = Cities.city(*key)
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                for city in data_dict.items():
                    yield city
            else:
                yield "No Data"

    def _get_top_cases_statistics(self, cities_fields: Tuple[AnyStr]) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Helper Method of top_cases_in_cities method for calculation & data manipulation.

        Note:
            private method which get called by top_cases_in_cities's method.
        Args:
            None.

        Returns:
            data_dict(Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]):
                top cities statistics data holder or "No Data" as bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            for column_name in cities_fields:
                df[column_name] = [self._convert_string_to_int(item) for item in df[column_name]]

            data_dict = defaultdict(lambda: defaultdict(int))
            for field in cities_fields:
                temp_df = df[["Date", "City_Name", field]]
                temp_df = temp_df.sort_values(['Date', field], ascending=False).head(10)

                for item in temp_df.values:
                    # item[1]: city, item[2]: amount
                    data_dict[field][item[1]] = item[2]

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                for city in data_dict.items():
                    yield city
            else:
                yield "No Data"

    @lru_cache
    def top_cases_in_cities(self)\
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Returns top cities with 5 calculated properties or "No Data" as bad result.

        Args:
            None.

        Returns:
            _(Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]):
                top cities statistics data holder or "No Data" as bad result.

        """

        return self._get_top_cases_statistics(Cities.fields[3:])

    @lru_cache
    def cases_statistics(self) \
            -> Generator[Dict[str, Dict[str, int or float]], None, None] or Generator[str, None, None]:
        """ Returns cases statistics .

        Note:
            use inherited methods from base class.
        Args:
            None.

        Returns:
            _(Generator[Dict[str, Dict[str, int or float]]]): top cities statistics data holder generator.

        """

        return self._get_statistics_by_columns_names(Cities.fields[3:])
