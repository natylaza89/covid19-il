from random import randint
import json
from functools import lru_cache
from typing import Dict, List

from covid19_il.data_handler.data_handlers.data_handler import DataHandler
from covid19_il.logger.logger import Logger
from covid19_il.data_handler.enums.area_event import AreaEvent


class Area(DataHandler):
    """ Covid19_IL Area Data Handler.

    Attributes:
        None.

    Methods:
        get_data_by_event_type(self, event_type: AreaEvent): get data of new events by town agas code.
        _string_parser(self, str_key: str) -> List[str]: Overridden Method - clean string from unnecessary chars
         for better presentation.
        _get_data_by_column(self, column_name: str, ascending_order: bool = True): returns a dictionary of top total
         amount of given column name via data frame.
        get_accumulated_tested_by_town(self, ascending_order: bool = True): returns accumulated tested amount by town
         stored in a dictionary.
        get_hospitalized_amount(self, ascending_order: bool = True): returns accumulated tested amount by town stored
         in a dictionary.
        get_accumulated_recoveries_amount(self, ascending_order: bool = True): returns accumulated recoveries amount
         stored in a dictionary.

    """

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def get_data_by_event_type(self, event_type: AreaEvent) -> Dict or None:
        """ Get data of new events by town agas code.

        Args:
            event_type(AreaEvent): event type(enum) which determined which data to get returns.

        Returns:
            data_dict(dict or none): data event stored in a dictionary.

        Raises:
            KeyError: concrete error which can occurred if data frame can't be access by given key.
        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['town', 'agas_code', event_type.name.lower()]]
            ser_group_by = df.groupby(['town', 'agas_code'])[event_type.name.lower()]
            data_dict = json.loads(ser_group_by.unique().to_json())
            data_dict = {tuple(self._string_parser(key)): value[0] for key, value in data_dict.items()}
        except KeyError as ke:
            self._logger.exception(ke)
            # TODO: need to check in the tests if it get raised.
            raise
        finally:
            return data_dict

    def _string_parser(self, str_key: str) -> List[str]:
        """ Parse & Clean string from unnecessary chars for better presentation.

        Note:
            Overridden Method: private method which get called by get_data_by_event_type's method.

        Args:
            str_key(str): a string which arrives from key's dictionary.

        Returns:
            _(List[str]): cleaned string.

        """

        return str_key.translate({ord('('): None,
                                  ord(')'): None,
                                  ord("'"): None})\
                      .split(", ")

    def _get_data_by_column(self, group_by_column: str, ascending_order: bool = True) -> Dict:
        """ Returns a Dictionary of Top Total Amount of given column name via DataFrame Data.

        Note:
            Overridden Method: private method which get called by the other methods by given event type.

        Args:
            group_by_column(str): column name of event type.
            ascending_order(bool): final result's ordering by de/ascending.

        Returns:
            data_dict(Dict): top total amount of given column(by event type) stored in a dictionary.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            # data which is under 15, replace it with a random number
            df[group_by_column] = df[group_by_column].apply(
                lambda input_string: int(input_string) if input_string != '<15' else randint(0, 15))
            df = df[['town', group_by_column]]
            ser_group_by = df.groupby('town')[group_by_column].unique()
            ser = ser_group_by.apply(lambda item: sum(item))
            ser = ser.sort_values(ascending=ascending_order)
            data_dict = {key: value for key, value in ser.items()}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache(maxsize=3)
    def get_accumulated_tested_by_town(self, ascending_order: bool = True) -> Dict or None:
        """ Returns accumulated tested amount by town stored in a dictionary.

        Args:
            ascending_order(bool): final result's ordering by de/ascending.

        Returns:
            data_dict(Dict): accumulated tested amount by town stored in a dictionary.

        """

        return self._get_data_by_column('accumulated_tested', ascending_order)

    @lru_cache(maxsize=3)
    def get_hospitalized_amount(self, ascending_order: bool = True) -> Dict or None:
        """ Returns hospitalized amount stored in a dictionary.

        Args:
            ascending_order(bool): final result's ordering by de/ascending.

        Returns:
            data_dict(Dict): accumulated tested amount by town stored in a dictionary.

        """

        return self._get_data_by_column('accumulated_hospitalized', ascending_order)

    @lru_cache(maxsize=3)
    def get_accumulated_recoveries_amount(self, ascending_order: bool = True) -> Dict or None:
        """ Returns accumulated recoveries amount stored in a dictionary.

        Args:
            ascending_order(bool): final result's ordering by de/ascending.

        Returns:
            data_dict(Dict): accumulated tested amount by town stored in a dictionary.

        """

        return self._get_data_by_column('accumulated_recoveries', ascending_order)
