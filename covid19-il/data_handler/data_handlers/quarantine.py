from functools import lru_cache
from typing import Dict

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler
from covid19_il.data_handler.enums.quarantine_amount import QuarantineAmount


class Quarantine(DataHandler):
    """ Covid19_IL Quarantine Data Handler.

    Attributes:
        None.

    Methods:
        isolated_today_contact_with_confirmed(self): get a dictionary of date: amount of isolated today_contact with
            confirmed.
        isolated_today_abroad(self): get a dictionary of date: amount of isolated today abroad.
        new_contact_with_confirmed(self): get a dictionary of date: amount of new contact with confirmed.
        new_from_abroad(self): get a dictionary of date: amount of new from abroad.

    """

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache
    def isolated_today_contact_with_confirmed(self) -> Dict:
        """ Get a dictionary of date: amount of isolated today_contact with confirmed.

        Args:
            None.

        Returns:
            _(Dict): desired data in data holder.

        """

        return self._get_data_by_column(QuarantineAmount.isolated_today_contact_with_confirmed.name,
                                        ascending_order=True)

    @lru_cache
    def isolated_today_abroad(self) -> Dict:
        """ Get a dictionary of date: amount of isolated today abroad.

        Args:
            None.

        Returns:
            _(Dict): desired data in data holder.

        """

        return self._get_data_by_column(QuarantineAmount.isolated_today_abroad.name, ascending_order=True)

    @lru_cache
    def new_contact_with_confirmed(self) -> Dict:
        """ Get a dictionary of date: amount of new contact with confirmed.

        Args:
            None.

        Returns:
            _(Dict): desired data in data holder.

        """

        return self._get_data_by_column(QuarantineAmount.new_contact_with_confirmed.name, ascending_order=True)

    @lru_cache
    def new_from_abroad(self) -> Dict:
        """ Get a dictionary of date: amount of new from abroad.

        Args:
            None.

        Returns:
            _(Dict): desired data in data holder.

        """

        return self._get_data_by_column(QuarantineAmount.new_from_abroad.name, ascending_order=True)
