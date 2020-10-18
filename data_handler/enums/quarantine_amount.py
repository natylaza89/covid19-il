from enum import Enum


class QuarantineAmount(Enum):
    """ Quarantine Data Amount using Enum """
    isolated_today_contact_with_confirmed = 1
    isolated_today_abroad = 2
    new_contact_with_confirmed = 3
    new_from_abroad = 4
