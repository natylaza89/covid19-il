from enum import Enum


class AreaEvent(Enum):
    """ Area Event Types using Enum """
    NEW_TESTED_ON_DATE = 1
    NEW_CASES_ON_DATE = 2
    NEW_RECOVERIES_ON_DATE = 3
    NEW_HOSPITALIZED_ON_DATE = 4
    NEW_DEATHS_ON_DATE = 5
