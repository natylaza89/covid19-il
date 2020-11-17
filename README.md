![covid19-il](https://raw.githubusercontent.com/natylaza89/covid19_il/main/pic/covid19_il_banner.png)

# covid19-il
python package which brings a "Facade" interface for using official covid19 israeli data gov's data.

<div align="center">

[![Stars](https://img.shields.io/github/stars/natylaza89/covid19-il?style=social)](https://pepy.tech/project/covid19-il)
[![Forks](https://img.shields.io/github/forks/natylaza89/covid19-il?style=social)](https://pepy.tech/project/covid19-il)

[![LastCommit](https://img.shields.io/github/last-commit/natylaza89/covid19-il/main)](https://pepy.tech/project/covid19-il)
[![PyPI Latest Release](https://img.shields.io/pypi/v/covid19-il.svg)](https://pypi.org/project/covid19-il/)
[![Package Status](https://img.shields.io/pypi/status/pandas.svg)](https://pypi.org/project/covid19-il/)
[![Downloads](https://img.shields.io/pypi/dm/covid19-il)](https://pepy.tech/project/covid19-il)
[![License](https://img.shields.io/pypi/l/covid19-il.svg)](https://github.com/natylaza89/covid19_il/blob/main/LICENSE)
</div>

## Dependencies
1. pandas
2. numpy
3. requests

## How to Use
Requirements: Python must already be installed.
1. Install requirements via CMD/Terminal:
```
pip install -r requirements.txt
```
2. Install covid19-il package via CMD/Terminal:
```
pip install covid19-il
```

## Example
Simple example of using the package's API:
```
from covid19_il.api_handler.api_factory.api_enum import ApiEnum
from covid19_il.api_handler.api_factory.api_factory import ApiFactory
from covid19_il.data_handler.data_handlers_factory.data_handler_factory import DataHandlerFactory
from covid19_il.data_handler.enums.resource_id import ResourceId


api_client = ApiFactory.create_api_client(ApiEnum.api_data_il)
if api_client:
    data = api_client.get_data_by_resource_id(enum_resource_id=ResourceId.CITIES_POPULATION_RESOURCE_ID,
                                              limit=1000,
                                              include_total=True)
    cities_data_handler = DataHandlerFactory.get_instance(
        ResourceId.CITIES_POPULATION_RESOURCE_ID,
        data)
    # Printing results from a generator function
    for city in cities_data_handler.top_cases_in_cities():
        print(city)

```

Output:
```
('Cumulative_verified_cases', defaultdict(<class 'int'>, {'אבו סנאן': 587, 'אבו גוש': 223, "אבו ג'ווייעד (שבט)": 14}))
('Cumulated_recovered', defaultdict(<class 'int'>, {'אבו סנאן': 564, 'אבו גוש': 215, "אבו ג'ווייעד (שבט)": 14}))
('Cumulated_deaths', defaultdict(<class 'int'>, {'אבו סנאן': 14, "אבו ג'ווייעד (שבט)": 0, 'אבו גוש': 0}))
('Cumulated_number_of_tests', defaultdict(<class 'int'>, {'אבו סנאן': 7608, 'אבו גוש': 5139, "אבו ג'ווייעד (שבט)": 290}))
('Cumulated_number_of_diagnostic_tests', defaultdict(<class 'int'>, {'אבו סנאן': 7130, 'אבו גוש': 4965, "אבו ג'ווייעד (שבט)": 288}))
```
## # TODO:
1. Refactor all methods returning type & value to a generator instead of a data type.0
2. refactor all tests according step 1.
2. Documentation of the package's API for ease of use using Sphinx.