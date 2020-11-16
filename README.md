![covid19_il](https://github.com/natylaza89/covid19_il/blob/main/pic/covid19_il_banner.png)

# covid19_il
python package which brings a "Facade" interface for using official covid19 israeli data gov's data.

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
2. Install covid19_il package via CMD/Terminal:
```
pip install covid19_il
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
    cities_top_cases_results = cities_data_handler.top_cases_in_cities_by_date('2020-11-01')
    print(cities_top_cases_results)
```

Output:
```
defaultdict(<function Cities._get_top_cases_statistics.<locals>.<lambda> at 0x040A6460>, {'Cumulative_verified_cases': defaultdict(<class 'int'>, {'אבו סנאן': 577, 'אבו גוש': 217, "אבו ג'ווייעד (שבט)": 14, 'אבו קורינאת (שבט)': 14}), 'Cumulated_recovered': defaultdict(<class 'int'>, {'אבו סנאן': 548, 'אבו גוש': 212, "אבו ג'ווייעד (שבט)": 14, 'אבו קורינאת (שבט)': 0}), 'Cumulated_deaths': defaultdict(<class 'int'>, {'אבו סנאן': 14, "אבו ג'ווייעד (שבט)": 0, 'אבו גוש': 0, 'אבו קורינאת (שבט)': 0}), 'Cumulated_number_of_tests': defaultdict(<class 'int'>, {'אבו סנאן': 7292, 'אבו גוש': 4836, 'אבו קורינאת (שבט)': 363, "אבו ג'ווייעד (שבט)": 271}), 'Cumulated_number_of_diagnostic_tests': defaultdict(<class 'int'>, {'אבו סנאן': 6828, 'אבו גוש': 4676, 'אבו קורינאת (שבט)': 363, "אבו ג'ווייעד (שבט)": 269})})

```
## # TODO:
1. Documentation of the package's API for ease of use.