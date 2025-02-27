from datetime import datetime
from typing import Any, get_args, get_origin, get_type_hints

from adh_sample_library_preview import SdsUom, UomValueInput

class CSVTransformer:
    def __init__(
        self,
        event_class: type,
        units_of_measure: dict[str, SdsUom] = None,
    ):
        self.__event_class = event_class
        self.__datetime_fields = set()
        self.__units_of_measure = units_of_measure

    def transform_row(self, row: dict):
        if not row:
            return None
        new_row = {}
        properties = get_type_hints(self.__event_class)
        for name, value in row.items():
            new_row.update(
                {name: self.__transform_value(name, value, properties[name])}
            )
        return new_row, self.__datetime_fields
    
    def __transform_value(self, name: str, value: Any, value_type: type):
        try:
            if value_type == datetime:
                self.__datetime_fields.add(name)
                return datetime.fromisoformat(value)
            elif value == '' and (value_type == float or value_type == int):
                return None
            if get_origin(value_type) is UomValueInput:
                if self.__units_of_measure and name in self.__units_of_measure:
                    return UomValueInput(
                        self.__transform_value(
                            'Value',
                            value,
                            get_args(value_type)[0],
                        ),
                        self.__units_of_measure[name],
                    )
                else:
                    return UomValueInput(
                        self.__transform_value(
                            'Value',
                            value,
                            get_args(value_type)[0],
                        ),
                        None,
                    )
            return value_type(value)
        except:
            pass

        return value

