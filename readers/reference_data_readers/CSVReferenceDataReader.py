import uuid
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Generic, TypeVar

from adh_sample_library_preview import (
    AuthorizationTag,
    BaseReferenceData,
    Enumeration,
    ReferenceDataType,
    SdsUom,
)

from data_types import AuthorizationTagEnum, EnumEnum, ReferenceDataTypeEnum

from ..CSVTransformer import CSVTransformer
from ..CSVIterator import CSVIterator
from ..GraphData import GraphData
from .ReferenceDataReader import ReferenceDataReader

T = TypeVar('T')


class CSVReferenceDataReader(ReferenceDataReader, Generic[T]):
    def __init__(
        self,
        id: str,
        name: str,
        file_path: str,
        reference_data_class: type,
        reference_data_type: ReferenceDataType,
        authorization_tags: list[AuthorizationTag] = None,
        units_of_measure: dict[str, SdsUom] = None,
        enumerations: list[Enumeration] = None,
    ):
        super().__init__(id, name)
        self.__reference_data_class = reference_data_class
        self.__reference_data_type = reference_data_type
        self.__authorization_tags = authorization_tags if authorization_tags else []
        self.__enumerations = enumerations if enumerations else []
        self.__file_path = file_path
        self.__units_of_measure = units_of_measure
        self.__transformer = CSVTransformer(
            self.__reference_data_class,
            units_of_measure=units_of_measure
        )
        self.__iterator = CSVIterator(
            file_path,
            self.__transformer,
            'CreatedDate',
            should_loop=False
        )

    def get_authorization_tags(self) -> list[AuthorizationTag]:
        return self.__authorization_tags

    def get_enumerations(self) -> list[Enumeration]:
        return self.__enumerations

    def get_type(self) -> ReferenceDataType:
        return self.__reference_data_type

    def read_reference_data(self, now: datetime) -> Iterator[GraphData[T]]:
        data: BaseReferenceData
        for data in self.__iterator:
            data = self.__reference_data_class(**data)
            # assign a random id and name if the reference data does not have one
            if not data.Id:
                data.Id = str(uuid.uuid4())
            if not data.Name:
                data.Name = str(uuid.uuid4())
            yield GraphData([data], self.__reference_data_type.Id)

    @staticmethod
    def fromJson(content: dict[str, Any]) -> 'CSVReferenceDataReader':
        event_class = ReferenceDataTypeEnum[content['ReferenceDataClass']].value[0]
        event_type = ReferenceDataTypeEnum[content['ReferenceDataClass']].value[1]

        authorization_tags = None
        if 'AuthorizationTags' in content and content['AuthorizationTags']:
            authorization_tags = [
                AuthorizationTagEnum[authorization_tag].value
                for authorization_tag in content['AuthorizationTags']
            ]

        enumerations = None
        if 'Enumerations' in content and content['Enumerations']:
            enumerations = [
                EnumEnum[enumeration].value for enumeration in content['Enumerations']
            ]

        units_of_measure = None
        if 'UnitsOfMeasure' in content and content['UnitsOfMeasure']:
            units_of_measure = {
                k: SdsUom.fromJson(uom) for k, uom in content['UnitsOfMeasure'].items()
            }

        result = CSVReferenceDataReader(
            content['Id'],
            content['Name'],
            content['FilePath'],
            event_class,
            event_type,
            authorization_tags,
            enumerations,
            units_of_measure,
        )

        return result

    def toDictionary(self) -> dict[str, Any]:
        authorization_tags = None
        if self.__authorization_tags:
            authorization_tags = [
                authorization_tag.toDictionary()
                for authorization_tag in self.__authorization_tags
            ]

        enumerations = None
        if self.__enumerations:
            enumerations = [
                enumeration.toDictionary() for enumeration in self.__enumerations
            ]

        units_of_measure = None
        if self.__units_of_measure:
            units_of_measure = {
                k: uom.toDictionary() for k, uom in self.__units_of_measure.items()
            }

        result = {
            'Reader': 'CSVReferenceDataReader',
            'Id': self.id,
            'Name': self.name,
            'FilePath': self.__file_path,
            'ReferenceDataClass': ReferenceDataTypeEnum(
                (self.__reference_data_class, self.__reference_data_type)
            ).name,
            'AuthorizationTags': authorization_tags,
            'Enumerations': enumerations,
            'UnitsOfMeature': units_of_measure,
        }

        return result
