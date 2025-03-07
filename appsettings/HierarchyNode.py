import json
from typing import Any

from adh_sample_library_preview import MetadataItem, SdsTypeCode, StreamReference
from adh_sample_library_preview.Asset import Asset

from readers.event_readers import EventReader
from readers.reference_data_readers import ReferenceDataReader
from readers.stream_readers import StreamReader
from readers import Reader


class HierarchyNode:
    def __init__(
        self,
        asset: str = Asset,
        stream_readers: list[StreamReader] = None,
        event_readers: list[EventReader] = None,
        reference_data_readers: list[ReferenceDataReader] = None,
        children: list['HierarchyNode'] = None,
    ):
        self.__asset = asset
        self.__stream_readers = stream_readers if stream_readers else []
        self.__event_readers = event_readers if event_readers else []
        self.__reference_data_readers = (
            reference_data_readers if reference_data_readers else []
        )
        self.__children = children if children else []

        """for event_reader in event_readers:
            if event_reader.__observing:
                for reader in readers:
                    if reader == """

    @property
    def Asset(self) -> Asset:
        return self.__asset

    @property
    def StreamReaders(self) -> list[StreamReader]:
        return self.__stream_readers

    @property
    def EventReaders(self) -> list[EventReader]:
        return self.__event_readers

    @property
    def ReferenceDataReaders(self) -> list[ReferenceDataReader]:
        return self.__reference_data_readers

    @property
    def Children(self) -> list['HierarchyNode']:
        return self.__children

    @Children.setter
    def Children(self, value: list['HierarchyNode']):
        self.__children = value

    def resolve_paths(
        self, parent_id: str = None, parent_name: str = None, path: str = None
    ):
        if not path:
            path = f'\\'

        path = f'{path}\\{self.Asset.Name}'
        
        if not self.Asset.Id:
            self.Asset.Id = path.replace('\\\\', '').replace('\\', '-')
        if not self.Asset.Name:
            self.Asset.Name = self.Asset.Id
        if not self.Asset.Metadata:
            self.Asset.Metadata = []
        for meta in self.Asset.Metadata:
            if meta.Name == '__Path':
                return
        self.Asset.Metadata.extend(
            [
                MetadataItem(
                    '__ParentName',
                    '__ParentName',
                    value=parent_name,
                    sds_type_code=SdsTypeCode.String,
                ),
                MetadataItem(
                    '__ParentId',
                    '__ParentId',
                    value=parent_id,
                    sds_type_code=SdsTypeCode.String,
                ),
                MetadataItem(
                    '__Path', '__Path', value=path, sds_type_code=SdsTypeCode.String
                ),
            ]
        )

        if not self.Asset.StreamReferences or self.Asset.StreamReferences == []:
            self.Asset.StreamReferences = []
            stream_reader: StreamReader
            for stream_reader in self.StreamReaders:
                stream_reader.id = f'{self.Asset.Id}-{stream_reader.id}'
                self.Asset.StreamReferences.append(
                    StreamReference(
                        stream_reader.name, stream_reader.name, stream_reader.id
                    )
                )

        event_reader: EventReader
        for event_reader in self.EventReaders:
            event_reader.reference_asset = self.Asset.Id

        child: HierarchyNode
        for child in self.Children:
            child.resolve_paths(self.Asset.Id, self.Asset.Name, path)

    def get_assets(self) -> list['Asset']:
        assets = []
        assets.append(self.Asset)

        child: HierarchyNode
        for child in self.Children:
            assets += child.get_assets()

        return assets

    def get_stream_readers(self) -> list[StreamReader]:
        stream_readers = []
        stream_readers += self.StreamReaders

        child: HierarchyNode
        for child in self.Children:
            stream_readers += child.get_stream_readers()

        return stream_readers

    def get_event_readers(self) -> list[EventReader]:
        event_readers = []
        event_readers += self.EventReaders

        child: HierarchyNode
        for child in self.Children:
            event_readers += child.get_event_readers()

        return event_readers

    def get_reference_data_readers(self) -> list[ReferenceDataReader]:
        reference_data_readers = []
        reference_data_readers += self.ReferenceDataReaders

        child: HierarchyNode
        for child in self.Children:
            reference_data_readers += child.get_reference_data_readers()

        return reference_data_readers

    @staticmethod
    def fromJson(content: dict[str, Any]) -> 'HierarchyNode':
        stream_readers = []
        if 'StreamReaders' in content:
            for stream_reader in content['StreamReaders']:
                reader_type = Reader[stream_reader['Reader']]
                stream_readers.append(reader_type.value.fromJson(stream_reader))

        event_readers = []
        if 'EventReaders' in content:
            for event_reader in content['EventReaders']:
                reader_type = Reader[event_reader['Reader']]
                event_readers.append(reader_type.value.fromJson(event_reader))

        reference_data_readers = []
        if 'ReferenceDataReaders' in content:
            for reference_data_reader in content['ReferenceDataReaders']:
                reader_type = Reader[reference_data_reader['Reader']]
                reference_data_readers.append(
                    reader_type.value.fromJson(reference_data_reader)
                )

        children = []
        if 'Children' in content:
            for child in content['Children']:
                children.append(HierarchyNode.fromJson(child))

        return HierarchyNode(
            Asset.fromJson(content['Asset']),
            stream_readers,
            event_readers,
            reference_data_readers,
            children,
        )

    def toJson(self):
        return json.dumps(self.toDictionary(), indent=2)

    def toDictionary(self) -> dict[str, Any]:
        result = {
            'Asset': self.Asset.toDictionary(),
            'StreamReaders': [
                stream_reader.toDictionary() for stream_reader in self.StreamReaders
            ],
            'EventReaders': [
                event_reader.toDictionary() for event_reader in self.EventReaders
            ],
            'ReferenceDataReaders': [
                reference_data_reader.toDictionary()
                for reference_data_reader in self.ReferenceDataReaders
            ],
            'Children': [child.toDictionary() for child in self.Children],
        }

        return result
