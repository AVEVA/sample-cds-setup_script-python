import argparse
import copy
import logging
import os
import threading
import time
import global_settings
from datetime import datetime, timedelta, timezone
from enum import Enum
from multiprocessing import Process
from typing import Any
from urllib.parse import urlparse

from adh_sample_library_preview import (
    AccessControlEntry,
    AccessControlList,
    AccessType,
    ADHClient,
    CommonAccessRightsEnum,
    DataViewShape,
    Roles,
    SdsSummaryType,
    Trustee,
    TrusteeType,
)
from omf_sample_library_preview.Client import ADHOMFClient

from appsettings import (
    AppSettings,
    Credentials,
    DataType,
    Resource,
    all_data_types,
    all_resources,
    preview_data_types,
    preview_resources,
    readAppsettings,
    writeAppsettings,
)
from procedures import (
    CleanupProcedures,
    EventRateCounter,
    EventSender,
    ReferenceDataSender,
    SetupProcedures,
    StreamSender,
    helpers,
)

# Logging Settings
level = logging.INFO
log_file_name = 'logfile.txt'

# Parallel Processing Settings
max_stream_processes = min(os.cpu_count(), 4)
max_threads_per_stream_process = 4
max_event_processes = min(os.cpu_count(), 4)
max_threads_per_event_process = 4
max_reference_data_processes = 1
max_threads_per_reference_data_process = 1

# Default Read Settings
default_read_interval = timedelta(seconds=5)
default_backfill_duration = timedelta(days=1)



def partitionList(
    list_to_partition: list[Any], partition_count: int
) -> list[list[Any]]:
    return [list_to_partition[i::partition_count] for i in range(partition_count)]


def streamProcessManager(
    appsettings: AppSettings, process_partition, event_rate_counter
):
    backfill_start = datetime.now(timezone.utc) - default_backfill_duration
    if appsettings.StreamBackfillStart:
        backfill_start = appsettings.StreamBackfillStart
    backfill_end = datetime.now(timezone.utc)
    read_interval = default_read_interval

    thread_partitions = partitionList(process_partition, max_threads_per_stream_process)

    threads: list[threading.Thread] = []
    for thread_partition in thread_partitions:
        if len(thread_partition) > 0:
            omf_clients = helpers.ADHOMFClients()
            for tenant in appsettings.Tenants:
                run_credentials = tenant.RunCredentials
                omf_client = ADHOMFClient(
                    tenant.NamespaceResource,
                    tenant.ApiVersion,
                    tenant.TenantId,
                    tenant.NamespaceId,
                    run_credentials.ClientId,
                    run_credentials.ClientSecret,
                    logging_enabled=True,
                )
                omf_clients.addClient(omf_client)
            thread = threading.Thread(
                target=StreamSender.start,
                args=(
                    omf_clients,
                    thread_partition,
                    read_interval,
                    backfill_start,
                    backfill_end,
                    event_rate_counter,
                ),
            )
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()


def eventProcessManager(
    appsettings: AppSettings, process_partition, event_rate_counter
):
    backfill_start = datetime.now(timezone.utc) - default_backfill_duration
    if appsettings.EventBackfillStart:
        backfill_start = appsettings.EventBackfillStart
    backfill_end = datetime.now(timezone.utc)
    read_interval = default_read_interval

    thread_partitions = partitionList(process_partition, max_threads_per_event_process)

    threads: list[threading.Thread] = []
    for thread_partition in thread_partitions:
        if len(thread_partition) > 0:
            adh_clients: list[ADHClient] = []
            namespaceList: list[str] = []
            for tenant in appsettings.Tenants:
                run_credentials = tenant.RunCredentials
                adh_client = ADHClient(
                    tenant.ApiVersion,
                    tenant.TenantId,
                    tenant.NamespaceResource,
                    run_credentials.ClientId,
                    run_credentials.ClientSecret,
                )
                namespaceList.append(tenant.NamespaceId)
                adh_clients.append(adh_client)
            thread = threading.Thread(
                target=EventSender.start,
                args=(
                    adh_clients,
                    namespaceList, 
                    thread_partition,
                    read_interval,
                    backfill_start,
                    backfill_end,
                    event_rate_counter,
                ),
            )
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()


def referenceDataProcessManager(
    appsettings: AppSettings, process_partition, event_rate_counter
):
    backfill_start = datetime.now(timezone.utc) - default_backfill_duration
    if appsettings.ReferenceDataBackfillStart:
        backfill_start = appsettings.ReferenceDataBackfillStart
    backfill_end = datetime.now(timezone.utc)
    read_interval = default_read_interval

    thread_partitions = partitionList(process_partition, max_threads_per_event_process)

    threads: list[threading.Thread] = []  #todo fix this
    for thread_partition in thread_partitions:
        if len(thread_partition) > 0:
            adh_clients: list[ADHClient] = []
            namespaceList: list[str] = []
            for tenant in appsettings.Tenants:
                run_credentials = tenant.RunCredentials
                adh_client = ADHClient(
                    tenant.ApiVersion,
                    tenant.TenantId,
                    tenant.NamespaceResource,
                    run_credentials.ClientId,
                    run_credentials.ClientSecret,
                )
                namespaceList.append(tenant.NamespaceId)
                adh_clients.append(adh_client)
            thread = threading.Thread(
                target=ReferenceDataSender.start,
                args=(
                    adh_clients,
                    namespaceList, 
                    thread_partition,
                    read_interval,
                    backfill_start,
                    backfill_end,
                    event_rate_counter,
                ),
            )
            threads.append(thread)
            thread.start()           

    for thread in threads:
        thread.join()


def monitorEventRate(
    stream_event_rate_counter: EventRateCounter,
    event_event_rate_counter: EventRateCounter,
    reference_data_event_rate_counter: EventRateCounter,
):
    while True:
        if stream_event_rate_counter is not None:
            print(
                f'Streams event sending rate: {stream_event_rate_counter.get_event_rate():.2f} Events/Seconds'
            )
        if event_event_rate_counter is not None:
            print(
                f'Events event sending rate: {event_event_rate_counter.get_event_rate():.2f} Events/Seconds'
            )
        if reference_data_event_rate_counter is not None:
            print(
                f'Reference Data event sending rate: {reference_data_event_rate_counter.get_event_rate():.2f} Events/Seconds'
            )
        time.sleep(30)


def run(appsettings: AppSettings, test: bool = False):
    print('Starting Data Sender')

    hierarchy = appsettings.Hierarchy

    data_types = all_data_types
    if not appsettings.Preview:
        for data_type in preview_data_types:
            data_types.remove(data_type)
    if appsettings.ExcludedDataTypes:
        for data_type in appsettings.ExcludedDataTypes:
            data_types.remove(data_type)

    # Check that there are run credentials
    if not appsettings.Tenants[0].RunCredentials:
        raise TypeError(
            'Run Credentials are not set. Please run the script in Setup mode first.'
        )

    processes: list[Process] = []

    stream_event_rate_counter = None
    event_event_rate_counter = None
    reference_data_event_rate_counter = None

    if DataType.Stream in data_types:
        # Make sure we do not have too many connections to Data Hub for 1 client
        assert max_stream_processes * max_threads_per_stream_process <= 24

        stream_event_rate_counter = EventRateCounter()
        stream_readers = hierarchy.get_stream_readers()
        if appsettings.SoloStreamReaders:
            stream_readers += appsettings.SoloStreamReaders
        stream_process_partitions = partitionList(stream_readers, max_stream_processes)

        for stream_process_partition in stream_process_partitions:
            if len(stream_process_partition) > 0:
                process = Process(
                    target=streamProcessManager,
                    args=(
                        appsettings,
                        stream_process_partition,
                        stream_event_rate_counter,
                    ),
                )
                processes.append(process)
                process.start()

    if DataType.Event in data_types:
        # Make sure we do not have too many connections to Event Store for 1 client
        assert max_event_processes * max_threads_per_event_process <= 24

        event_event_rate_counter = EventRateCounter()
        event_readers = hierarchy.get_event_readers()
        if appsettings.SoloEventReaders:
            event_readers += appsettings.SoloEventReaders
        event_process_partitions = partitionList(event_readers, max_event_processes)

        for event_process_partition in event_process_partitions:
            if len(event_process_partition) > 0:
                process = Process(
                    target=eventProcessManager,
                    args=(
                        appsettings,
                        event_process_partition,
                        event_event_rate_counter,
                    ),
                )
                processes.append(process)
                process.start()

    if DataType.ReferenceData in data_types:
        reference_data_event_rate_counter = EventRateCounter()
        reference_data_readers = hierarchy.get_reference_data_readers()
        if appsettings.SoloReferenceDataReaders:
            reference_data_readers += appsettings.SoloReferenceDataReaders
        reference_data_process_partitions = partitionList(
            reference_data_readers, max_reference_data_processes
        )

        for reference_data_process_partition in reference_data_process_partitions:
            if len(reference_data_process_partition) > 0:
                process = Process(
                    target=referenceDataProcessManager,
                    args=(
                        appsettings,
                        reference_data_process_partition,
                        reference_data_event_rate_counter,
                    ),
                )
                processes.append(process)
                process.start()

    # Make sure we are not using too many processes
    assert len(processes) <= os.cpu_count() * 2 + 1

    eventRateThread = threading.Thread(
        target=monitorEventRate,
        args=(
            stream_event_rate_counter,
            event_event_rate_counter,
            reference_data_event_rate_counter,
        ),
        daemon=True,
    )
    eventRateThread.start()

    process: Process
    while any([process.is_alive() for process in processes]):
        if test:
            time.sleep(30)
            break
        elif input('Type "exit" to quit application: \n').casefold() == 'exit':
            break

    for process in processes:
        process.terminate()


def setup(appsettings: AppSettings): #TODO parrallize this to create each at the same time
    asset_types = appsettings.AssetTypes
    hierarchy = appsettings.Hierarchy
    labels = appsettings.Labels
    custom_contributor_role: Roles

    # Make a list of resources to create
    resources = all_resources
    if not appsettings.Preview:
        for resource in preview_resources:
            resources.remove(resource)
    if appsettings.ExcludedSetupResources:
        for resource in appsettings.ExcludedSetupResources:
            if resource in resources: resources.remove(resource)

    stream_readers = []
    if Resource.Streams in resources:
        stream_readers = hierarchy.get_stream_readers()
        if appsettings.SoloStreamReaders:
            stream_readers = stream_readers + appsettings.SoloStreamReaders

    assets = []
    if Resource.Assets in resources:
        assets = hierarchy.get_assets()
    
    event_readers = hierarchy.get_event_readers()
    if appsettings.SoloEventReaders:
        event_readers += appsettings.SoloEventReaders
    reference_data_readers = hierarchy.get_reference_data_readers()
    if appsettings.SoloReferenceDataReaders:
        reference_data_readers += appsettings.SoloReferenceDataReaders

    for tenant in appsettings.Tenants: #TODO parrallize this to create each at the same time -- need to consider the write back of the appsettings
        setup_credentials = tenant.SetupCredentials
        namespace_id = tenant.NamespaceId
        tenant_id = tenant.TenantId

        adh_client = ADHClient(
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.AuthenticationResource,
            setup_credentials.ClientId,
            setup_credentials.ClientSecret,
        )

        print('Get Namespace Region')
        namespace = adh_client.Namespaces.getNamespaceById(namespace_id)
        namespace_resource = f'https://{urlparse(namespace.Self).hostname}'
        tenant.NamespaceResource = namespace_resource

        print('Creating Community')
        if Resource.Community in resources:
            community = SetupProcedures.createCommunity(
                adh_client,
                labels.CommunityName,
                labels.CommunityDescription,
                labels.CommunityContactEmail,
            )

        print('Creating Roles')
        if Resource.Roles in resources:
            custom_roles = (
                labels.CustomAdministratorRoleName,
                labels.CustomContributorRoleName,
                labels.CustomViewerRoleName,
            )
            role_descriptions = (
                labels.CustomAdministratorRoleDescription,
                labels.CustomContributorRoleDescription,
                labels.CustomViewerRoleDescription,
            )
            (
                custom_administrator_role,
                custom_contributor_role,
                custom_viewer_role,
            ) = SetupProcedures.createRoles(adh_client, custom_roles, role_descriptions)

        # Get Tenant Administrator Role
        all_roles = adh_client.Roles.getRoles(count=1000)
        tenant_administrator_role = next(
            iter(
                [
                    role
                    for role in all_roles
                    if role.RoleTypeId
                    and role.RoleTypeId.casefold()
                    == Roles.TenantAdministratorRoleTypeId.casefold()
                ]
            )
        )

        # Get Tenant Contributor Role
        tenant_contributor_role = next(
            iter(
                [
                    role
                    for role in all_roles
                    if role.RoleTypeId
                    and role.RoleTypeId.casefold()
                    == Roles.TenantContributorRoleTypeId.casefold()
                ]
            )
        )

        # Get Tenant Member Role
        tenant_member_role = next(
            iter(
                [
                    role
                    for role in all_roles
                    if role.RoleTypeId
                    and role.RoleTypeId.casefold()
                    == Roles.TenantMemberRoleTypeId.casefold()
                ]
            )
        )

        # Create access control list to be used on created resources
        acl = AccessControlList(
            [
                AccessControlEntry(
                    Trustee(TrusteeType.Role, tenant_id, tenant_administrator_role.Id),
                    AccessType.Allowed,
                    CommonAccessRightsEnum.All,
                )
            ]
        )
        if Resource.Roles in resources:
            acl.RoleTrusteeAccessControlEntries.extend(
                [
                    AccessControlEntry(
                        Trustee(TrusteeType.Role, tenant_id, custom_administrator_role.Id),
                        AccessType.Allowed,
                        CommonAccessRightsEnum.All,
                    ),
                    AccessControlEntry(
                        Trustee(TrusteeType.Role, tenant_id, custom_contributor_role.Id),
                        AccessType.Allowed,
                        CommonAccessRightsEnum.Write | CommonAccessRightsEnum.Read,
                    ),
                    AccessControlEntry(
                        Trustee(TrusteeType.Role, tenant_id, custom_viewer_role.Id),
                        AccessType.Allowed,
                        CommonAccessRightsEnum.Read,
                    ),
                ]
            )

        # Create a separate access control list for streams in case communties are enabled
        stream_acl = copy.deepcopy(acl)
        if Resource.Community in resources:
            stream_acl.RoleTrusteeAccessControlEntries.append(
                AccessControlEntry(
                    Trustee(TrusteeType.Role, object_id=community.MemberRoleId),
                    AccessType.Allowed,
                    CommonAccessRightsEnum.Read,
                )
            )

        run_credentials = None
        if tenant.RunCredentials:
            run_credentials = tenant.RunCredentials
        else:
            print('Creating Client Credentials Client')
            client_credentials_client = SetupProcedures.createClient(
                adh_client,
                [
                    tenant_contributor_role.Id,
                    tenant_member_role.Id,
                    custom_contributor_role.Id,
                ],
                labels.ClientName,
                labels.ClientSecretDescription,
            )
            run_credentials = Credentials(
                client_credentials_client.get('Id'),
                client_credentials_client.get('Secret', None),
            )

        # If there is no client secret, generate a new one or exit early.
        if (
            not run_credentials.ClientSecret
            and input(
                'No stored client secret found in run credentials. Would you like to generate new ones? (y/n): '
            ).casefold()
            == 'y'
        ):
            print('Generating New Client Secret for Run Credentials')
            run_credentials.ClientSecret = SetupProcedures.createSecret(
                adh_client, run_credentials.ClientId, labels.OMFConnectionDescription
            )
        elif not run_credentials.ClientSecret:
            print('No Run Credentials Available. Exiting Setup Early.')
            return

        print('Saving Run Credentials to Appsettings')
        tenant.RunCredentials = run_credentials

        # Setting base client url to namespace url
        # Note: this should be handled in the client library instead
        auth_object = adh_client.baseClient._BaseClient__auth_object
        adh_client = ADHClient(
            tenant.ApiVersion, tenant.TenantId, namespace_resource, None
        )
        adh_client.baseClient._BaseClient__auth_object = auth_object

        print('Creating OMF Connection')
        SetupProcedures.createOmfConnection(
            adh_client,
            namespace_id,
            run_credentials.ClientId,
            labels.OMFConnectionName,
            labels.OMFConnectionDescription,
        )

        omf_client = ADHOMFClient(
            namespace_resource,
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.NamespaceId,
            run_credentials.ClientId,
            run_credentials.ClientSecret,
            logging_enabled=True,
        )

        if Resource.Streams in resources:
            print('Creating Types')
            SetupProcedures.createTypes(adh_client, omf_client, stream_readers)
            SetupProcedures.setTypeACLs(adh_client, namespace_id, stream_readers, acl)

            print('Creating Streams')
            SetupProcedures.createStreams(adh_client, omf_client, stream_readers)
            SetupProcedures.setStreamACLs(
                adh_client, namespace_id, stream_readers, stream_acl
            )

        print('Creating Metadata Rule')
        # TODO Not in the library

        if Resource.Assets in resources:
            print('Creating AssetTypes')
            SetupProcedures.createAssetTypes(adh_client, namespace_id, acl, asset_types)

            print('Creating Assets')
            SetupProcedures.createAssets(adh_client, namespace_id, acl, assets)

        print('Creating Asset Rule')
        # TODO Not in the library

        if Resource.DataViews in resources and Resource.Assets in resources:
            print('Creating Data Views')
            SetupProcedures.createAssetTypeDataViews(
                adh_client, namespace_id, acl, asset_types
            )
            SetupProcedures.createAssetTypeDataViews(
                adh_client, namespace_id, acl, asset_types, SdsSummaryType.Mean
            )
            SetupProcedures.createAssetTypeDataViews(
                adh_client, namespace_id, acl, asset_types, shape=DataViewShape.Narrow
            )

        if Resource.EventTypes in resources or Resource.ReferenceDataTypes in resources:
            print('Creating Authorization Tags')
            SetupProcedures.createAuthorizationTags(
                adh_client, namespace_id, event_readers, reference_data_readers
            )

        if Resource.Enumerations in resources:
            print('Creating Enumerations')
            SetupProcedures.createEnumerations(
                adh_client, namespace_id, event_readers, reference_data_readers
            )

        if Resource.ReferenceDataTypes in resources:
            print('Creating Reference Data Types')
            SetupProcedures.createReferenceDataTypes(
                adh_client, namespace_id, reference_data_readers
            )

        if Resource.EventTypes in resources:
            print('Creating Event Types')
            SetupProcedures.createEventTypes(adh_client, namespace_id, event_readers)
    
    writeAppsettings(appsettings)


def cleanup(appsettings: AppSettings): #TODO parrallize this to delete each at the same time 

    asset_types = appsettings.AssetTypes
    hierarchy = appsettings.Hierarchy
    labels = appsettings.Labels

    # Make a list of resources to delete
    resources = all_resources
    if not appsettings.Preview:
        for resource in preview_resources:
            resources.remove(resource)
    if appsettings.ExcludedCleanupResources:
        for resource in appsettings.ExcludedCleanupResources:
            resources.remove(resource)

    event_readers = []
    if Resource.EventTypes in resources:
        event_readers = hierarchy.get_event_readers()
        if appsettings.SoloEventReaders:
            event_readers += appsettings.SoloEventReaders
    
    reference_data_readers = []
    if Resource.ReferenceDataTypes in resources:
        reference_data_readers = hierarchy.get_reference_data_readers()
        if appsettings.SoloReferenceDataReaders:
            reference_data_readers += appsettings.SoloReferenceDataReaders

    assets = []    
    if Resource.Assets in resources:
        assets = hierarchy.get_assets()

    stream_readers = []
    if Resource.Streams in resources:
        print('Deleting Streams')
        stream_readers = hierarchy.get_stream_readers()
        if appsettings.SoloStreamReaders:
            stream_readers += appsettings.SoloStreamReaders

    for tenant in appsettings.Tenants: #TODO parrallize this to delete each at the same time -- need to consider the write back of the appsettings
        setup_credentials = tenant.SetupCredentials
        namespace_id = tenant.NamespaceId


        adh_client = ADHClient(
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.AuthenticationResource,
            setup_credentials.ClientId,
            setup_credentials.ClientSecret,
        )

        # Setting base client url to namespace url
        # Note: this should be handled in the client library instead
        auth_object = adh_client.baseClient._BaseClient__auth_object
        adh_client = ADHClient(
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.NamespaceResource,
            None,
        )
        adh_client.baseClient._BaseClient__auth_object = auth_object

        run_credentials = tenant.RunCredentials

        # If there is no client secret generate new one
        if not run_credentials.ClientSecret:
            run_credentials.ClientSecret = SetupProcedures.createSecret(
                adh_client, run_credentials.ClientId, labels.OMFConnectionDescription
            )

        omf_client = ADHOMFClient(
            tenant.NamespaceResource,
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.NamespaceId,
            run_credentials.ClientId,
            run_credentials.ClientSecret,
            logging_enabled=True,
        )

        print('Deleting Demo Edge System')
        # TODO

        if Resource.EventTypes in resources:
            print('Deleting Events')
            # Skip for now since deleting events is slow
            # CleanupProcedures.deleteEvents(adh_client, namespace_id, event_readers)

        if Resource.ReferenceDataTypes in resources:
            print('Deleting Reference Data')
            CleanupProcedures.deleteReferenceData(
                adh_client, namespace_id, reference_data_readers
            )

        if Resource.DataViews in resources:
            print('Deleting Data Views')
            CleanupProcedures.deleteAssetTypeDataViews(
                adh_client, namespace_id, asset_types
            )
            CleanupProcedures.deleteAssetTypeDataViews(
                adh_client, namespace_id, asset_types, SdsSummaryType.Mean
            )
            CleanupProcedures.deleteAssetTypeDataViews(
                adh_client, namespace_id, asset_types, shape=DataViewShape.Narrow
            )

        print('Deleting Metadata Rule')
        # TODO

        print('Deleting Asset Rule')
        # TODO

        if Resource.Assets in resources:
            print('Deleting Assets')
            CleanupProcedures.deleteAssets(adh_client, namespace_id, assets)

            print('Deleting Asset Types')
            CleanupProcedures.deleteAssetTypes(adh_client, namespace_id, asset_types)

        if Resource.Streams in resources:
            print('Deleting Streams')
            CleanupProcedures.deleteStreams(omf_client, stream_readers)

            print('Deleting Types')
            CleanupProcedures.deleteTypes(adh_client, omf_client, stream_readers)

        # Setting base client url to namespace url
        # Note: this should be handled in the client library instead
        auth_object = adh_client.baseClient._BaseClient__auth_object
        adh_client = ADHClient(
            tenant.ApiVersion,
            tenant.TenantId,
            tenant.AuthenticationResource,
            None,
        )
        adh_client.baseClient._BaseClient__auth_object = auth_object

        if Resource.Community in resources:
            print('Deleting Community')
            CleanupProcedures.deleteCommunity(adh_client, labels.CommunityName)

        if Resource.Roles in resources:
            print('Deleting Roles')
            custom_roles = [
                labels.CustomAdministratorRoleName,
                labels.CustomContributorRoleName,
                labels.CustomViewerRoleName,
            ]
            CleanupProcedures.deleteRoles(adh_client, custom_roles)

        if Resource.OMFConnection in resources:
            print('Deleting OMF Connection')
            CleanupProcedures.deleteOMFConnection(
                adh_client, namespace_id, labels.OMFConnectionName
            )

        if Resource.Client in resources:
            print('Deleting Client Credentials Client')
            CleanupProcedures.deleteClient(adh_client, labels.ClientName)

            print('Removing Run Client from Appsetting')
            tenant.RunCredentials = None

    writeAppsettings(appsettings)


def main():
    print('Starting Application')

    parser = argparse.ArgumentParser(description='Setup Script for ADH')
    parser.add_argument(
        'application_mode',
        type=global_settings.ApplicationMode,
        choices=list(global_settings.ApplicationMode),
        help='Application mode selector',
    )
    args = parser.parse_args()

    global_settings.application_mode = args.application_mode
    print('Initializing readers... this may take some time depending on the size of your dataset')
    initialization_start = time.time()
    appsettings = readAppsettings()
    initialization_end = time.time()
    print(f"Initialization took: {initialization_end - initialization_start}")
    if (
        args.application_mode is global_settings.ApplicationMode.Setup
        or args.application_mode is global_settings.ApplicationMode.SetupAndRun
    ):
        print('Starting Setup Procedures')
        setup(appsettings)

    if (
        args.application_mode is global_settings.ApplicationMode.Run
        or args.application_mode is global_settings.ApplicationMode.SetupAndRun
    ):
        print('Starting Run Procedures')
        run(appsettings)

    if args.application_mode is global_settings.ApplicationMode.Cleanup:
        print('Starting Cleanup Procedures')
        cleanup(appsettings)

    print('Complete!')


if __name__ == '__main__':
    # Set up the logger
    logging.basicConfig(
        filename=log_file_name,
        encoding='utf-8',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(asctime)s %(module)16s,line: %(lineno)4d %(levelname)8s | %(message)s',
    )

    main()
