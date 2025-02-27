# CDS Python Setup Script

**Version:** 1.0.0  

Developed against Python 3.11.3

## Requirements

- Python 3.11+
- Register an [Authorization Code Client](https://datahub.connect.aveva.com/clients) and ensure that the registered client in Cds contains `http://localhost:5004/callback.html` in the list of RedirectUris.
- When running the script you should log in with an account that has Tenant Administrator and Tenant Community Administrator Roles.
- Install required modules: `pip install -r requirements.txt`

## About this Sample

This sample creates a set of resources commonly used in AVEVA CONNECT data services for learning, development, and training purposes. It also provides an extensible framework for building custom demo systems. For a full list of resources created, please refer to the [What the Script Creates](#what-the-script-creates) section below.

This sample in run mode is intended to be run in an environment where the computer stays on and is consistently connected to the internet.

## Running the Sample

1. Clone the GitHub repository
1. Install required modules: `pip install -r requirements.txt`
1. Open the folder with your favorite IDE.
1. Configure the sample using the file [appsettings.placeholder.json](appsettings.placeholder.json). Before editing, rename this file to `appsettings.json`. This repository's `.gitignore` rules should prevent the file from ever being checked in to any fork or branch, to ensure credentials are not compromised.
1. Update `appsettings.json` with your connection information and client credentials.
1. Run `program.py`, passing in one of the command line arguments: `Setup`, `Run`, `SetupAndRun`, or `Cleanup`. For example, `python program.py Setup`. For more information on these command line arguments see the next section, [Sample Modes](#sample-modes).

## Sample Modes

| Sample Mode | Description                                                                                        |
| ----------- | -------------------------------------------------------------------------------------------------- |
| Setup       | Creates the [static resources](#static-resources) listed below                                     |
| Run         | Backfills data if possible and continuously sends new data for streams, reference data, and events |
| SetupAndRun | Combination of the previous two Sample Modes                                                       |
| Cleanup     | Deletes all created resources                                                                      |

## Configuring the Sample

The sample is configured by modifying the file appsettings.placeholder.json. Details on how to configure it can be found in the sections below. Before editing appsettings.placeholder.json, rename this file to appsettings.json. This repository's .gitignore rules should prevent the file from ever being checked in to any fork or branch, to ensure credentials are not compromised.  

If multiple instances of this application are being run in the same AVEVA CONNECT data services instance there may be naming collisions. To avoid collisions with the client, the `ClientName` [label](#labels) should be specified.

### Configuring appsettings.json

| Parameter                  | Required | Type                                    | Description                                                                                                                                                                                                          |
| -------------------------- | -------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tenants                    | Required | [Endpoint](#Endpoint) List              | Endpoint to send data to                                                                                               |
| DataConfigurationPath      | Required | String (Relative File Path)             | Path to data [configuration file](#data-configuration)                                                                                                                                                               |
| Preview                    | Optional | Boolean                                 | Whether to include preview features<br>If preview features are not enabled in your account then this should be left false<br>Default: False                                                                          |
| Labels                     | Optional | [Labels](#labels)                       | Labels used throughout the application for resource names                                                                                                                                                            |
| ExcludedSetupResources     | Optional | [Resource](#resources-enumeration) List | Resources to not create when application run in Setup mode<br>The application is not aware of dependent resources<br>If, for example, Streams are disabled then created Assets will not have valid stream references |
| ExcludedCleanupResources   | Optional | [Resource](#resources-enumeration) List | Resources to not delete when application run in Cleanup mode                                                                                                                                                         |
| ExcludedDataTypes          | Optional | [Resource](#resources-enumeration) List | Data types not to send when application run in Run mode                                                                                                                                                              |
| StreamBackfillStart        | Optional | String (ISO 8601 Timestamp)             | Backfill start time for stream data                                                                                                                                                                                  |
| EventBackfillStart         | Optional | String (ISO 8601 Timestamp)             | Backfill start time for event data                                                                                                                                                                                   |
| ReferenceDataBackfillStart | Optional | String (ISO 8601 Timestamp)             | Backfill start time for referene data                                                                                                                                                

### Endpoint

| Parameter    | Required | Type   | Description                                                                                                                                   |
| ------------ | -------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| AuthenticationResource     | Required | String (Uri)                            | Base  URL being used                                                                                                                                                                                              |
| NamespaceResource          | Optional | String (Uri)                            | Region specific Namespace URL<br>Automatically added when Setup mode is run                                                                                                                                          |
| ApiVersion                 | Required | String                                  | API Version                                                                                                                                                                                                          |
| TenantId                   | Required | String                                  | Tenant Id of Tenant being sent to                                                                                                                                                                                    |
| NamespaceId                | Required | String                                  | Namespace Id of Namespace being sent to                                                                                                                                                                              |
| SetupCredentials           | Required | [Credentials](#credentials)             | Authorization Code or Client Credentials Client used in Setup mode to create resources and Run<br>If sending to multiple tenants use Client Credentials for each. Credentials                                                                                                                             |
| RunCredentials             | Optional | [Credentials](#credentials)             | Client Credentials used for sending OMF messages in both Setup and Run modes<br>Automatically created when Setup mode is run.  This *cannot* be an Authorization Code Credential   

### Credentials

| Parameter    | Required | Type   | Description                                                                                                                                   |
| ------------ | -------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| ClientId     | Required | String | Client Id for either a Client Credentials Client or an Authorization Code Client                                                              |
| ClientSecret | Optional | String | Client Secret for when a Client Credentials Client is being used<br>This should not be included if an Authorization Code Client is being used |

### Data Configuration

Data configurations contain the defintion of what resources to create and what data to send. This is done by specifying a heirarchy of Assets, associated resources like Asset Types, and optional stadalone resources that are not tied to the heirarchy. The data configurations included with this sample are described in the table below.

| Data Configuration | Relative Path                          | Description                                                                                                                                 |
| ------------------ | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| test               | data\test\data_configuration.json      | Test data set to verify that sample is working                                                                                              |
| gentovo            | data\gentovo\data_configuration.json   | Gentovo energy drink plant data set<br>Contains the fictitious Gentovo energy drink plant data created by TwinThread                        |
| windtopia          | data\windtopia\data_configuration.json | Windtopia wind turbine data set<br>Contains the fictitious Windtopia organization with 10 wind turbine Assets spread out across three sites |



### Labels

| Parameter                          | Required | Type   | Description                                                                                                  |
| ---------------------------------- | -------- | ------ | ------------------------------------------------------------------------------------------------------------ |
| CommunityName                      | Optional | String | Name of Community created in Setup mode<br>Default: Setup Script Community                                   |
| CommunityDescription               | Optional | String | Description of Community created in Setup mode<br>Default: Setup Script Community                            |
| CommunityContactEmail              | Optional | String | Email of contact for Community created in Setup mode<br>Default: null                                        |
| CustomAdministratorRoleName        | Optional | String | Name of Custom Administrator Role created in Setup mode<br>Default: Setup Administrator                      |
| CustomAdministratorRoleDescription | Optional | String | Description of Custom Administrator Role created in Setup mode<br>Default: Setup Administrator Role          |
| CustomContributorRoleName          | Optional | String | Name of Custom Contributor Role created in Setup mode<br>Default: Setup Contributor                          |
| CustomContributorRoleDescription   | Optional | String | Description of Custom Contributor Role created in Setup mode<br>Default: Setup Contributor Role              |
| CustomViewerRoleName               | Optional | String | Name of Custom Viewer Role created in Setup mode<br>Default: Setup Viewer                                    |
| CustomViewerRoleDescription        | Optional | String | Description of Custom Viewer Role created in Setup mode<br>Default: Setup Viewer Role                        |
| RoleDescription                    | Optional | String | Description of all custom Roles created in Setup mode<br>Default: Setup Script Role                          |
| ClientName                         | Optional | String | Name of Run mode<br>Default:  client created in Setup mode<br>Default: Setup Script Client                   |
| ClientSecretDescription            | Optional | String | Description of Run mode<br>Default:  client created in Setup mode<br>Default: Client Secret for Setup Script |
| OMFConnectionName                  | Optional | String | Name of OMF connection created in Setup mode<br>Default: Setup Script OMF Client                             |
| OMFConnectionDescription           | Optional | String | Description of OMF connection created in Setup mode<br>Default: OMF Connection for Setup Script              |

### Resources Enumeration

| Property           | Description                      |
| ------------------ | -------------------------------- |
| Community          | Community                        |
| Roles              | Custom Roles                     |
| Streams            | Streams and Types                |
| Assets             | Assets and Asset Types           |
| DataViews          | Data Views                       |
| Enumerations       | Event Store Enumerations         |
| ReferenceDataTypes | Event Store Reference Data Types |
| EventTypes         | Event Store Event Types          |
| OMFConnection      | OMF Connection                   |
| Client             | Client CredentialsClient         |

## What the Script Creates

### Static Resources

1. Community  
   ![image](/images/community.png)
1. Roles  
   ![image](/images/roles.png)
1. Client Credential Client  
   ![image](/images/client.png)
1. OMF Connection  
   ![image](/images/omf_connection.png)
1. Stream Types  
   ![image](/images/stream_types.png)
1. Streams  
   ![image](/images/streams.png)
1. Asset Types  
   ![image](/images/asset_types.png)
1. Assets  
   ![image](/images/assets.png)
1. Data Views  
   ![image](/images/data_views.png)
1. Authorization Tags  
   ![image](/images/authorization_tags.png)
1. Enumerations  
   ![image](/images/enumerations.png)
1. Reference Data Types  
   ![image](/images/reference_data_types.png)
1. Event Types  
   ![image](/images/event_types.png)

### Backfilled and Streamed Data

1. Stream Data
1. Reference Data
1. Event Data

---

For the main AVEVA samples page [ReadMe](https://github.com/AVEVA)
