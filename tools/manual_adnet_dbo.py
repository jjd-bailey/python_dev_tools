'''
    pip install pyyaml
    pip install polars
    pip install connectorx
    pip install pyarrow
'''
import time
import polars
from os import (
    path,
    makedirs,
)
from yaml import (
    dump,
)
from text_formatting.text_formatting import (
    camelcase_to_snakecase,
)
from mssql.list_tables import (
    create_mssql_engine,
    get_mssql_tables,
)
from cloudformation.definitions.table_definitions import(
    create_table_definitions_dicts,
)
from cloudformation.templates.glue_jobs import (
    group_tables,
    create_glue_pyshell_jobs_dicts,
)


server_name= 'AWDEV1DA2ASQL02'
port_number= 1433
target_database= 'Adnet2_PROD'


camel_case = True

target_schema = 'dbo'
#region
conn = f'mssql://GVW_AnalyticsReader:Hm555yIs1aWcLn2LtHdD@{server_name}/{target_schema}?driver=SQL+Server&trusted_connection={True}'
#endregion


mssql_engine = create_mssql_engine(
    host = server_name,
    port = port_number,
    database = target_database
)

database_tables = get_mssql_tables(engine = mssql_engine)

table_sorted = sorted(
    database_tables,
    key = lambda x: (list(x.values())[0]['schema'],list(x.values())[0]['record_count']),
    reverse = True
)

"""
    Focus on desired schema
"""

target_tables = []

for q in table_sorted:
    for w in q:
        if q[w]['schema'] == target_schema:
            target_tables.append(q)

len(target_tables)


'''
    Create definitions dict
'''
adnet_inputs = {
    'SpecDetailRequest': {
        'ExtractType': 'full_extract',
    },
    'xx_sls_code': {
        'ExtractType': 'full_extract',
    },
    'xx_sls_code': {
        'ExtractType': 'full_extract',
    },
    'TranslationSalescodeRequest': {
        'ExtractType': 'full_extract',
    },
    'SpecDetailOrder': {
        'ExtractType': 'full_extract',
    },
    'SpecDetail': {
        'ExtractType': 'full_extract',
    },
    'TranslationMfgRequest': {
        'ExtractType': 'full_extract',
    },
    'TranslationBomRequest': {
        'ExtractType': 'full_extract',
    },
    'BomUserSelectionsRequest': {
        'ExtractType': 'full_extract',
    },
    'TranslationSalescodeOrder': {
        'ExtractType': 'full_extract',
    },
    'xx_iman_bom': {
        'ExtractType': 'full_extract',
    },
    'TranslationMissingBOMChartRequest': {
        'ExtractType': 'full_extract',
    },
    'TranslationBomOrder': {
        'ExtractType': 'full_extract',
    },
    'TranslationMfgOrder': {
        'ExtractType': 'full_extract',
    },
    'BomUserSelectionsOrder': {
        'ExtractType': 'full_extract',
    },
    'BomOptionThatChanged': {
        'ExtractType': 'full_extract',
    },
    'RuleCondition': {
        'ExtractType': 'full_extract',
    },
    'TranslationMissingBOMChartOrder': {
        'ExtractType': 'full_extract',
    },
    'xx_trans_ecf': {
        'ExtractType': 'full_extract',
    },
    'EcfMaster': {
        'ExtractType': 'full_extract',
    },
    'TranslationEcfRequest': {
        'ExtractType': 'full_extract',
    },
    'BomOption': {
        'ExtractType': 'full_extract',
    },
    'ExpressionBreakdown': {
        'ExtractType': 'full_extract',
    },
    'RuleHeader': {
        'ExtractType': 'full_extract',
    },
    'VisitorLog': {
        'Grain': 'VisitorLogID',
        'create_delta': 'Date',
        'update_delta': 'Date',
        'ExtractType': 'delta_extract',
    },
    'RuleAction': {
        'ExtractType': 'full_extract',
    },
    'AltDealerChargesRequest': {
        'ExtractType': 'full_extract',
    },
    'Expression': {
        'ExtractType': 'full_extract',      # StartDate and EndDate are strings
    },
    'TranslationEcfOrder': {
        'ExtractType': 'full_extract',
    },
    'AlteredCodeRequest': {
        'ExtractType': 'full_extract',
    },
    'AltTaxesRequest': {
        'ExtractType': 'full_extract',
    },
    'TranslationFeatureRequest': {
        'ExtractType': 'full_extract',
    },
    'ModelOption': {
        'ExtractType': 'full_extract',
    },
    'MatchOrderOption': {
        'ExtractType': 'full_extract',
    },
    'TranslationFeatureOrder': {
        'ExtractType': 'full_extract',
    },
    'BillOfMaterialThatChanged': {
        'ExtractType': 'full_extract',
    },
    'ChartOption': {
        'ExtractType': 'full_extract',
    },
    'CustomerOrderItemAddress': {
        'ExtractType': 'full_extract',
    },
    'CustomerAddress': {
        'ExtractType': 'full_extract',          # Duplicate records, no Primary or Composite key to use -- delta columns exist
    },
    'ExceptionLog': {
        'Grain': 'ExceptionID',
        'create_delta': 'ExceptionTime',
        'update_delta': 'ExceptionTime',
        'ExtractType': 'delta_extract',
    },
    'xx_alt_sls': {
        'ExtractType': 'full_extract',
    },
    'mapAlteredCodeId': {
        'ExtractType': 'full_extract',
    },
    'AltDealerCharges': {
        'ExtractType': 'full_extract',
    },
    'BillOfMaterialCommitLog': {
        'Grain': 'Id',
        'create_delta': 'CommittedDate',
        'update_delta': 'CommittedDate',
        'ExtractType': 'delta_extract',
    },
    'ShadowBillOfMaterialHistory': {
        'ExtractType': 'full_extract',          # Duplicate records, no Primary or Composite key to use -- delta columns exist
    },
    'AlteredCodeOrder': {
        'ExtractType': 'full_extract',
    },
    'SeriesOption': {
        'ExtractType': 'full_extract',
    },
    'AltDealerChargesOrder': {
        'ExtractType': 'full_extract',
    },
    'ShippingOrderItemAddress': {
        'ExtractType': 'full_extract',
    },
    'WeightOverride': {
        'ExtractType': 'full_extract',
    },
    'DrivelineCombinations': {
        'ExtractType': 'full_extract',
    },
    'ChartOptionThatChanged': {
        'ExtractType': 'full_extract',
    },
    'DealerChargesRequest': {
        'ExtractType': 'full_extract',
    },
    'DealerInvoiceRequest': {
        'ExtractType': 'full_extract',
    },
    'OrderHeaderRequest': {
        'ExtractType': 'full_extract',
    },
    'OwnerRequest': {
        'ExtractType': 'full_extract',
    },
    'QuoteRequest': {
        'ExtractType': 'full_extract',
    },
    'SolutionDetailRequest': {
        'Grain': 'SolutionId,SolutionRev,QuoteExpirationDate',
        'create_delta': 'LastupdateDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SolutionRequest': {
        'Grain': 'SolutionId,SolutionRev,ModelId,Databook',
        'create_delta': 'CreationDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SpecHeaderRequest': {
        'ExtractType': 'full_extract',
    },
    'TaxesRequest': {
        'ExtractType': 'full_extract',
    },
    'AltTaxesOrder': {
        'ExtractType': 'full_extract',
    },
    'ShadowChartOptionHistory': {
        'ExtractType': 'full_extract',
    },
    'AltTaxes': {
        'ExtractType': 'full_extract',
    },
    'VendorPartFeature': {
        'ExtractType': 'full_extract',
    },
    'QuoteRequestRequest': {
        'ExtractType': 'full_extract',
    },
    'AlteredCode': {
        'ExtractType': 'full_extract',
    },
    'BillOfMaterial': {
        'ExtractType': 'full_extract',
    },
    'NAOverrideRequest': {
        'ExtractType': 'full_extract',
    },
    'BomOptionRevisionReference': {
        'ExtractType': 'full_extract',
    },
    'PriceOverride': {
        'ExtractType': 'full_extract',
    },
    'OrderItemOrder': {
        'ExtractType': 'full_extract',
    },
    'ShippingAddress': {
        'ExtractType': 'full_extract',
    },
    'OrderChangeNotesOrder': {
        'ExtractType': 'full_extract',
    },
    'CustomerAddressChangeRequest': {
        'Grain': 'SolutionId, SolutionRev, CustomerAddressId',
        'create_delta': 'CreationDate',
        'update_delta': 'UpdateDate',
        'ExtractType': 'delta_extract',
    },
    'xx_mfg_action': {
        'ExtractType': 'full_extract',
    },
    'xx_cust_ord': {
        'ExtractType': 'full_extract',
    },
    'xx_ord_head': {
        'ExtractType': 'full_extract',
    },
    'xx_ship_ord': {
        'ExtractType': 'full_extract',
    },
    'GeneratedOption': {
        'ExtractType': 'full_extract',
    },
    'EngineeringCost': {
        'ExtractType': 'full_extract',
    },
    'xx_mso_mstr': {
        'ExtractType': 'full_extract',
    },
    'xx_vid_info': {
        'ExtractType': 'full_extract',
    },
    'MatchUserSelection': {
        'ExtractType': 'full_extract',
    },
    'ChartOptionRevisionReference': {
        'ExtractType': 'full_extract',
    },
    'DealerChargesOrder': {
        'ExtractType': 'full_extract',
    },
    'DealerInvoiceOrder': {
        'ExtractType': 'full_extract',
    },
    'OrderHeaderOrder': {
        'ExtractType': 'full_extract',
    },
    'OwnerOrder': {
        'ExtractType': 'full_extract',
    },
    'QuoteOrder': {
        'ExtractType': 'full_extract',
    },
    'SolutionDetailOrder': {
        'Grain': 'SolutionId,SolutionRev,QuoteId',
        'create_delta': 'LastupdateDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SolutionOrder': {
        'Grain': 'SolutionId,SolutionRev,ModelId',
        'create_delta': 'CreationDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SpecHeaderOrder': {
        'ExtractType': 'full_extract',
    },
    'TaxesOrder': {
        'ExtractType': 'full_extract',
    },
    'NAOverrideOrder': {
        'ExtractType': 'full_extract',
    },
    'ProductOption': {
        'ExtractType': 'full_extract',
    },
    'DealerCharges': {
        'ExtractType': 'full_extract',
    },
    'DealerInvoice': {
        'ExtractType': 'full_extract',
    },
    'OrderHeader': {
        'ExtractType': 'full_extract',
    },
    'Owner': {
        'ExtractType': 'full_extract',
    },
    'Quote': {
        'ExtractType': 'full_extract',
    },
    'Solution': {
        'Grain': 'SolutionId,SolutionRev',
        'create_delta': 'CreationDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SolutionDetail': {
        'Grain': 'SolutionId,SolutionRev',
        'create_delta': 'LastupdateDate',
        'update_delta': 'LastupdateDate',
        'ExtractType': 'delta_extract',
    },
    'SpecHeader': {
        'ExtractType': 'full_extract',
    },
    'Taxes': {
        'ExtractType': 'full_extract',
    },
    'xx_upload_order': {
        'Grain': 'Id',
        'create_delta': 'DateSubmitted',
        'update_delta': 'DateSubmitted',
        'ExtractType': 'delta_extract',
    },
    'ShadowBomOption': {
        'ExtractType': 'full_extract',
    },
    'TranslationHeaderRequest': {
        'ExtractType': 'full_extract',
    },
    'BomMaster': {
        'ExtractType': 'full_extract',
    },
    'xx_trans_sls_code': {
        'ExtractType': 'full_extract',
    },
    'xx_transfer_order': {
        'Grain': 'id',
        'create_delta': 'TransferDate',
        'update_delta': 'TransferDate',
        'ExtractType': 'delta_extract',
    },
    'OrderChangeRequestRequest': {
        'ExtractType': 'full_extract',
    },
    'mapSpecId': {
        'ExtractType': 'full_extract',
    },
    'PropertyOverrideAction': {
        'ExtractType': 'full_extract',
    },
    'ShippingAddressChangeRequest': {
        'Grain': 'SolutionId,SolutionRev,ShippingAddressId',
        'create_delta': 'CreationDate',
        'update_delta': 'UpdateDate',
        'ExtractType': 'delta_extract',
    },
    'OptionSubCategory': {
        'ExtractType': 'full_extract',
    },
    'ModuleSelectionsValidated': {
        'ExtractType': 'full_extract',
    },
    'xx_pnt_mstr': {
        'ExtractType': 'full_extract',
    },
    'ExceptionLogBackup': {
        'Grain': 'ExceptionId',
        'create_delta': 'ExceptionTime',
        'update_delta': 'ExceptionTime',
        'ExtractType': 'delta_extract',
    },
    'MatchFlaggedAlteration': {
        'ExtractType': 'full_extract',
    },
    'xx_iman_action': {
        'ExtractType': 'full_extract',
    },
    'ShadowDesignChart': {
        'ExtractType': 'full_extract',
    },
    'TranslationHeaderOrder': {
        'ExtractType': 'full_extract',
    },
    'VendorPart': {
        'ExtractType': 'full_extract',
    },
    'BillOfMaterialRevisionReference': {
        'ExtractType': 'full_extract',
    },
    'mapSolutionId': {
        'ExtractType': 'full_extract',
    },
    'DesignChart': {
        'ExtractType': 'full_extract',
    },
    'MatchLockedSubCategory': {
        'ExtractType': 'full_extract',
    },
    'Bom': {
        'ExtractType': 'full_extract',
    },
    'NAOverride': {
        'ExtractType': 'full_extract',
    },
    'xx_new_sls': {
        'ExtractType': 'full_extract',
    },
    'SolutionAttachmentRequest': {
        'Grain': 'SolutionId,AttachmentIndex',
        'create_delta': 'CreationDate',
        'update_delta': 'LastUpdateDate',
        'ExtractType': 'delta_extract',
    },
    'SolutionAttachment': {
        'Grain': 'SolutionId,AttachmentIndex',
        'create_delta': 'CreationDate',
        'update_delta': 'LastUpdateDate',
        'ExtractType': 'delta_extract',
    },
    'ChartOptionTeamLeader': {
        'ExtractType': 'full_extract',
    },
    'DealerOptionRequest': {
        'ExtractType': 'full_extract',
    },
    'UserRoleTie': {
        'ExtractType': 'full_extract',
    },
    'DrivelineAutocalculationResultLog': {
        'Grain': 'ID',
        'create_delta': 'Date',
        'update_delta': 'Date',
        'ExtractType': 'delta_extract',
    },
    'SubCategory': {
        'ExtractType': 'full_extract',
    },
    'DrivelineTrans': {
        'ExtractType': 'full_extract',
    },
    'MatchSolution': {
        'ExtractType': 'full_extract',
    },
    'Match_UploadedOrders': {
        'Grain': 'OPrderId',
        'create_delta': 'LastUpdateDate',
        'update_delta': 'LastUpdateDate',
        'ExtractType': 'delta_extract',
    },
    'mapDealerOptionId': {
        'ExtractType': 'full_extract',
    },
    'ShadowChartOption': {
        'ExtractType': 'full_extract',
    },
    'mapQuoteId': {
        'ExtractType': 'full_extract',
    },
    'VendorFeature': {
        'ExtractType': 'full_extract',
    },
    'TeamLeaderBom': {
        'ExtractType': 'full_extract',
    },
    'ShadowBillOfMaterial': {
        'ExtractType': 'full_extract',
    },
    'ShadowGeneratedOption': {
        'ExtractType': 'full_extract',
    },
    'ShadowBomMaster': {
        'ExtractType': 'full_extract',
    },
    'SolutionAttachmentOrder': {
        'Grain': 'SolutionId,AttachmentIndex',
        'create_delta': 'CreationDate',
        'update_delta': 'LastUpdateDate',
        'ExtractType': 'delta_extract',
    },
    'UserSettings': {
        'Grain': 'UserId,[Key]',
        'create_delta': 'CreateDate',
        'update_delta': 'UpdateDate',
        'ExtractType': 'delta_extract',
    },
    'Users': {
        'ExtractType': 'full_extract',
    },
    'DrivelineSalesCodeRule': {
        'ExtractType': 'full_extract',
    },
    'DealerOption': {
        'ExtractType': 'full_extract',
    },
    'DealerOptionOrder': {
        'ExtractType': 'full_extract',
    },
    'ProductOptionDocument': {
        'ExtractType': 'full_extract',
    },
    'UsersOldPassword': {
        'ExtractType': 'full_extract',
    },
    'MatchDeletedOption': {
        'ExtractType': 'full_extract',
    },
    'QuantityRule': {
        'ExtractType': 'full_extract',
    },
    'PerfPsi': {
        'ExtractType': 'full_extract',
    },
    'ModelVisibility': {
        'ExtractType': 'full_extract',
    },
    'FeatureGroup': {
        'ExtractType': 'full_extract',
    },
    'PerfAuxTire': {
        'ExtractType': 'full_extract',
    },
    'PerfRearTire': {
        'ExtractType': 'full_extract',
    },
    'VendorTire': {
        'ExtractType': 'full_extract',
    },
    'DrivelineCDCode': {
        'ExtractType': 'full_extract',
    },
    'mapSalespersonId': {
        'ExtractType': 'full_extract',
    },
    'EcfDetail': {
        'ExtractType': 'full_extract',
    },
    'WeightImport': {
        'ExtractType': 'full_extract',
    },
    'Checkout': {
        'ExtractType': 'full_extract',
    },
    'VepsMaster': {
        'ExtractType': 'full_extract',
    },
    'TraceEvent': {
        'Grain': 'EventId',
        'create_delta': 'EventDate',
        'update_delta': 'EventDate',
        'ExtractType': 'delta_extract',
    },
    'QuantityRestrictionSubCat': {
        'ExtractType': 'full_extract',
    },
    'UserRoleFunctionTie': {
        'ExtractType': 'full_extract',
    },
    'Model': {
        'ExtractType': 'full_extract',
    },
    'PerfGAWR': {
        'ExtractType': 'full_extract',
    },
    'PerfFrontTire': {
        'ExtractType': 'full_extract',
    },
    'PerfGVWR': {
        'ExtractType': 'full_extract',
    },
    'Match_DatafeedLog': {
        'Grain': 'Id',
        'create_delta': 'Date',
        'update_delta': 'Date',
        'ExtractType': 'delta_extract',
    },
    'PerfModelDimension': {
        'ExtractType': 'full_extract',
    },
    'DealerRSM': {
        'ExtractType': 'full_extract',
    },
    'PerfEngine': {
        'ExtractType': 'full_extract',
    },
    'EngineVinData': {
        'ExtractType': 'full_extract',
    },
    'Driveline300Code': {
        'ExtractType': 'full_extract',
    },
    'Dealer': {
        'ExtractType': 'full_extract',
    },
    'ModelVisibilityUser': {
        'ExtractType': 'full_extract',
    },
    'MatchOpenSubCategory': {
        'ExtractType': 'full_extract',
    },
    'PerfRearAxle': {
        'ExtractType': 'full_extract',
    },
    'UserFunction': {
        'ExtractType': 'full_extract',
    },
    'Constant': {
        'ExtractType': 'full_extract',
    },
    'TraceSolution': {
        'Grain': 'Id',
        'create_delta': 'TraceCreationDate',
        'update_delta': 'TraceCreationDate',
        'ExtractType': 'delta_extract',
    },
    'Category': {
        'ExtractType': 'full_extract',
    },
    'mapCompanyId': {
        'ExtractType': 'full_extract',
    },
    'Lot': {
        'ExtractType': 'full_extract',
    },
    'PerfRearSuspension': {
        'ExtractType': 'full_extract',
    },
    'PerfFrontAxle': {
        'ExtractType': 'full_extract',
    },
    'DrivelineADim': {
        'ExtractType': 'full_extract',
    },
    'Databook': {
        'ExtractType': 'full_extract',
    },
    'DrivelineBDim': {
        'ExtractType': 'full_extract',
    },
    'PerfModel': {
        'ExtractType': 'full_extract',
    },
    'PerfTransmission': {
        'ExtractType': 'full_extract',
    },
    'GvwrRestrictions': {
        'ExtractType': 'full_extract',
    },
    'TraceNewSpec': {
        'Grain': 'Id',
        'create_delta': 'Date',
        'update_delta': 'Date',
        'ExtractType': 'delta_extract',
    },
    'SupplementaryData': {
        'ExtractType': 'full_extract',
    },
    'CwoCost': {
        'ExtractType': 'full_extract',
    },
    'DealerSettings': {
        'Grain': 'DealerId',
        'create_delta': 'CreateDate',
        'update_delta': 'UpdateDate',
        'ExtractType': 'delta_extract',
    },
    'DrivelineBracketData': {
        'ExtractType': 'full_extract',
    },
    'Attachment': {
        'ExtractType': 'skip',
    },
}

'''
    Create iter item to check table definition
'''
resource = {}
schema_list = {}

table_meta = iter(target_tables)
table_data = next(table_meta)


"""
    Primary column check
"""
loop = True
#region Loop Tables
while loop:
    loop = False
    next_table = None

    for q in table_data:
        table_name = q
        grain_check = table_data[table_name]['primary_column']
        delta_check = table_data[table_name]['create_delta']

    if table_name.split('.')[2] in adnet_inputs:
        """
            Manually Configured
        """
        if adnet_inputs[table_name.split('.')[2]]['ExtractType'] == 'full_extract':
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)

            resource[new_name] = {
                'sql': 'mssql_full_extract.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                }
            }

            next_table = True

        elif adnet_inputs[table_name.split('.')[2]]['ExtractType'] == 'skip':
            """
                Skip Tables
            """
            next_table = True
        else:
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)


            resource[new_name] = {
                'sql': 'mssql_delta_extract_2date_delta.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': adnet_inputs[table_name.split('.')[2]]['Grain'],
                    'create_delta': adnet_inputs[table_name.split('.')[2]]['create_delta'],
                    'update_delta': adnet_inputs[table_name.split('.')[2]]['update_delta']
                }
            }

            next_table = True
    else:
        """
            Check if programatic values can be used
        """
        if (grain_check != '__NoColumnNameDefined__' \
        and delta_check != '__NoColumnNameDefined__'):
            ####
            # Get table name in snake_case
            ####
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)

            resource[new_name] = {
                'sql': 'mssql_delta_extract_2date_delta.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': table_data[table_name]['primary_column'],
                    'create_delta': table_data[table_name]['create_delta'],
                    'update_delta': table_data[table_name]['update_delta']
                }
            }

            next_table = True
        else:
            next_table = None

    if next_table:
        f'{table_data} configured'
        grain_check = None
        table_data = next(table_meta)
        loop = True
#endregion

table_data[table_name]['table']


len(target_tables)
len(resource) - len(adnet_inputs)
len(target_tables) - len(resource)



body = {}
for table in resource:
    body[table] = resource[table]

###
# Check if directory exists
###
if not path.exists(f'zz_output/cloudformation/definitions/{target_database.lower()}'):
    makedirs(f'zz_output/cloudformation/definitions/{target_database.lower()}')

###
# Write config file
###
with open(f'zz_output/cloudformation/definitions/{target_database.lower()}/{target_schema.lower()}.yaml', mode = 'w') as f:
    f.write(dump(body, sort_keys = False))







"""
    Table does not have PKey defined
        pull table into python to investigate table further
"""



query = f'SELECT * FROM {table_name}'

df = polars.read_database(query, conn)

"""
    Table Count
"""
f'{int(df.select(polars.count()).item()):,}'
"""
    Headers
"""
df.head()


"""
    Identify grain
"""
int(df.select(polars.count()).item()) - int(df.select(polars.col('TruckId').n_unique()).item())

df = None
table_data = next(table_meta)

"""
    Identify tables not configured
"""
for x in target_tables:
    for y in x:
        if camelcase_to_snakecase(x[y]['table']) not in resource:
            y