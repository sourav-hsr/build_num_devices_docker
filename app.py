import time
import boto3

#query = "SELECT * FROM ('stg-authdatadb'.sgcbgcensus)"

#2021-06-28

DATABASE = 'stg-authdatadb'
TABLE='sgcbgcensus'
TABLE2='home_panel_summary'



output='s3://stg-hsr-athena/bri-refactor/Sourav_test_athena/docker_github_actions_results/'

def lambda_handler(event, context):
    
    STATE=event['state'].lower()
    DATE=event['date']
    
    print(STATE)
    print(DATE)

    #query = """ SELECT * FROM "stg-authdatadb"."sgcbgcensus" """
    query=("SELECT b.number_devices_residing,"
    "b.census_block_group,"
    "c.b01001e1, "
    "b.date_range_start "
    f"""FROM "{DATABASE}"."{TABLE}" c """
    f"""LEFT OUTER JOIN "{DATABASE}"."{TABLE2}" b ON lpad(replace(c.census_block_group, '.0'), 12, '0') = lpad(replace(replace(b.census_block_group, 'CA:',''), '.0', ''), 12, '0') """
    f"""WHERE b.region = '{STATE}' """
    f"""and lpad(b.date_range_start, 10, '0') = '{DATE}' """)
    
    print(query)

    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )
    return response
