import base64
import json
import gzip
import boto3

def lambda_handler(event, context):
    database = 'dev' #Default database to connect to 
    group = 'group1' #group to which user needs to be added
    secret_arn = 'arn:aws:secretsmanager:us-west-2:1234567888:secret:xxxxxxx!xxxxxx-xxxx-4821-xxxx-xxxxxxxx-xxxxxxxx' #AWS Secrets Manager secret ARN
    base64Data = event['awslogs']['data']
    user_create_check = check_user_created(base64Data)
    if user_create_check is not None:
       user = user_create_check[0].strip()
       cluster_identifier = user_create_check[1]
       assign_user_perm(user, group, cluster_identifier, database, secret_arn)
    else:
       print('Not a Redshift user creation event')
       
def check_user_created(base64Data): #Function to verify whether the Lambda event is for Redshift user creation.
    actual_event = json.loads(gzip.decompress(base64.b64decode(base64Data)))
    try:
        if 'create' in (actual_event['logEvents'][0]['message']):
           user = str(actual_event['logEvents'][0]['message']).split('|')[1]
           cluster_identifier = actual_event['logStream']
           return user, cluster_identifier
        else:
           print('No user creation event in user log')
    except Exception as e:
       print(e)
    
def assign_user_perm(user, group, cluster_identifier, database, secret_arn):#Function to perform to add user to the group in Redshift.
    print(f'Assigning user {user} to group {group}'.format(user=user, group=group))
    sql = 'alter group {group} add user "{user}"'.format(group=group, user=user)#Construct the ALTER GROUP SQL
    print(sql)
    redshift_client = boto3.client('redshift-data', region_name='us-west-2')#Create a Boto3 client for Redshift Data API
    try:
        response = redshift_client.execute_statement(ClusterIdentifier=cluster_identifier,Database=database,SecretArn=secret_arn,Sql=sql)#Connect to Redshift and perform the ALTER GROUP ADD USER
        ID = response['Id']
        print(f'Redshift DataAPI Request Id: {ID}'.format(ID=ID))
    except Exception as e:
       print(f"Error executing SQL: {e}")
    redshift_client.close()
