## Sample Python code to assign IDCS user to Redshift IDCS application. https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-iam-access-control-idp-connect.html

aws_access_key_id='ajgdkfgkjgflfg'
aws_secret_access_key='gkwejfgjwefgjwfg'

idcs_app_provider_arn = 'arn:aws:sso::aws:applicationProvider/redshift'
identity_store_id = ""
idcs_instance_arn = ""
redshift_idcs_app_arn = ""

#Below variables to pass from calling app.
idcs_instance_name = 'IDCS-Instance-1'
aws_account = 1234556567
aws_region = 'us-west-2'
attr_path = 'UserName'
attr_value = 'DWUSER1'
principal_type = 'USER'

import boto3
boto3.setup_default_session(region_name=aws_region) #Set region
#SSO client 
sso_client = boto3.client('sso-admin', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
#IDCS client
idcs_client = boto3.client('identitystore', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

#Extract IDCS identity store ID & IDCS instance ARN
idcs_instances = sso_client.list_instances()
for items in idcs_instances["Instances"]:
    if (items["Name"] == idcs_instance_name and items["OwnerAccountId"] == str(aws_account)):
        identity_store_id = items["IdentityStoreId"]
        idcs_instance_arn = items["InstanceArn"]

#Extract IDCS user principal ID    
user_pricipal_id = idcs_client.list_users(IdentityStoreId=identity_store_id, Filters=[{'AttributePath': attr_path, 'AttributeValue': attr_value}])['Users'][0]['UserId']
#Extract Redshift IDCS App ARN
redshift_idcs_app_arn = sso_client.list_applications(InstanceArn=idcs_instance_arn, Filter={'ApplicationAccount': str(aws_account), 'ApplicationProvider': idcs_app_provider_arn})["Applications"][0]["ApplicationArn"]
#Assign IDCS user to Redshift IDCS Application(Assign button functionlaity on Redshift IDCS console)
response = sso_client.create_application_assignment(ApplicationArn=redshift_idcs_app_arn, PrincipalId=user_pricipal_id, PrincipalType=principal_type)
print(response["ResponseMetadata"]['HTTPStatusCode'])
