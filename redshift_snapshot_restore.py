#This script needs an CSV input file with columns in the order "region name", "snapshot identifier", "redshift subnet group name".

import sys
import boto3
import csv

#Function to launch cluster from snapshot 
def restore_rs_cluster(redshift_client, ClusterIdentifier, SnapshotIdentifier, Port, AvailabilityZone, ClusterSubnetGroupName, NodeType, NumberOfNodes, MaintenanceTrackName, Encrypted=False, KmsKeyId=None, ManageMasterPassword=False):
      if Encrypted and KmsKeyId is not None:
        try:
          print("Restoring KMS enabled " + ClusterIdentifier)
          restore_rs_cluster=redshift_client.restore_from_cluster_snapshot(ClusterIdentifier=ClusterIdentifier, 
                                                                     SnapshotIdentifier=SnapshotIdentifier,
                                                                     Port=Port, 
                                                                     AvailabilityZone=AvailabilityZone, 
                                                                     ClusterSubnetGroupName=ClusterSubnetGroupName,
                                                                     KmsKeyId=KmsKeyId,
                                                                     NodeType=NodeType,
                                                                     MaintenanceTrackName=MaintenanceTrackName,
                                                                     NumberOfNodes=NumberOfNodes,
                                                                     Encrypted=Encrypted,
                                                                     ManageMasterPassword=ManageMasterPassword
                                                                     )
          print(restore_rs_cluster)
        except Exception as e:
         print("Below exception happend for "+ ClusterIdentifier)
         print(e)
     
      else:
        try:
          print("Restoring Non KMS " + ClusterIdentifier)
          restore_rs_cluster=redshift_client.restore_from_cluster_snapshot(ClusterIdentifier=ClusterIdentifier, 
                                                                     SnapshotIdentifier=SnapshotIdentifier,
                                                                     Port=Port, 
                                                                     AvailabilityZone=AvailabilityZone, 
                                                                     ClusterSubnetGroupName=ClusterSubnetGroupName,
                                                                     NodeType=NodeType,
                                                                     MaintenanceTrackName=MaintenanceTrackName,
                                                                     NumberOfNodes=NumberOfNodes,
                                                                     Encrypted=Encrypted,
                                                                     ManageMasterPassword=ManageMasterPassword
                                                                     )
          print(restore_rs_cluster)
        except Exception as e:
         print("Below exception happend for "+ ClusterIdentifier)
         print(e)

#Function to extract details from snapshot. Expect a CSV file path(Ex: /home/user/file.csv) with columns in the order "region name", "snapshot identifier", "redshift subnet group name".
def get_snapshot_details(csv_file_path):
  with open(csv_file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader) #This line removes the header from CSV file, if the CSV file doesnt have a header, remove/comment this line. 
            for row in reader:
                 region_name=row[0].lower()
                 snapshot_id=row[1]
                 rs_subnet_grp=row[2]
                 redshift_client = boto3.client('redshift', region_name=region_name)
                 snapshot_details = redshift_client.describe_cluster_snapshots(SnapshotIdentifier=snapshot_id)
                 cluster_name = snapshot_details['Snapshots'][0]['ClusterIdentifier']
                 port=snapshot_details['Snapshots'][0]['Port']
                 node_type=snapshot_details['Snapshots'][0]['NodeType']
                 no_of_nodes=snapshot_details['Snapshots'][0]['NumberOfNodes']
                 avaialability_zone = snapshot_details['Snapshots'][0]['AvailabilityZone']
                 encrypted=snapshot_details['Snapshots'][0]['Encrypted']
                 maintenance_track=snapshot_details['Snapshots'][0]['MaintenanceTrackName']
                 if 'MasterPasswordSecretArn' in snapshot_details['Snapshots'][0]:
                  manage_master_passwd=True
                 else:
                  manage_master_passwd=False
                 if encrypted:
                  kms_key_id=snapshot_details['Snapshots'][0]['KmsKeyId']
                  restore_rs_cluster(redshift_client, cluster_name, snapshot_id, port, avaialability_zone, rs_subnet_grp, node_type, no_of_nodes, maintenance_track, encrypted, kms_key_id, manage_master_passwd)
                 else: 
                  restore_rs_cluster(redshift_client, cluster_name, snapshot_id, port, avaialability_zone, rs_subnet_grp, node_type, no_of_nodes, maintenance_track, encrypted, manage_master_passwd)
  
  
#redshift_client = boto3.client('redshift')
#response = redshift_client.describe_cluster_snapshots(SortingEntities=[{'Attribute':'CREATE_TIME','SortOrder':'DESC'}])
#snapshots = response['Snapshots']

#snapshot_id='idc-snap1'
#redshift_client = boto3.client('redshift', region_name='us-west-2')
#snapshot_details = redshift_client.describe_cluster_snapshots(SnapshotIdentifier=snapshot_id)
#restore_rs_cluster=redshift_client.restore_from_cluster_snapshot(SnapshotIdentifier=snapshot_id,ClusterIdentifier='cluster1-enc', Encrypted=True, ManageMasterPassword=True)
                  
#Main Function to read CSV file path as argument and pass it to the next function, CSV file should be of format "region name", "snapshot identifier", "redshift subnet group name". 
def main():
   if len(sys.argv) < 2:
          print("Error: Please provide a CSV file name as an argument. Scipt usage: python3 redshift_snapshot_restore.py /pathto/cavfile.csv")
          exit()
   else:
      csv_file_path=sys.argv[1]
      get_snapshot_details(csv_file_path)

if __name__ == "__main__":
   main()
