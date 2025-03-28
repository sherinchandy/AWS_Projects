from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
import pysolr
import time
from typing import List, Dict, Any

class SolrToOpenSearchMigrator:
    def __init__(
        self,
        solr_url: str,
        opensearch_host: str,
        region: str,
        index_name: str,
        batch_size: int = 1000
    ):
        # Initialize Solr client
        self.solr = pysolr.Solr(solr_url, always_commit=True)
        
        # Initialize OpenSearch client
        #credentials = boto3.Session().get_credentials()
        #auth = AWSV4SignerAuth(credentials, region, 'aoss')
        auth = ('osadmin', 'Pass12345')
        
        self.opensearch_client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            #verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
        
        self.index_name = index_name
        self.batch_size = batch_size

    def create_index_if_not_exists(self, mapping: Dict = None):
        """Create OpenSearch index if it doesn't exist"""
        try:
            if not self.opensearch_client.indices.exists(self.index_name):
                if mapping:
                    self.opensearch_client.indices.create(
                        index=self.index_name,
                        body=mapping
                    )
                else:
                    self.opensearch_client.indices.create(index=self.index_name)
                print(f"Created index: {self.index_name}")
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            raise

    def get_total_docs(self) -> int:
        """Get total number of documents in Solr collection"""
        results = self.solr.search('*:*', rows=0)
        return results.hits

    def process_batch(self, docs: List[Dict[str, Any]]) -> None:
        """Process and index a batch of documents"""
        if not docs:
            return

        bulk_data = []
        for doc in docs:
            # Remove Solr-specific fields if they exist
            doc.pop('_version_', None)
            doc.pop('score', None)

            # Prepare bulk indexing action
            bulk_data.append({
                "index": {
                    "_index": self.index_name,
                    "_id": doc.get('id', None)
                }
            })
            bulk_data.append(doc)

        try:
            # Bulk index the documents
            response = self.opensearch_client.bulk(body=bulk_data)
            
            if response.get('errors', False):
                print("Some errors occurred during bulk indexing")
                for item in response['items']:
                    if item['index'].get('error'):
                        print(f"Error: {item['index']['error']}")
        except Exception as e:
            print(f"Error during bulk indexing: {str(e)}")
            raise

    def migrate(self) -> None:
        """Migrate documents from Solr to OpenSearch"""
        start = 0
        total_docs = self.get_total_docs()
        print(f"Total documents to migrate: {total_docs}")

        while start < total_docs:
            try:
                # Query Solr with pagination
                results = self.solr.search(
                    '*:*',
                    **{
                        'start': start,
                        'rows': self.batch_size,
                        'sort': 'id asc'
                    }
                )
                
                # Process the batch
                self.process_batch(results.docs)
                
                start += self.batch_size
                print(f"Processed {min(start, total_docs)}/{total_docs} documents")
                
                # Optional: Add a small delay to prevent overwhelming the servers
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing batch starting at {start}: {str(e)}")
                raise

def main():
    # Configuration
    solr_url = "http://ec2-35-37-80.us-west-2.compute.amazonaws.com:8983/solr/techproducts"
    opensearch_host = "search-dom1-yuzxmftfywbtrou.us-west-2.es.amazonaws.com"  # without https://
    region = "us-west-2"  # e.g., us-west-2
    index_name = "techproducts_1"
    
    # Optional: Define mapping for the OpenSearch index
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                # Add other fields as needed
            }
        }
    }

    # Initialize migrator
    migrator = SolrToOpenSearchMigrator(
        solr_url=solr_url,
        opensearch_host=opensearch_host,
        region=region,
        index_name=index_name,
        batch_size=1000
    )

    # Create index if it doesn't exist
    #migrator.create_index_if_not_exists(mapping)
    migrator.create_index_if_not_exists()

    # Start migration
    migrator.migrate()

if __name__ == "__main__":
    main()
