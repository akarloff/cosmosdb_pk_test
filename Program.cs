using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Threading.Tasks;

namespace CosmosDBTest
{
    class Program
    {
        public class CosmosDbStore
        {
            private const string _databaseId = "db";
            private const string _collectionId = "common";
            private const string _accountEndpoint = "add endpoint here";
            private const string _accountKey = "add key here";
            private const string _partitionKeyName = "PartitionKey";
            private const string _documentIdName = "id";
            private const string _eTagName = "_etag";
            private const string _ttlName = "ttl";
            private const int _ttlInSeconds = 500;

            private readonly DocumentClient _client;

            public readonly string BasePartitionKey = "CosmosDbPKTest_";
            public readonly string BaseDocumentId = "CosmosDbPKTestDocument_";

            public CosmosDbStore(string basePartitionKey = null)
            {
                var connectionPolicy = new ConnectionPolicy();
                _client = new DocumentClient(new Uri(_accountEndpoint), _accountKey, connectionPolicy);
            }

            public async Task<ResourceResponse<Document>> CreateDocumentAsync(string partitionKey, string documentId)
            {
                var document = new JObject();
                document.Add(_partitionKeyName, GetStorePartitionKey(partitionKey));
                document.Add(_documentIdName, GetStoreDocumentId(documentId));
                document.Add(_eTagName, null);
                document.Add(_ttlName, _ttlInSeconds);

                return await _client.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
                    document,
                    GetCorrectWriteRequestOptions(document),
                    true).ConfigureAwait(false);
            }

            public async Task<ResourceResponse<Document>> GetDocumentAsync(string partitionKey, string documentId)
            {
                return await _client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(
                        _databaseId, 
                        _collectionId, 
                        GetStoreDocumentId(documentId)),
                    GetCorrectReadRequestOptions(partitionKey)).ConfigureAwait(false);
            }

            private RequestOptions GetCorrectWriteRequestOptions(JObject document)
            {
                // Throw etag conflict
                var ac = new AccessCondition
                {
                    Condition = document.GetValue(_eTagName).ToString(),
                    Type = AccessConditionType.IfMatch
                };

                var requestOptions = new RequestOptions
                {
                    PartitionKey = new PartitionKey(document.GetValue(_partitionKeyName).ToString()),
                    AccessCondition = ac
                };

                return requestOptions;
            }

            private RequestOptions GetCorrectReadRequestOptions(string partitionKey)
            {
                return new RequestOptions
                {
                    PartitionKey = new PartitionKey(GetStorePartitionKey(partitionKey))
                };
            }

            private string GetStorePartitionKey(string partitionKey)
            {
                return BasePartitionKey + partitionKey;
            }

            private string GetStoreDocumentId (string documentId)
            {
                return BaseDocumentId + documentId;
            }
        }

        static void Main(string[] args)
        {
            var store = new CosmosDbStore();
            var pk = Guid.NewGuid().ToString();
            var docId = Guid.NewGuid().ToString().PadRight(50, 'y');

            // Test writing the same documentId to multiple partitions
            // See when we trigger a conflict for a document that does not exist.
            for (int pkLength = 75; pkLength < 100; pkLength++)
            {
                for (int pkInd = 0; pkInd < 10; pkInd++)
                {
                    var partitionKey = (pk + "_" + pkInd).PadLeft(pkLength, 'x'); // Note: PadRight always succeeds
                    try
                    {
                        store.CreateDocumentAsync(partitionKey, docId).GetAwaiter().GetResult();
                    }
                    catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        // Throws not found when the doc should exist because a conflict was detected
                        store.GetDocumentAsync(partitionKey, docId).GetAwaiter().GetResult();
                    }
                }
            }
        }
    }
}
