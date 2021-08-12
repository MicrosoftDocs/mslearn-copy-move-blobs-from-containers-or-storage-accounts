using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;

namespace TransferBlobs
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string sourceConnection = args[0];
            string sourceContainer = args[1];
            string destConnection = args[2];
            string destContainer = args[3];
            DateTimeOffset transferBlobsModifiedSince = DateTimeOffset.Parse(args[4]);
            Console.WriteLine($"Moving blobs modified since {transferBlobsModifiedSince}");

            // Connect to Azure Storage
            BlobServiceClient sourceClient = new BlobServiceClient(sourceConnection);
            BlobServiceClient destClient = new BlobServiceClient(destConnection);

            BlobContainerClient sourceBlobContainer = sourceClient.GetBlobContainerClient(sourceContainer);
            sourceBlobContainer.CreateIfNotExists();

            BlobContainerClient destBlobContainer = destClient.GetBlobContainerClient(destContainer);
            destBlobContainer.CreateIfNotExists();

            // Find all blobs that have been changed since the specified date and time
            IEnumerable<BlobClient> sourceBlobRefs = await FindMatchingBlobsAsync(sourceBlobContainer, transferBlobsModifiedSince);

            // Move matching blobs to the destination container
            await MoveMatchingBlobsAsync(sourceBlobRefs, sourceBlobContainer, destBlobContainer);

            Console.WriteLine("\nDone");
        }

        // Find all blobs that have been modified since the specified date and time
        private static async Task<IEnumerable<BlobClient>> FindMatchingBlobsAsync(BlobContainerClient blobContainer, DateTimeOffset transferBlobsModifiedSince)
        {
            List<BlobClient> blobList = new List<BlobClient>();

            // Iterate through the blobs in the source container
            List<BlobItem> segment = await blobContainer.GetBlobsAsync(prefix: "").ToListAsync();
            foreach (BlobItem blobItem in segment)
            {
                BlobClient blob = blobContainer.GetBlobClient(blobItem.Name);

                // Check the source file's metadata
                Response<BlobProperties> propertiesResponse = await blob.GetPropertiesAsync();
                BlobProperties properties = propertiesResponse.Value;
                
                // Check the last modified date and time
                // Add the blob to the list if has been modified since the specified date and time
                if (DateTimeOffset.Compare(properties.LastModified.ToUniversalTime(), transferBlobsModifiedSince.ToUniversalTime()) > 0)
                {
                    blobList.Add(blob);
                }
            }

            // Return the list of blobs to be transferred
            return blobList;
        }

        // Iterate through the list of source blobs, and transfer them to the destination container
        private static async Task MoveMatchingBlobsAsync(IEnumerable<BlobClient> sourceBlobRefs, BlobContainerClient sourceContainer, BlobContainerClient destContainer)
        {
            foreach (BlobClient sourceBlobRef in sourceBlobRefs)
            {
                // Copy the source blob
                BlobClient sourceBlob = sourceContainer.GetBlobClient(sourceBlobRef.Name);

                // Check the source file's metadata
                Response<BlobProperties> propertiesResponse = await sourceBlob.GetPropertiesAsync();
                BlobProperties properties = propertiesResponse.Value;

                BlobClient destBlob = destContainer.GetBlobClient(sourceBlobRef.Name);
                CopyFromUriOperation ops = await destBlob.StartCopyFromUriAsync(GetSharedAccessUri(sourceBlobRef.Name, sourceContainer));

                // Display the status of the blob as it is copied
                while(ops.HasCompleted == false)
                {
                    long copied = await ops.WaitForCompletionAsync();

                    Console.WriteLine($"Blob: {destBlob.Name}, Copied: {copied} of {properties.ContentLength}");
                    await Task.Delay(500);
                }

                Console.WriteLine($"Blob: {destBlob.Name} Complete");

                // Remove the source blob
                bool blobExisted = await sourceBlobRef.DeleteIfExistsAsync();
            }
        }

        // Create a SAS token for the source blob, to enable it to be read by the StartCopyFromUriAsync method
        private static Uri GetSharedAccessUri(string blobName, BlobContainerClient container)
        {
            DateTimeOffset expiredOn = DateTimeOffset.UtcNow.AddMinutes(60);

            BlobClient blob = container.GetBlobClient(blobName);
            Uri sasUri = blob.GenerateSasUri(BlobSasPermissions.Read, expiredOn);

            return sasUri;
        }
    }
}
