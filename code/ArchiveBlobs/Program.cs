using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ArchiveBlobs
{
    class Program
    {
        static void Main(string[] args)
        {
            string sourceConnection = args[0];
            string sourceContainer = args[1];
            string destConnection = args[2];
            string destContainer = args[3];
            DateTime transferBlobsNotModifiedSince = DateTime.Parse(args[4]);
            Console.WriteLine($"Moving blobs not modified since {transferBlobsNotModifiedSince}");

            // Connect to Azure Storage
            CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(sourceConnection);
            CloudStorageAccount destAccount = CloudStorageAccount.Parse(destConnection);
            CloudBlobClient sourceClient = sourceAccount.CreateCloudBlobClient();
            CloudBlobClient destClient = destAccount.CreateCloudBlobClient();
            CloudBlobContainer sourceBlobContainer = sourceClient.GetContainerReference(sourceContainer);

            // Find all blobs that haven't changed since the specified date and time
            IEnumerable<ICloudBlob> sourceBlobRefs = FindMatchingBlobsAsync(sourceBlobContainer, transferBlobsNotModifiedSince).Result;

            // Move matching blobs to the destination container
            CloudBlobContainer destBlobContainer = destClient.GetContainerReference(destContainer);
            MoveMatchingBlobsAsync(sourceBlobRefs, sourceBlobContainer, destBlobContainer).Wait();

            Console.WriteLine("\nDone");
        }

        // Find all blobs that haven't been modified since the specified date and time
        private static async Task<IEnumerable<ICloudBlob>> FindMatchingBlobsAsync(CloudBlobContainer blobContainer, DateTime transferBlobsNotModifiedSince)
        {
            List<ICloudBlob> blobList = new List<ICloudBlob>();
            BlobContinuationToken token = null;

            // Iterate through the blobs in the source container
            do
            {
                BlobResultSegment segment = await blobContainer.ListBlobsSegmentedAsync(prefix: "", currentToken: token);
                foreach (CloudBlockBlob blobItem in segment.Results)
                {
                    ICloudBlob blob = await blobContainer.GetBlobReferenceFromServerAsync(blobItem.Name);

                    // Check the last modified date and time
                    // Add the blob to the list if has not been modified since the specified date and time
                    if (DateTime.Compare(blob.Properties.LastModified.Value.UtcDateTime, transferBlobsNotModifiedSince) <= 0)
                    {
                        blobList.Add(blob);
                    }
                }
            } while (token != null);

            // Return the list of blobs to be transferred
            return blobList;
        }

        // Iterate through the list of source blobs, and transfer them to the destination container
        private static async Task MoveMatchingBlobsAsync(IEnumerable<ICloudBlob> sourceBlobRefs, CloudBlobContainer sourceContainer, CloudBlobContainer destContainer)
        {
            foreach (ICloudBlob sourceBlobRef in sourceBlobRefs)
            {
                // Copy the source blob
                CloudBlockBlob destBlob = destContainer.GetBlockBlobReference(sourceBlobRef.Name);

                await destBlob.StartCopyAsync(new Uri(GetSharedAccessUri(sourceBlobRef.Name, sourceContainer)));

                // Display the status of the blob as it is copied
                ICloudBlob destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
                while (destBlobRef.CopyState.Status == CopyStatus.Pending)
                {
                    Console.WriteLine($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                    await Task.Delay(500);
                    destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
                }
                Console.WriteLine($"Blob: {destBlob.Name} Complete");

                // Remove the source blob
                bool blobExisted = await sourceBlobRef.DeleteIfExistsAsync();
            }
        }

        // Create a SAS token for the source blob, to enable it to be read by the StartCopyAsync method
        private static string GetSharedAccessUri(string blobName, CloudBlobContainer container)
        {
            DateTime toDateTime = DateTime.Now.AddMinutes(60);

            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sas = blob.GetSharedAccessSignature(policy);

            return blob.Uri.AbsoluteUri + sas;
        }
    }
}
