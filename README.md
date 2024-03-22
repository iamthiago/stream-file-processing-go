# File Service in Go
This was created as part of a lab days project at one of the companies I work/worked for.
The general idea was to consume files from the origin bucket in GCS, do some processing and put it back in the destination bucket.
It uses the official Google library to access GCS, streaming the files all the way from the bucket and to the bucket as we apply transformations.
Files are gziped, so we also have to ungzip them while reading them.

Useful links:
- https://cloud.google.com/storage/docs/streaming-downloads#storage-stream-download-object-go
- https://cloud.google.com/storage/docs/streaming-uploads#storage-stream-upload-object-go
