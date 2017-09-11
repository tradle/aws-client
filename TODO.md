
support multipart upload

maybe switch to s3-blob-store, if an s3-upload-client is created
watch this issue: https://github.com/jsantell/s3-stream-upload/issues/15
or implement multipart upload: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html

back off on _reset's that fail in a row
