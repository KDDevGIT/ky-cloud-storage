import boto3
import os
import errno
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Initialize S3 client
s3_client = boto3.client('s3', region_name='us-east-2')

# Set your bucket name and local folder to monitor
BUCKET_NAME = 'kystoragebucket'
LOCAL_FOLDER = 'C:/Users/kdabc/Documents/Cloud Storage'

class S3SyncHandler(FileSystemEventHandler):
    """Handles file system events and syncs them to S3"""
    
    def on_modified(self, event):
        if not event.is_directory:
            if os.path.exists(event.src_path):
                self.upload_to_s3(event.src_path)

    def on_created(self, event):
        if event.is_directory:
            folder_key = os.path.relpath(event.src_path, LOCAL_FOLDER).replace('\\', '/') + '/'
            try:
                s3_client.put_object(Bucket=BUCKET_NAME, Key=folder_key)
                print(f"Simulated folder creation in s3://{BUCKET_NAME}/{folder_key}")
            except Exception as e:
                print(f"Failed to create folder in S3: {str(e)}")
        else:
            if os.path.exists(event.src_path):
                self.upload_to_s3(event.src_path)

    def on_deleted(self, event):
        if event.is_directory:
            folder_key = os.path.relpath(event.src_path, LOCAL_FOLDER).replace('\\', '/') + '/'
            print(f"Folder deleted: {event.src_path}")
            self.delete_from_s3(folder_key)
        else:
            file_key = os.path.relpath(event.src_path, LOCAL_FOLDER).replace('\\', '/')
            print(f"File deleted: {event.src_path}")
            self.delete_from_s3(file_key)

    def upload_to_s3(self, file_path):
        """Uploads the file to S3"""
        relative_path = os.path.relpath(file_path, LOCAL_FOLDER)
        s3_key = relative_path.replace('\\', '/')

        if os.path.basename(file_path).startswith('~$'):
            print(f"Skipping temporary file: {file_path}")
            return

        try:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as file:
                    s3_client.upload_fileobj(file, BUCKET_NAME, s3_key)
                print(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {file_path}: {str(e)}")

    def delete_from_s3(self, s3_key):
        """Deletes the file or folder from S3"""
        try:
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
            print(f"Deleted s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Failed to delete {s3_key} from S3: {str(e)}")

    def sync_s3_to_local(self):
        """Sync changes from S3 to the local folder without timeouts"""
        print("Syncing from S3 to local...")
        s3_objects = self.list_s3_objects()

        # List all files in local directory
        local_files = self.list_local_files()

        # Download new/updated files from S3
        for s3_key in s3_objects:
            local_path = os.path.join(LOCAL_FOLDER, s3_key.replace('/', '\\'))
            if not os.path.exists(local_path):
                print(f"File {local_path} not found locally, downloading...")
                self.download_from_s3(s3_key, local_path)

        # Remove files that were deleted in S3
        for local_file in local_files:
            s3_key = local_file.replace(LOCAL_FOLDER, '').replace('\\', '/').lstrip('/')
            if s3_key not in s3_objects:
                # Double-check existence in case of path format issues or sync timing problems
                if not any(s3_key == key for key in s3_objects):
                    print(f"File {local_file} exists locally but not in S3, deleting...")
                    os.remove(local_file)

    def download_from_s3(self, s3_key, local_path):
        """Downloads a file from S3 to the local system"""
        try:
            if not os.path.exists(os.path.dirname(local_path)):
                os.makedirs(os.path.dirname(local_path))  # Ensure the directory exists
                print(f"Created local directory: {os.path.dirname(local_path)}")

            s3_client.download_file(BUCKET_NAME, s3_key, local_path)
            print(f"Downloaded {s3_key} to {local_path}")
        except Exception as e:
            print(f"Failed to download {s3_key} from S3: {str(e)}")

    def list_s3_objects(self):
        """Returns a list of object keys in the S3 bucket"""
        s3_objects = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_objects.append(obj['Key'])
        return s3_objects

    def list_local_files(self):
        """Returns a list of file paths in the local folder"""
        local_files = []
        for root, dirs, files in os.walk(LOCAL_FOLDER):
            for file in files:
                local_files.append(os.path.join(root, file))
        return local_files

if __name__ == "__main__":

    if not os.path.exists(LOCAL_FOLDER):
        print(f"Error: The folder {LOCAL_FOLDER} does not exist.")
        exit(1)

    event_handler = S3SyncHandler()
    observer = Observer()
    observer.schedule(event_handler, path=LOCAL_FOLDER, recursive=True)
    print(f"Watching for changes in {LOCAL_FOLDER}...")

    try:
        observer.start()
        observer.join()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
