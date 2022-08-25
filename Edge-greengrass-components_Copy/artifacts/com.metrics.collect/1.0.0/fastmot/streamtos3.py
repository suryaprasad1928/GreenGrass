
import asyncio
import logging
import time
import boto3
import cv2
from pathlib import Path
import io
import zipfile

from stream_manager import (
    ExportDefinition,
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    S3ExportTaskDefinition,
    S3ExportTaskExecutorConfig,
    Status,
    StatusConfig,
    StatusLevel,
    StatusMessage,
    StrategyOnFull,
    StreamManagerClient,
    StreamManagerException,
)
from stream_manager.util import Util
import logging

LOGGER = logging.getLogger(__name__)

class StreamToS3:
    """
    Class for uploading images to s3 through greengrass stream manager.
    Parameters
    ----------

    """
    def __init__(self, stream_name,store_id,camera_id,local_folder,bucket_name,upload_type='image'):
        try:
            self.stream_name = stream_name
            self.store_id = store_id
            self.camera_id = camera_id
            self.local_folder = local_folder
            self.bucket_name = bucket_name
            self.upload_type = upload_type
            self.client = StreamManagerClient()
            # Try deleting the stream (if it exists) so that we have a fresh start
            try:
                self.client.delete_message_stream(stream_name=stream_name)
            except ResourceNotFoundException:
                pass

            self.exports = ExportDefinition(
                s3_task_executor=[
                    S3ExportTaskExecutorConfig(
                        identifier="S3TaskExecutor" + stream_name,  # Required
                        # Optional. Add an export status stream to add statuses for all S3 upload tasks.
                        # status_config=StatusConfig(
                        #     status_level=StatusLevel.INFO,  # Default is INFO level statuses.
                        #     # Status Stream should be created before specifying in S3 Export Config.
                        #     status_stream_name=status_stream_name,
                        # ),
                    )
                ]
            )
            # Create the message stream with the S3 Export definition.
            self.client.create_message_stream(
                MessageStreamDefinition(
                    name=stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData, export_definition=self.exports
                )
            )
        except Exception:
            LOGGER.exception("Exception while running")
    
    @staticmethod
    def write_fileobj_to_s3(s3_bucket_name,s3_prefix,s3key, frame,log):
        s3 = boto3.resource("s3")
        is_success, im_arr = cv2.imencode(".jpg", frame)
        im_bytes = im_arr.tobytes()
        try:
            #s3.Bucket(s3_bucket_name).upload_file(Key=s3_prefix + s3key, Filename=filename)
            s3.Bucket(s3_bucket_name).put_object(Key=s3_prefix + s3key, Body=im_bytes)
        except Exception as e:
            LOGGER.error(f"[write_to_s3] Error - the exception is {e}")
            # raise the exception to indicate failure
            raise
            
        LOGGER.debug('[write_to_s3] Uploaded ' + s3key + ' to S3') 

    
    def _gen_uuid(self,current_timestamp):
        uuid=f'{current_timestamp}_{self.camera_id}_{self.store_id}'
        return uuid

    def _store_image(self,frame,now,current_timestamp):
        uuid=self._gen_uuid(current_timestamp)
        snapshot_filename = uuid + '.jpg'
        
        s3_prefix = f"{self.upload_type}/{self.store_id}/{self.camera_id}/"
        s3_key = str(now.year) + '/' + str(now.month) + '/' + str(now.day) + '/' + \
        str(now.hour) + '/' + snapshot_filename
        
        local_path = self.local_folder + '/' + s3_prefix + s3_key

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        success = cv2.imwrite(local_path,frame)
        
        if success:
            print('success writing')
            return local_path,(s3_prefix + s3_key)
        else:
            print('failure writing')
            return None

    def _store_image_zip(self,imgs,trkids,tlbrs,now,current_timestamp,frame_no):
        uuid=self._gen_uuid(current_timestamp)
        base_img_filename = f"{uuid}_{frame_no}.jpg"
        snapshot_filename = f"{uuid}_{frame_no}.zip"
        
        s3_prefix = f"{self.upload_type}/{self.store_id}/{self.camera_id}/"
        s3_key = str(now.year) + '/' + str(now.month) + '/' + str(now.day) + '/' + \
        str(now.hour) + '/' + snapshot_filename
        
        local_path = self.local_folder + '/' + s3_prefix + s3_key

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, False) as zip_file:
            for img in zip(trkids,imgs,tlbrs):
                fname=f"{img[0]}_{img[2][0]}-{img[2][1]}-{img[2][2]}-{img[2][3]}_{base_img_filename}"
                is_success,data=cv2.imencode(".jpg", img[1])
                databytes=data.tobytes()
                zip_file.writestr(fname, databytes)

        with open(local_path, 'wb') as f:
            f.write(zip_buffer.getvalue())
            return local_path,(s3_prefix + s3_key)

    def send_s3_export(self,frame,now,current_timestamp):
        file_url,key_name=self._store_image(frame,now,current_timestamp)
        input_file_url='file:' + file_url
        LOGGER.debug("The file_url : {} and the bucket name is : {} with the key : {}".format(input_file_url,self.bucket_name,key_name))
        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=input_file_url, bucket=self.bucket_name, key=key_name)
        LOGGER.info(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            self.client.append_message(self.stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )

    def send_s3_export_imgs(self,imgs,trkids,tlbrs,now,current_timestamp,frame_no):
        file_url,key_name=self._store_image_zip(imgs,trkids,tlbrs,now,current_timestamp,frame_no)
        input_file_url='file:' + file_url
        LOGGER.debug("The file_url : {} and the bucket name is : {} with the key : {}".format(input_file_url,self.bucket_name,key_name))
        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=input_file_url, bucket=self.bucket_name, key=key_name)
        LOGGER.info(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            self.client.append_message(self.stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )