
import asyncio
import logging
import time
import boto3
import cv2
from pathlib import Path
import io
from zipfile import ZipFile,ZIP_DEFLATED
import os
from os.path import basename
from collections import deque
import threading
import numpy as np
import time

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
            self.img_data={}
            self.buffer_size=10

            self.img_queue = deque([], maxlen=self.buffer_size)
            self.cond = threading.Condition()
            self.exit_event = threading.Event()
            self.write_thread = threading.Thread(target=self._write_img_data)

            
            current_timestamp = int(time.time())
            self.duration=300
            self.last_run=current_timestamp-current_timestamp%self.duration
            self.list_files=[]
            

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
        except Exception as e:
            LOGGER.exception(f"Exception while running. The exception is {e}")
    def start_stream(self):
        """Start capturing from file or device."""
        if not self.write_thread.is_alive():
            self.write_thread.start()

    def stop_stream(self):
        """Stop capturing from file or device."""
        with self.cond:
            self.exit_event.set()
            self.cond.notify()
        #TODO : clear the pending data to disk and s3
        self.img_queue.clear()
        self.write_thread.join()

# This method is thread triggered at beginning. It will read the image data from the queue. converts the data into given supported format, 
# store it in disk, every 5 minutes compresses all past 5 min images. Save it in disk, generates file url and s3 key, and send to stream manager to upload to s3
    def _write_img_data(self):
        while not self.exit_event.is_set():
            with self.cond:
                if len(self.img_queue) == 0 and not self.exit_event.is_set():
                    self.cond.wait()
                if len(self.img_queue) > 0:
                    img_data = self.img_queue.popleft()
                    try:
                        Path(img_data['fname']).parent.mkdir(parents=True, exist_ok=True)
                        if img_data['type'] == 'npy':
                            np.save(img_data['fname'] + '.npy', img_data['img'])
                        else:
                            cv2.imwrite(img_data['fname'],img_data['img'])
                        self.list_files.append(img_data['fname'])
                    except Exception as e:
                        LOGGER.info(f"Failed in saving the file to disk. The error is {e}")
                current_timestamp = int(time.time())
                if current_timestamp >= self.last_run + self.duration :
                    LOGGER.debug("It will upload images now")
                    tt=time.gmtime(self.last_run)
                    zipfile_loc=f"{self.store_id}/{tt.tm_mon}/{tt.tm_mday}/{tt.tm_hour}/{self.camera_id}"
                    zipfile_name=f"{tt.tm_min}.zip"
                    Path(f'{self.local_folder}/{zipfile_loc}/{zipfile_name}').parent.mkdir(parents=True, exist_ok=True)
                    
                    # create a ZipFile object
                    with ZipFile(f'{self.local_folder}/{zipfile_loc}/{zipfile_name}', 'w') as zipObj:
                    # Iterate over all the files in directory
                        for filename in self.list_files:
                            # Add file to zip
                            zipObj.write(filename, basename(filename)) 
                    # Send the image to stream manager
                    self.send_file_tos3(f'{self.local_folder}/{zipfile_loc}/{zipfile_name}',f'{zipfile_loc}/{zipfile_name}')
                    # reset the list of files
                    self.last_run=current_timestamp
                    self.list_files=[]

# Direct upload to s3 without using stream manager. File will be directly uploaded from memory to s3
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
# This method will be used to asynchronously upload images to s3 using threading and stream manager
# This appends the received image into the queue. 
    def send_frame_to_disk(self,frame,now,current_timestamp,frame_no,storeformat="jpg"):

        if storeformat not in ('png','jpg','npy'):
            LOGGER.error("Invalid format given. Supported formats are png|jpg|npy")
            raise
        
        uuid=self._gen_uuid(current_timestamp)
        # snapshot_filename = uuid + '.jpg'
        base_img_filename = f"{uuid}_{frame_no}.{storeformat}"
        
        s3_prefix = f"{self.upload_type}/{self.store_id}/{self.camera_id}/"
        s3_key = str(now.year) + '/' + str(now.month) + '/' + str(now.day) + '/' + \
        str(now.hour) + '/full/' + base_img_filename
        
        local_path = self.local_folder + '/' + s3_prefix + s3_key

        img_data={}
        img_data['fname']=local_path
        img_data['img']=frame
        img_data['type']=storeformat
        with self.cond:
            self.img_queue.append(img_data)
            self.cond.notify()


# This method will be used to asynchronously upload array of images to s3 using threading and stream manager
# This loops and appends the received array of images into a queue one by one
    def send_tracks_to_disk(self, imgs, trkids, tlbrs, now, current_timestamp, frame_no,storeformat="jpg"):
        
        if storeformat not in ('png','jpg','npy'):
            LOGGER.error("Invalid format given. Supported formats are png|jpg|npy")
            raise

        uuid=self._gen_uuid(current_timestamp)
        base_img_filename = f"{uuid}_{frame_no}.{storeformat}"
        
        s3_prefix = f"{self.upload_type}/{self.store_id}/{self.camera_id}/"
        s3_key = str(now.year) + '/' + str(now.month) + '/' + str(now.day) + '/' + \
        str(now.hour) + '/' 

        for img in zip(trkids,imgs,tlbrs):
            fname=f"{img[0]}_{img[2][0]}-{img[2][1]}-{img[2][2]}-{img[2][3]}_{base_img_filename}"
            img_data={}
            img_data['fname']=self.local_folder + '/' + s3_prefix + s3_key + '/' + fname
            img_data['img']=img[1]
            img_data['type']=storeformat
            with self.cond:
                self.img_queue.append(img_data)
                self.cond.notify()

# This method is used to store the image in local disk , returns the file url and s3 key
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

# this method is used by send_s3_export_imgs method. This will recieve the images, compress them, store the zip in local, returns the file url and s3 key
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
        with ZipFile(zip_buffer, "w", ZIP_DEFLATED, False) as zip_file:
            for img in zip(trkids,imgs,tlbrs):
                fname=f"{img[0]}_{img[2][0]}-{img[2][1]}-{img[2][2]}-{img[2][3]}_{base_img_filename}"
                is_success,data=cv2.imencode(".jpg", img[1])
                databytes=data.tobytes()
                zip_file.writestr(fname, databytes)

        with open(local_path, 'wb') as f:
            f.write(zip_buffer.getvalue())
            return local_path,(s3_prefix + s3_key)
    
# call this method to send given file url to upload to given s3 key location
    def send_file_tos3(self,file_url,key_name):
        input_file_url='file:' + file_url
        LOGGER.debug("The file_url : {} and the bucket name is : {} with the key : {}".format(input_file_url,self.bucket_name,key_name))
        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=input_file_url, bucket=self.bucket_name, key=key_name)
        LOGGER.debug(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            self.client.append_message(self.stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )

# call this method to send single image to s3. It internally stores the image in local disk first , generates file url and s3 key
    def send_s3_export(self,frame,now,current_timestamp):
        file_url,key_name=self._store_image(frame,now,current_timestamp)
        input_file_url='file:' + file_url
        LOGGER.debug("The file_url : {} and the bucket name is : {} with the key : {}".format(input_file_url,self.bucket_name,key_name))
        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=input_file_url, bucket=self.bucket_name, key=key_name)
        LOGGER.debug(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            self.client.append_message(self.stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )

# Call this method to send list of images directly send to s3 as a zip. It internally stores the images in zipe form in local disk first , generates file url and s3 key
    def send_s3_export_imgs(self,imgs,trkids,tlbrs,now,current_timestamp,frame_no):
        file_url,key_name=self._store_image_zip(imgs,trkids,tlbrs,now,current_timestamp,frame_no)
        input_file_url='file:' + file_url
        LOGGER.debug("The file_url : {} and the bucket name is : {} with the key : {}".format(input_file_url,self.bucket_name,key_name))
        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=input_file_url, bucket=self.bucket_name, key=key_name)
        LOGGER.debug(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            self.client.append_message(self.stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )