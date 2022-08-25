#!/usr/bin/env python3

from pathlib import Path
from types import SimpleNamespace
import argparse
import logging
import json
import cv2
import datetime
import numpy as np
import time
from fastmot.utils.rect import multi_crop

import fastmot
import fastmot.models
from fastmot.utils import ConfigDecoder, Profiler


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    group = parser.add_mutually_exclusive_group()
    required.add_argument('-i', '--input-uri', metavar="URI", required=True, help=
                          'URI to input stream\n'
                          '1) image sequence (e.g. %%06d.jpg)\n'
                          '2) video file (e.g. file.mp4)\n'
                          '3) MIPI CSI camera (e.g. csi://0)\n'
                          '4) USB camera (e.g. /dev/video0)\n'
                          '5) RTSP stream (e.g. rtsp://<user>:<password>@<ip>:<port>/<path>)\n'
                          '6) HTTP stream (e.g. http://<user>:<password>@<ip>:<port>/<path>)\n')
    required.add_argument('-p', '--modelpath', metavar="FILE", required=True, help="Path for the location of the models", default=None)
    optional.add_argument('-c', '--config', metavar="FILE",
                          default=Path(__file__).parent / 'cfg' / 'mot.json',
                          help='path to JSON configuration file')
    optional.add_argument('-l', '--labels', metavar="FILE",
                          help='path to label names (e.g. coco.names)')
    optional.add_argument('-o', '--output-uri', metavar="URI",
                          help='URI to output video file')
    optional.add_argument('-t', '--txt', metavar="FILE",
                          help='path to output MOT Challenge format results (e.g. MOT20-01.txt)')
    optional.add_argument('-f', '--send_frequency', 
                          help='Send frequency of mqtt and image uploads to cloud. This is used either sendmqtt or sends3 is set') 
    optional.add_argument('-b', '--mqtt_base_path', 
                          help='Base path of mqtt topic. All the mqtt topics begin with this path')
    optional.add_argument('-u', '--bucket_name', 
                          help='S3 bucket name where the uploaded images stored')
    optional.add_argument('-d', '--local_folder', 
                          help='Local folder location where the files one copy will be maintained')
    optional.add_argument('-r', '--store_id', 
                          help='Store id - uniquely identifies the deployed store. Used in s3 / mqtt messages')  
    optional.add_argument('-a', '--camera_id', 
                          help='Camera id - identified the camera within the store.Used in s3 / mqtt messages')                                                                                                                                
    optional.add_argument('-m', '--mot', action='store_true', help='run multiple object tracker')
    optional.add_argument('-e', '--sendmqtt', action='store_true', help='Send mqtt message to given mqtt topic')
    optional.add_argument('-s3', '--streamtos3', action='store_true', help='Send mqtt message to given mqtt topic')
    optional.add_argument('-s', '--show', action='store_true', help='show visualizations')
    group.add_argument('-q', '--quiet', action='store_true', help='reduce output verbosity')
    group.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity')
    parser._action_groups.append(optional)
    args = parser.parse_args()
    if args.txt is not None and not args.mot:
        raise parser.error('argument -t/--txt: not allowed without argument -m/--mot')

    # set up logging
    logging.basicConfig(format='%(asctime)s [%(levelname)8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(fastmot.__name__)
    if args.quiet:
        logger.setLevel(logging.WARNING)
    elif args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # load config file
    with open(args.config) as cfg_file:
        config = json.load(cfg_file, cls=ConfigDecoder, object_hook=lambda d: SimpleNamespace(**d))

    # load labels if given
    if args.labels is not None:
        with open(args.labels) as label_file:
            label_map = label_file.read().splitlines()
            fastmot.models.set_label_map(label_map)

    stream = fastmot.VideoIO(config.resize_to, args.input_uri, args.output_uri, **vars(config.stream_cfg))

    mot = None
    txt = None
    if args.mot:
        draw = args.show or args.output_uri is not None
        mot = fastmot.MOT(config.resize_to, args.modelpath, **vars(config.mot_cfg), draw=draw)
        mot.reset(stream.cap_dt)
    if args.txt is not None:
        Path(args.txt).parent.mkdir(parents=True, exist_ok=True)
        txt = open(args.txt, 'w')
    if args.show:
        cv2.namedWindow('Video', cv2.WINDOW_AUTOSIZE)

    # Create mqtt class if the mqtt args given
    if args.sendmqtt:
        try:
            mqttcli = fastmot.SendMQTT(10)
        except Exception as e:
            raise
        send_by_frame=int(stream.cap_fps) * int(args.send_frequency)
        general_topic=args.mqtt_base_path + '/general' 
        logger.info('Send MQTT is enabled. It will send by every %d frame',send_by_frame)
    else:
        logger.info('Send MQTT message to cloud is not enabled')

    # Create streamtos3 class if the upload to s3 is set to true
    if args.streamtos3:
        logger.info("Stream to s3 is set as true. S3 stream will be created and frames will be uploaded")
        try:
            streamtos3 = fastmot.StreamToS3(args.camera_id + '_upload_stream',args.store_id,args.camera_id,args.local_folder,args.bucket_name)
        except Exception as e:
            logger.error(f"Unable to create s3 stream. The exception is {e}")
            raise
    else:
        logger.info("Stream to s3 is not set. Images will not be streamed to s3 location")

    logger.info('Starting video capture...')
    stream.start_capture()
    # logger.info('Moving to the loop')
    # all_start=time.perf_counter()
    try:
        with Profiler('app') as prof:
            while not args.show or cv2.getWindowProperty('Video', 0) >= 0:
                # start=time.perf_counter()
                orig_frame,frame = stream.read()
                if frame is None:
                    break

                if args.mot:
                    mot.step(frame)
                    if txt is not None:
                        current_count=0
                        for track in mot.visible_tracks():
                            current_count += 1
                            tl = track.tlbr[:2] / config.resize_to * stream.resolution
                            br = track.tlbr[2:] / config.resize_to * stream.resolution
                            w, h = br - tl + 1
                            txt.write(f'{mot.frame_count},{track.trk_id},{tl[0]:.6f},{tl[1]:.6f},'
                                      f'{w:.6f},{h:.6f},-1,-1,-1\n')
                if (args.sendmqtt or args.streamtos3) and mot.frame_count % send_by_frame == 0:
                    now = datetime.datetime.now()
                    current_timestamp = str(int(now.timestamp()))
                    if args.streamtos3:
                        tmp_tlbrs=None
                        tmp_trkids=[]
                        current_count=0
                        for track in mot.visible_tracks():
                            tl = track.tlbr[:2] / config.resize_to * stream.resolution
                            br = track.tlbr[2:] / config.resize_to * stream.resolution
                            current_count += 1
                            tmp_tlbr=np.concatenate([tl,br])
                            if tmp_tlbrs is None:
                                tmp_tlbrs=np.asarray([tmp_tlbr])
                            else:
                                tmp_tlbrs=np.concatenate((tmp_tlbrs,[tmp_tlbr]),axis=0)
                            tmp_trkids.append(track.trk_id)    
                        if tmp_tlbrs is not None:
                            tmp_imgs = multi_crop(orig_frame, tmp_tlbrs)

                            logger.debug(f"TLBR shapes : {tmp_tlbrs.shape} and imgs shapes : {len(tmp_imgs)}")
                            try:
                                streamtos3.send_s3_export_imgs(tmp_imgs,tmp_trkids,tmp_tlbrs,now,current_timestamp,mot.frame_count)
                            except Exception as e:
                                logger.error(f"Error in uploading s3 file. The exception is {e}")   
                        else:
                            logger.debug("No tracks detected in this")

                    if args.sendmqtt:
                        general_inference_msg = {
                            "type" : "individual",
                            "frame_no": mot.frame_count,
                            "timestamp": current_timestamp,
                            "camera_id": args.camera_id,
                            "store_id": args.store_id,
                            "current_count": current_count,
                            "in_count": mot.tracker.in_count,
                            "out_count": mot.tracker.out_count,
                            "total_count": mot.tracker.total_count
                        }
                        logger.debug('General inference message - {general_inference_msg}')

                        try:
                            mqttcli.send_mqtt_msg(general_topic,general_inference_msg)
                        except Exception as e:
                            logger.error(f"Error in sending mqtt message to cloud. The error is - {e}")
                        
                        #Resetting in and out count for the next cycle
                        mot.tracker.in_count = 0
                        mot.tracker.out_count = 0

                    
                if args.show:
                    cv2.imshow('Video', frame)
                    if cv2.waitKey(1) & 0xFF == 27:
                        break
                if args.output_uri is not None:
                    stream.write(frame)
                # end=time.perf_counter()
                # logger.info(f"Current step time : {end-start} on frame number {mot.frame_count}")
    finally:
        # clean up resources
        if txt is not None:
            txt.close()
        stream.release()
        cv2.destroyAllWindows()
        # all_end=time.perf_counter()

    # timing statistics
    if args.mot:
        avg_fps = round(mot.frame_count / prof.duration)
        logger.info('Average FPS: %d', avg_fps)
        mot.print_timing_info()
        # logger.info(f"Total execution time : {all_end-all_start}")


if __name__ == '__main__':
    main()
