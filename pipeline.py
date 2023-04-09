import os
import shutil
import cv2
import io
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from google.cloud import storage

client = storage.Client()
bucket_name = "highd_project"
def generate_images(frame_data):
    
    # os.mkdir('images')
    df = pd.read_csv('gs://highd_project/01_tracks.csv')
    groups = df.groupby('frame')

    bucket = client.get_bucket(bucket_name)
    

    for frame, group in groups:
        # Create a new figure and axis for the current frame
        fig, ax = plt.subplots()

        # Set the axis limits
        ax.set_xlim(0, 200)
        ax.set_ylim(0, 50)

        # Iterate over the rows in the current group and add a rectangle to the axis for each row
        for index, row in group.iterrows():
            rect = patches.Rectangle((row['x'], row['y']), row['width'], row['height'], linewidth=1, edgecolor='r', facecolor='none')
            ax.add_patch(rect)

        # Save the figure as an image in the local directory
        blob = bucket.blob(f'images/{frame}.png') 
        ## Use bucket.get_blob('path/to/existing-blob-name.txt') to write to existing blobs
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)      
        blob.upload_from_file(buf)
        # plt.savefig(f'gs://highd_project/images/{frame}.png')
        buf.close()


        # Close the figure
        plt.close()


def generate_video(image_dir, video_path, fps=60):
    # Set up the OpenCV video writer
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter('zmy_video.mp4', fourcc, fps, (640, 480))

    # Loop through all image files in the local image directory
    count = len(os.listdir(image_dir))
    for i in range(count):
        image_path = os.path.join(image_dir, f'{i}.png')
        img = cv2.imread(image_path)
        if video_writer is None:
            height, width, _ = img.shape
            video_writer = cv2.VideoWriter(video_path, fourcc, fps, (width, height))
        video_writer.write(img)

    video_writer.release()

    # Clean up the local image directory
    # shutil.rmtree(image_dir)


def run_pipeline(argv=None):
    """Runs the dataflow pipeline."""
    pipeline_options = PipelineOptions(argv)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the file as a bounded source
        input_pcoll = (
            pipeline
            | 'Read File' >> beam.io.ReadFromText('gs://highd_project/01_tracks.csv')
        )

        # Apply the transformations to generate the images and video
        (
            input_pcoll
            | 'Generate Images' >> beam.Map(generate_images)
            | 'Generate Video' >> beam.Map(generate_video, 'gs://highd_project/images', 'gs://highd_project/my-video.mp4', 60)
        )


if __name__ == '__main__':
    run_pipeline()
