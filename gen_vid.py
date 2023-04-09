import cv2
import os
def generate_video(image_dir, video_path, fps=60):
    # Set up the OpenCV video writer
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter(video_path, fourcc, fps, (640, 480))

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

generate_video('images', 'demo.mp4')