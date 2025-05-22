"""This is the main file for the TikTokManager project.
It initializes the TikTokManager class and calls its methods to extract video data and transcriptions.
"""
from TikTokManager.tiktokmanager import TikTokManager


if __name__ == "__main__":
    tkm = TikTokManager("./output", "./temp")
    # res = tkm.extract_videos_data_v2(["carlosfernandogalan","galánalcalde","alcaldegalan"], 700, 2000)
    res = tkm.extract_videos_data_v2(["gustavopetro","GobiernoColombiano","petropresidente"], 1000, 2000)
    # res = tkm.extract_videos_data_v2(["trump"], 5, 1)
    # res = tkm.get_video_transcription(['7229747166057712901'])
    print(res)