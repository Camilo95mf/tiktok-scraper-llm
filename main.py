
import os
from TikTokManager.tiktokmanager import TikTokManager

# import pyktok as pyk
# import asyncio
# import json


# pyk.specify_browser('firefox')


# pyk.save_tiktok('https://www.tiktok.com/@talianavargass/video/7428749845650754822?is_from_webapp=1&sender_device=pc',
# 	        True,
#                 'video_data.csv',
# 		'firefox')

# pyk.get_video_urls('cop16', ent_type="video_related", video_ct=30, headless=True)

# print(pyk.ms_token)

# asyncio.run(pyk.get_video_urls('cop16', ent_type="video_related", video_ct=3, headless=False))


# def test_TikTok():
#     print("here")
#     pyk.specify_browser('firefox')

    #Meta data from multi videos based on hashtag
    # pyk.save_tiktok_multi_page('datascience',ent_type='hashtag',video_ct=10,save_video=False,metadata_fn='./output/test_datascience.csv', sleep=10)
    
    #Download a video
    # pyk.save_tiktok(
    #     'https://www.tiktok.com/@talianavargass/video/7428749845650754822?is_from_webapp=1&sender_device=pc',
	#     True,
    #     '/output/video_data.csv',
	# 	'firefox')

    # tt_json = pyk.alt_get_tiktok_json('https://www.tiktok.com/@tiktok/video/7362568430282738987') ####### look at this to know how to search for a specific video
    # json_data = json.dumps(tt_json, indent=4)
    # with open("./output/data.json", "w") as json_file:
    #     json_file.write(json_data)
    # print(tt_json)
    
    # pyk.get_video_urls('cop16', ent_type="video_related", video_ct=5, headless=True)

    # asyncio.run(pyk.get_video_urls('cop16', ent_type="video_related", video_ct=3, headless=False))


if __name__ == "__main__":
    tkm = TikTokManager("./output", "./temp")
    # res = tkm.extract_videos_data_v2(["carlosfernandogalan","galánalcalde","alcaldegalan"], 700, 2000)
    res = tkm.extract_videos_data_v2(["gustavopetro","GobiernoColombiano","petropresidente"], 1000, 2000)
    # res = tkm.extract_videos_data_v2(["trump"], 5, 1)
    # res = tkm.get_video_transcription(['7229747166057712901'])
    print(res)