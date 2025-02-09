"""
Class to manage data extraction from tiktok api. 
- code based on pyktok library
"""
import json
import os
import asyncio
import time

from TikTokApi import TikTokApi
import pyktok as pyk
import requests
import pandas as pd

# from TikTokApi import TikTokApi
# import asyncio
# import os

ms_token = os.environ.get("ms_token", None)  # set your own ms_token
context_dict = {'viewport': {'width': 0,
                             'height': 0},
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'}

class TikTokManager():
    """Class to extract data from tiktok api"""
    def __init__(self, output_path:str, temp_path:str) -> None:
        self.output_path = output_path
        self.temp_path = temp_path
        pyk.specify_browser('firefox')

    async def get_video_urls(self,
                            tt_ent,
                            video_ct:int,
                            ent_type:str,
                            headless=True
                            ):
        """
        Extract video urls based on tt_ent param.
        ent_type must be "user", "hashtag", or "video_related"
        """
        if ent_type not in ['user','hashtag','video_related']:
            raise ValueError('Only allowed `ent_type` values are "user", "hashtag", or "video_related".')

        url_p1 = "https://www.tiktok.com/@"
        url_p2 = "/video/"
        tt_list = []

        async with TikTokApi() as api:
            await api.create_sessions(headless=headless,
                                    ms_tokens=[ms_token],
                                    num_sessions=1,
                                    sleep_after=3,
                                    context_options=context_dict)
            if ent_type == 'user':
                ent = api.user(tt_ent)
            elif ent_type == 'hashtag':
                ent = api.hashtag(name=tt_ent)
            elif ent_type == 'key_words':
                ent = api.search()
            else:
                ent = api.video(url=tt_ent)

            video_count = 0
            if ent_type in ['user','hashtag']:
                async for video in ent.videos(count=video_ct):
                    video_count += 1
                    if video_count > video_ct:
                        break
                    tt_list.append(video.as_dict)
            else:
                async for related_video in ent.related_videos(count=video_ct):
                    video_count += 1
                    if video_count > video_ct:
                        break
                    tt_list.append(related_video.as_dict)

        id_list = [i['id'] for i in tt_list]
        if ent_type == 'user':
            video_list = [url_p1 + tt_ent + url_p2 + i for i in id_list]
        else:
            author_list = [i['author']['uniqueId'] for i in tt_list]
            video_list = []
            for n, i in enumerate(author_list):
                video_url = url_p1 + author_list[n] + url_p2 + id_list[n]
                video_list.append(video_url)
        return video_list
        
    def extract_videos_data(self, hashtag:str, video_amount:int):
        """
        Extract relevant info from videos related with hashtag parameter
        - video limit extraction = video_amount
        """
        tiktok_url = 'https://www.tiktok.com/@tiktok/video/'
        temp_data_path = f'{self.temp_path}/temp_data.csv'
        output_path = f'{self.output_path}/video_info.xlsx'
        print(video_amount)

        url_list = asyncio.run(self.get_video_urls(
                                            tt_ent=hashtag,
                                            ent_type="hashtag",
                                            video_ct=video_amount,
                                        ))

        pyk.save_tiktok_multi_urls(
                                    video_urls=url_list,
                                    save_video=False,
                                    sleep=5,
                                    metadata_fn=temp_data_path
                                )
        
        data_df = pd.read_csv(temp_data_path, sep=',', dtype=str)
        data_df['video_language'] = ''
        data_df['video_trasncription'] = ''
        # print(data_df)

        for index, row in data_df.iterrows():
            aux_url = tiktok_url+row['video_id']
            # print(row)
            tiktok_json = pyk.alt_get_tiktok_json(aux_url)
            if tiktok_json is not None:
                #Temporal
                # json_data = json.dumps(tiktok_json, indent=4)
                # with open("./output/data_test2.json", "w") as json_file:
                #     json_file.write(json_data)

                trasncription_list = tiktok_json.get("__DEFAULT_SCOPE__").get("webapp.video-detail").get("itemInfo").get("itemStruct").get("video").get("subtitleInfos")
                if len(trasncription_list) > 0:
                    language = trasncription_list[0].get("LanguageCodeName")
                    trasncription_url = trasncription_list[0].get("Url")
                    response = requests.get(trasncription_url, timeout=10)
                    result_array = response.text.split('\n')
                    result_array = [item for item in result_array if item and "-->" not in item and "WEBVTT" not in item]
                    final_trasncription =' '.join(result_array)

                    data_df.at[index, 'video_language'] = language
                    data_df.at[index, 'video_trasncription'] = final_trasncription
            break

        data_df.to_excel(output_path,'Results', index=False)
        



        # pyk.save_tiktok_multi_page(
        #     hashtag, ent_type='hashtag',
        #     video_ct=video_amount,
        #     save_video=False,
        #     metadata_fn=temp_data_path,
        #     sleep=5
        # )

        # data_df = pd.read_csv(temp_data_path, sep=',')
        # print(data_df)
