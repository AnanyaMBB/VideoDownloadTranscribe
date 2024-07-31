from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
import time
import json
import brotli
from hashlib import md5
import sqlite3
from collections import deque
from dotenv import load_dotenv
import os 
import redis 

load_dotenv()

def extract_number(filename): 
    return int(filename.split('.')[0])

try: 
    count = int(sorted(os.listdir(os.getcwd() + './dataset/unparsed_json'), key=extract_number)[-1].split('.')[0]) 
except: 
    count = 0
    
print("Count: ", count)

username = os.getenv("INSTAGRAM_USERNAME")
password = os.getenv("INSTAGRAM_PASSWORD")

redisClient = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0)



url="https://www.instagram.com/"
driver = webdriver.Chrome()


try:
    
    driver.get(url)
    time.sleep(2)

    username_field = driver.find_element("name", "username")
    username_field.send_keys(username)
    password_field = driver.find_element("name", "password")
    password_field.send_keys(password)
    password_field.send_keys(Keys.RETURN)

    time.sleep(4)

    driver.requests.clear()
    reels_url = "https://www.instagram.com/reels/"
    driver.get(reels_url)

    actions = ActionChains(driver)
    processed_requests = set()
    
    processed_hashes = set()

    # count = 0
    # count = int(sorted(os.listdir(os.getcwd() + './dataset/unparsed_json'), key=extract_number)[-1].split('.')[0])
    # print("Count: ", count)
    for _ in range(100000):
        time.sleep(2)
        for request in driver.requests:
            if request.response is not None:
                if request not in processed_requests:
                    if request.url == 'https://www.instagram.com/graphql/query':
                        print("Found")
                        body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity')).decode('utf-8')
                        
                        
                        # Compute the MD5 hash of the body
                        body_hash = md5(body.encode('utf-8')).hexdigest()
                        
                        # Check if this hash has already been processed
                        if body_hash in processed_hashes:
                            continue  # Skip this response

                        body = json.loads(body)
                        print(body['data'])
                        if 'xdt_api__v1__clips__home__connection_v2' in body['data'].keys():
                            file_path = f'./dataset/unparsed_json/{count}.json'
                            # Write the body to the file as it is not a duplicate
                            with open(file_path, 'w', encoding='utf-8') as file:
                                file.write(json.dumps(body, indent=4, ensure_ascii=False))
                            
                            redisClient.rpush('unparsed_json_queue', file_path)
                            # Add the hash of this body to the set of processed hashes
                            processed_hashes.add(body_hash)
                            count += 1
                
            processed_requests.add(request)
        
        actions.send_keys(Keys.PAGE_DOWN).perform()
        

finally:
    driver.quit()