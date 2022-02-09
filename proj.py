from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
from bs4 import BeautifulSoup
import psycopg2

# DB 접속 ===========================================================

conn_str = "host=localhost dbname=crawling_TEAM user=postgres password=admin port=5432"
try:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    print("=== 접속 성공 =====")
except psycopg2.DatabaseError as db_err:
    print("접속오류 !!")
    print(db_err)
    exit(1)

# DB 테이블 생성 ===========================================================

###기본정보 TABLE 생성###

try:
    cur.execute("drop table if exists basic_info;")
    cur.execute('''create table basic_info (id serial,
        title varchar(200),
        release_date varchar(30),
        price integer,
        evaluation varchar(50),
        evaluation_cnt varchar(50),
        evaluation_pct varchar(50),
        genre varchar(200),
        developer varchar(1000),
        publisher varchar(1000)
        );''')
    conn.commit()
    print('기본정보 테이블 생성 success')
except:
    conn.rollback()
    print("--- 기본정보 테이블 생성 실패!")
    exit()

###특징 TABLE 생성###

try:
    cur.execute("drop table if exists feature;")
    cur.execute('''create table feature (id serial,
        single boolean NOT NULL,
        multi boolean NOT NULL,
        online_pvp boolean NOT NULL,
        lan_pvp boolean NOT NULL,
        shared_split_screen_pvp boolean NOT NULL,
        online_coop boolean NOT NULL,
        lan_coop boolean NOT NULL,
        shared_split_screen_coop boolean NOT NULL,
        cross_platform_multiplayer boolean NOT NULL,
        language varchar(300)
        );''')
    conn.commit()
    print('특징 테이블 생성 success')
except:
    conn.rollback()
    print("--- 특징 테이블 생성 실패!")
    exit()

###최소사양 TABLE 생성###

try:
    cur.execute("drop table if exists min_spec;")
    cur.execute('''create table min_spec (id serial, 
        os varchar(1000), 
        processor varchar(1000), 
        memory varchar(1000),
        graphics varchar(1000),
        storage varchar(1000)
        );''')
    conn.commit()
    print('최소사양 테이블 생성 success')
except:
    conn.rollback()
    print("--- 최소사양 테이블 생성 실패!")
    exit()

###권장사양 TABLE 생성###

try:
    cur.execute("drop table if exists good_spec;")
    cur.execute('''create table good_spec (id serial, 
        os varchar(1000), 
        processor varchar(1000), 
        memory varchar(1000),
        graphics varchar(1000),
        storage varchar(1000)
        );''')
    conn.commit()
    print('권장사양 테이블 생성 success')
except:
    conn.rollback()
    print("--- 권장사양 테이블 생성 실패!")
    exit()


# DB 함수 정의===========================================================

### 기본정보 values 입력 함수 ###

def insert_into_basic_info(release_date, price, evaluation, evaluation_cnt, evaluation_pct, title, genre, developer,
                           publisher):
    cur.execute("insert into basic_info (release_date, price, evaluation, evaluation_cnt, evaluation_pct, title, genre, developer, publisher) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (release_date, price, evaluation, evaluation_cnt, evaluation_pct, title, genre, developer, publisher))


### 특징 values 입력 함수 ###

def insert_into_feature(Single, Multi, Online_PvP, LAN_PvP, Shared_Split_Screen_PvP, Online_Coop, LAN_Coop,
                        Shared_Split_Screen_Coop, Cross_Platform_Multiplayer, lang):
    cur.execute('''insert into feature (single, multi, online_pvp, lan_pvp,
    shared_split_screen_pvp, online_coop, lan_coop, shared_split_screen_coop,
    cross_platform_multiplayer, language)
    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (Single, Multi, Online_PvP, LAN_PvP, Shared_Split_Screen_PvP, Online_Coop, LAN_Coop, Shared_Split_Screen_Coop, Cross_Platform_Multiplayer, lang))


### 최소사양 values 입력 함수 ###

def insert_into_min_spec(os, processor, memory, graphics, storage):
    cur.execute("insert into min_spec (os, processor, memory, graphics, storage) values(%s, %s, %s, %s, %s)", (os, processor, memory, graphics, storage))


### 권장사양 values 입력 함수 ###

def insert_into_good_spec(os, processor, memory, graphics, storage):
    cur.execute("insert into good_spec (os, processor, memory, graphics, storage) values(%s, %s, %s, %s, %s)", (os, processor, memory, graphics, storage))


#=================================================================================

driver = webdriver.Chrome()
driver.maximize_window()  # 브라우저 화면 최대화
driver.get('https://store.steampowered.com/search/?filter=topsellers&os=win')
time.sleep(1)
SCROLL_PAUSE_SEC = 1

# 스크롤 높이 가져옴
last_height = driver.execute_script("return document.body.scrollHeight")

while True:
    # 끝까지 스크롤 다운
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # 1초 대기
    time.sleep(SCROLL_PAUSE_SEC)

    # 스크롤 다운 후 스크롤 높이 다시 가져옴
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height

#==============================================================================

tbody = driver.find_element(By.ID, 'search_resultsRows')  # 챠트 리스트
rows = tbody.find_elements(By.TAG_NAME, 'a')
for row in rows:
    release_date = None  # 출시일
    price = None  # 가격
    evaluation = None  # 총 평가
    evaluation_cnt = None  # 세부평가
    evaluation_pct = None
    title = None  # 타이틀
    genre = None  # 장르
    developer = None  # 개발사
    publisher = None  # 배급사
    Single = False  # 싱글 플레이
    Multi = False  # 멀티 플레이
    Online_PvP = False  # 온라인 PvP
    LAN_PvP = False  # LAN PvP
    Shared_Split_Screen_PvP = False  # 스크린 공유 및 분할 PvP
    Online_Coop = False  # 온라인 협동
    LAN_Coop = False  # LAN 협동
    Shared_Split_Screen_Coop = False  # 스크린 공유 및 분할 협동
    Cross_Platform_Multiplayer = False  # 플롯폼간 멀티플레이어
    lang = None  # 언어
    req_min_os = None  # 최소 요구 os
    req_min_process = None  # 최소 요구 프로세서
    req_min_memory = None  # 최소 요구 메모리
    req_min_graphiccard = None  # 최소 요구 그래픽
    req_min_storage = None  # 최소 요구 저장 공간
    req_rec_os = None  # 권장 os
    req_rec_process = None  # 권장 프로세서
    req_rec_memory = None  # 권장 메모리
    req_rec_graphiccard = None  # 권장 그래픽
    req_rec_storage = None  # 권장 저장 공간

    # EA Play 예외처리
    if 'EA_Play' in row.get_attribute('href'):
        continue

    # 출시일
    temp = row.find_element(By.CLASS_NAME, 'col.search_released.responsive_secondrow')
    if temp == []:
        pass
    else:
        release_date = temp.text
    # 가격
    price = row.find_element(By.CLASS_NAME, 'col.search_price_discount_combined.responsive_secondrow').text
    if '\n' in price:
        price = int(price.split('\n')[2].replace(',', '')[2:])
    elif price != '':
        price = int(price.replace(',', '')[2:])

    # 총 평가
    if row.find_elements(By.CLASS_NAME, 'search_review_summary') == []:
        pass
    else:
        review_point = row.find_element(By.CLASS_NAME, 'search_review_summary').get_attribute(
            'data-tooltip-html').split('<br>')
        evaluation = review_point[0]
        a = review_point[1].split()
        evaluation_cnt = a[3]
        evaluation_pct = a[0]

    # 새로운 url에서 진행되는 부분
    url = row.get_attribute('href')
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')
    body = soup.select_one('body')
    tbody = soup.select_one('#tabletGrid > div.page_content_ctn')

    # ===========================================================
    if tbody == None:  # 나이 확인창
        driver1 = webdriver.Chrome()
        driver1.maximize_window()  # 브라우저 화면 최대화
        driver1.get(url)
        time.sleep(1)

        abc = driver1.find_element(By.ID, 'ageYear')
        abc.find_elements(By.TAG_NAME, 'option')[40].click()
        driver1.find_element(By.CLASS_NAME, 'btnv6_blue_hoverfade.btn_medium').click()
        time.sleep(1)

        body = driver1.find_element(By.TAG_NAME, 'body')

        if 'app' in body.get_attribute('class'):  # 패키지가 아닌 상품
            # 타이틀
            tbody = driver1.find_element(By.CLASS_NAME, 'page_content_ctn')
            title = tbody.find_element(By.ID, 'appHubAppName').text

            # 장르
            rightcol = tbody.find_element(By.CLASS_NAME, 'rightcol.game_meta_data')
            genr = rightcol.find_element(By.ID, 'genresAndManufacturer')
            genre = genr.find_element(By.TAG_NAME, 'span').text

            # 개발사 배급사
            developer = genr.find_elements(By.TAG_NAME, 'div')[0].text
            publisher = genr.find_elements(By.TAG_NAME, 'div')[1].text

            # 특징
            features_list = rightcol.find_element(By.CLASS_NAME, 'game_area_features_list_ctn').text

            if 'Single' in features_list:
                Single = True

            if 'Multi' in features_list:
                Multi = True

            if 'Online PvP' in features_list:
                Online_PvP = True

            if 'LAN PvP' in features_list:
                LAN_PvP = True

            if 'Shared/Split Screen PvP' in features_list:
                Shared_Split_Screen_PvP = True

            if 'Online Co-op' in features_list:
                Online_Coop = True

            if 'LAN Co-op' in features_list:
                LAN_Coop = True

            if 'Shared/Split Screen Co-op' in features_list:
                Shared_Split_Screen_Coop = True

            if 'Cross-Platform Multiplayer' in features_list:
                Cross_Platform_Multiplayer = True

            # 사양
            sys_req_tb = tbody.find_element(By.CLASS_NAME, 'game_area_sys_req')
            sys_req = sys_req_tb.find_elements(By.TAG_NAME, 'li')
            n = int(len(sys_req) / 2)
            for i in range(n):
                if 'OS:' in sys_req[i].text:
                    req_min_os = sys_req[i].text.replace('OS: ', '')
                if 'Processor:' in sys_req[i].text:
                    req_min_process = sys_req[i].text.replace('Processor: ', '')
                if 'Memory:' in sys_req[i].text:
                    req_min_memory = sys_req[i].text.replace('Memory: ', '')
                if 'Graphics:' in sys_req[i].text:
                    req_min_graphiccard = sys_req[i].text.replace('Graphics: ', '')
                if 'Storage:' in sys_req[i].text:
                    req_min_storage = sys_req[i].text.replace('Storage: ', '')
            for i in range(n, 2 * n):
                if 'OS:' in sys_req[i].text:
                    req_rec_os = sys_req[i].text.replace('OS: ', '')
                if 'Processor:' in sys_req[i].text:
                    req_rec_process = sys_req[i].text.replace('Processor: ', '')
                if 'Memory:' in sys_req[i].text:
                    req_rec_memory = sys_req[i].text.replace('Memory: ', '')
                if 'Graphics:' in sys_req[i].text:
                    req_rec_graphiccard = sys_req[i].text.replace('Graphics: ', '')
                if 'Storage:' in sys_req[i].text:
                    req_rec_storage = sys_req[i].text.replace('Storage: ', '')

            # 언어
            languages = rightcol.find_element(By.CLASS_NAME, 'game_language_options').find_elements(By.TAG_NAME,
                                                                                                    'tr')
            lang = ''
            for language in languages[1:]:
                lang += language.find_elements(By.TAG_NAME, 'td')[0].get_attribute('innerHTML') \
                            .replace('\r', '').replace('\n', '').replace('\t', '') + ','

            driver1.close()
        # ======================================================================
        else:  # 패키지인 상품
            # 타이틀
            tbody = driver1.find_element(By.CLASS_NAME, 'page_content_ctn')
            title = tbody.find_element(By.CLASS_NAME, 'pageheader').text

            # 장르
            rightcol = tbody.find_element(By.CLASS_NAME, 'rightcol.game_meta_data')
            genr = rightcol.find_element(By.CLASS_NAME, 'details_block')
            genre = genr.find_element(By.TAG_NAME, 'span').text

            # 개발사 배급사
            developer = genr.find_elements(By.TAG_NAME, 'span')[1].text
            publisher = genr.find_elements(By.TAG_NAME, 'span')[2].text

            # 특징
            features_list = rightcol.find_element(By.CLASS_NAME, 'game_area_details_specs_ctn').text

            if 'Single' in features_list:
                Single = True

            if 'Multi' in features_list:
                Multi = True

            if 'Online PvP' in features_list:
                Online_PvP = True

            if 'LAN PvP' in features_list:
                LAN_PvP = True

            if 'Shared/Split Screen PvP' in features_list:
                Shared_Split_Screen_PvP = True

            if 'Online Co-op' in features_list:
                Online_Coop = True

            if 'LAN Co-op' in features_list:
                LAN_Coop = True

            if 'Shared/Split Screen Co-op' in features_list:
                Shared_Split_Screen_Coop = True

            if 'Cross-Platform Multiplayer' in features_list:
                Cross_Platform_Multiplayer = True

            # 언어
            language = rightcol.find_element(By.CLASS_NAME, 'language_list')
            lan = language.text.replace(
                '\nListed languages may not be available for all games in the package. View the individual games for more details.',
                '').replace('LANGUAGES: ', '')

            driver1.close()
    # ===================================================================================
    else:  # 나이 확인창 X
        if 'app' in body['class']:  # 패키지가 아닌 상품
            # 타이틀
            title = tbody.select_one('#appHubAppName').text

            # 장르
            rightcol = tbody.select_one('div.rightcol.game_meta_data')
            genr = rightcol.select_one('#genresAndManufacturer')
            if genr.find('span') == None:
                pass
            else:
                genre = genr.find('span').text

            # 개발사 배급사
            developer = genr.select('div > a')[0].text
            if len(genr.select('div > a')) > 1:
                publisher = genr.select('div > a')[1].text

            # 특징
            if rightcol.select_one('div.game_area_features_list_ctn') == None:
                pass

            else:
                features_list = rightcol.select_one('div.game_area_features_list_ctn').text
                if 'Single' in features_list:
                    Single = True

                if 'Multi' in features_list:
                    Multi = True

                if 'Online PvP' in features_list:
                    Online_PvP = True

                if 'LAN PvP' in features_list:
                    LAN_PvP = True

                if 'Shared/Split Screen PvP' in features_list:
                    Shared_Split_Screen_PvP = True

                if 'Online Co-op' in features_list:
                    Online_Coop = True

                if 'LAN Co-op' in features_list:
                    LAN_Coop = True

                if 'Shared/Split Screen Co-op' in features_list:
                    Shared_Split_Screen_Coop = True

                if 'Cross-Platform Multiplayer' in features_list:
                    Cross_Platform_Multiplayer = True

            # 사양
            sys_req_tb = tbody.select_one('div.game_area_sys_req')

            if sys_req_tb == None:
                pass
            elif 'Supported Video Cards' in sys_req_tb.text:
                for i in sys_req_tb.select_one('div > ul > ul').text.replace('\t', '').split('\n')[1:7]:
                    if 'OS:' in i:
                        req_min_os = i.replace('OS: ', '')
                    if 'Processor:' in i:
                        req_min_process = i.replace('Processor: ', '')
                    if 'Memory:' in i:
                        req_min_memory = i.replace('Memory: ', '')
                    if 'Graphics:' in i:
                        req_min_graphiccard = i.replace('Graphics: ', '')
                    if 'Hard Drive:' in i:
                        req_min_storage = i.replace('Hard Drive: ', '')
            elif len(sys_req_tb.select('div')) == 2:
                sys_req = sys_req_tb.find_all('li')
                n = len(sys_req)
                for i in range(n):
                    if 'OS:' in sys_req[i].text:
                        req_min_os = sys_req[i].text.replace('OS: ', '')
                        req_rec_os = req_min_os
                    if 'Processor:' in sys_req[i].text:
                        req_min_process = sys_req[i].text.replace('Processor: ', '')
                        req_rec_process = req_min_process
                    if 'Memory:' in sys_req[i].text:
                        req_min_memory = sys_req[i].text.replace('Memory: ', '')
                        req_rec_memory = req_min_memory
                    if 'Graphics:' in sys_req[i].text:
                        req_min_graphiccard = sys_req[i].text.replace('Graphics: ', '')
                        req_rec_graphiccard = req_rec_graphiccard
                    if 'Storage:' in sys_req[i].text:
                        req_min_storage = sys_req[i].text.replace('Storage: ', '')
                        req_rec_storage = req_min_storage
            elif len(sys_req_tb.select('div')) == 3:
                sys_req = sys_req_tb.select('div')[0].find_all('li')
                for i in sys_req:
                    if 'OS:' in i.text:
                        req_min_os = i.text.replace('OS: ', '')
                    if 'Processor:' in i.text:
                        req_min_process = i.text.replace('Processor: ', '')
                    if 'Memory:' in i.text:
                        req_min_memory = i.text.replace('Memory: ', '')
                    if 'Graphics:' in i.text:
                        req_min_graphiccard = i.text.replace('Graphics: ', '')
                    if 'Storage:' in i.text:
                        req_min_storage = i.text.replace('Storage: ', '')
                sys_req = sys_req_tb.select('div')[1].find_all('li')
                for i in sys_req:
                    if 'OS:' in i.text:
                        req_rec_os = i.text.replace('OS: ', '')
                    if 'Processor:' in i.text:
                        req_rec_process = i.text.replace('Processor: ', '')
                    if 'Memory:' in i.text:
                        req_rec_memory = i.text.replace('Memory: ', '')
                    if 'Graphics:' in i.text:
                        req_rec_graphiccard = i.text.replace('Graphics: ', '')
                    if 'Storage:' in i.text:
                        req_rec_storage = i.text.replace('Storage: ', '')

            # 언어
            if rightcol.select_one('table.game_language_options') == None:
                pass
            else:
                lang = ''
                languages = rightcol.select_one('table.game_language_options').find_all('tr')
                for language in languages[1:]:
                    lang += language.find_all('td')[0].text.replace('\r', '').replace('\n', '').replace('\t',
                                                                                                        '') + ','
        # ==========================================================================================
        else:  # 패키지인 상품
            # 타이틀
            title = tbody.select_one('h2.pageheader').text

            # 장르
            rightcol = tbody.select_one('div.rightcol.game_meta_data')
            genr = rightcol.select('span')
            genre = genr[0].text

            # 개발사 배급사
            developer = genr[1].text
            publisher = genr[2].text

            # 특징
            features_list = rightcol.select_one('div.details_block.details_specs_ctn')
            if features_list == None:
                features_list = rightcol.select('div.details_block')[1].text
            else:
                features_list = features_list.text

            if 'Single' in features_list:
                Single = True

            if 'Multi' in features_list:
                Multi = True

            if 'Online PvP' in features_list:
                Online_PvP = True

            if 'LAN PvP' in features_list:
                LAN_PvP = True

            if 'Shared/Split Screen PvP' in features_list:
                Shared_Split_Screen_PvP = True

            if 'Online Co-op' in features_list:
                Online_Coop = True

            if 'LAN Co-op' in features_list:
                LAN_Coop = True

            if 'Shared/Split Screen Co-op' in features_list:
                Shared_Split_Screen_Coop = True

            if 'Cross-Platform Multiplayer' in features_list:
                Cross_Platform_Multiplayer = True

            # 언어
            language = rightcol.select_one('span.language_list')
            lang = language.text.replace(
                'Listed languages may not be available for all games in the package. View the individual games for more details.',
                '').replace('LANGUAGES: ', '').replace('\n', '').replace('\t', '').replace('Languages: ', '')

    try:
        ### 기본정보에 값 입력 함수 test ###
        insert_into_basic_info(release_date, price, evaluation, evaluation_cnt, evaluation_pct, title, genre, developer,
                               publisher)

        ### 특징에 값 입력 함수 test ###
        insert_into_feature(Single, Multi, Online_PvP, LAN_PvP, Shared_Split_Screen_PvP, Online_Coop, LAN_Coop,
                            Shared_Split_Screen_Coop, Cross_Platform_Multiplayer, lang)

        ### 최소사양에 값 입력 함수 test ###
        insert_into_min_spec(req_min_os, req_min_process, req_min_memory, req_min_graphiccard, req_min_storage)

        ### 권장사양에 값 입력 함수 test ###
        insert_into_good_spec(req_rec_os, req_rec_process, req_rec_memory, req_rec_graphiccard, req_rec_storage)

        # 정상적인 부분이라면 DB에 입력
        conn.commit()
        print("--- db 입력 성공")
    except:
        # 잘못 구성되어 있는 더미 부분을 롤백
        conn.rollback()
        print("--- db 입력 실패")