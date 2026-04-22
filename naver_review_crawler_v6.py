"""
=============================================================================
[ 네이버 지도 방문자 리뷰 크롤러 (GitHub Actions 클라우드 전용 완성본) ]

- 목적: 특정 타겟(상호명+주소 일치)의 방문자 리뷰 10개를 수집하고, 3개월 이내 리뷰만 정제합니다.
- 아키텍처 (클라우드 생존 룰 적용):
  1. (대규모 스케일링) 1회 실행 시 총 200개의 검색어를 순회 처리.
  2. (메모리 환기) 20개 검색어를 처리할 때마다 브라우저를 껐다 켜서 메모리를 초기화.
  3. (자동 백업) 20개 완료 시점 및 최종 종료 시점에 중간 데이터를 계속 깃허브에 Commit & Push.
  4. (데이터 누적) 기존 수집 데이터가 날아가지 않도록 계속 Append(이어쓰기) 유지.
=============================================================================
"""

import os
import sys
import subprocess
import urllib.parse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager 
from pathlib import Path
import time
import random
import logging
import re
import json
import threading
from datetime import datetime, timedelta

# ===========================
# ⚙️ 1. Configuration (설정)
# ===========================
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
SEARCH_FILE = "검색어.csv" 
PROGRESS_FILE = "progress_review.json"  
HEADLESS_MODE = True             
MAX_RETRY_ATTEMPTS = 3
MAX_REVIEWS_TO_SCRAPE = 10

# 💡 [스케일 업 로직 적용] 카운터 설정
TOTAL_TARGET_PER_RUN = 200       # 1회 실행 시 처리할 총 검색어 개수
RESTART_INTERVAL = 20            # 브라우저 재시작 및 중간 백업 간격

WATCHDOG_TIMEOUT = 60            
CURRENT_DATE = datetime(2026, 4, 8) 
RAW_DATA_FILE = "원본_리뷰_데이터.csv"
FINAL_DATA_FILE = "최종_정제완료_데이터.csv"

# ===========================
# 📝 2. Logging Setup (로그)
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('review_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ===========================
# ⏱️ 3. 생존 로직 (Watchdog & Progress)
# ===========================
class WatchdogTimer:
    def __init__(self, timeout_seconds=60):
        self.timeout = timeout_seconds
        self.last_activity = time.time()
        self.is_active = False
        self.lock = threading.Lock()
        
    def reset(self):
        with self.lock: self.last_activity = time.time()
            
    def start(self):
        self.is_active = True
        self.reset()
        
    def stop(self):
        self.is_active = False
        
    def check_timeout(self):
        if not self.is_active: return False
        with self.lock:
            elapsed = time.time() - self.last_activity
            if elapsed > self.timeout:
                logging.warning(f"⚠️ Watchdog timeout! {elapsed:.1f}초 동안 활동 없음. 강제 재시도 유도.")
                return True
            return False

watchdog = WatchdogTimer(WATCHDOG_TIMEOUT)

def load_progress():
    progress_file = Path(PROGRESS_FILE)
    if progress_file.exists():
        try:
            with open(progress_file, "r", encoding="utf-8") as file: return json.load(file)
        except: return {"current_search": None, "completed_searches": []}
    return {"current_search": None, "completed_searches": []}

def save_progress(search_query):
    try:
        progress = load_progress()
        progress["current_search"] = search_query
        with open(PROGRESS_FILE, "w", encoding="utf-8") as file: json.dump(progress, file, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"진행 상태 저장 실패: {e}")

def mark_search_completed(search_query):
    try:
        progress = load_progress()
        if search_query not in progress["completed_searches"]: 
            progress["completed_searches"].append(search_query)
        progress["current_search"] = None
        with open(PROGRESS_FILE, "w", encoding="utf-8") as file: json.dump(progress, file, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"완료 상태 저장 실패: {e}")

# ===========================
# ☁️ 4. GitHub 자동 Push 함수
# ===========================
def git_commit_and_push(is_intermediate=False):
    try:
        subprocess.run(["git", "config", "--local", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        subprocess.run(["git", "config", "--local", "user.name", "github-actions[bot]"], check=False)
        
        subprocess.run(["git", "add", "*.csv", PROGRESS_FILE], check=True)
        
        status = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if status.returncode != 0:
            msg_type = "중간 백업" if is_intermediate else "최종 저장"
            commit_msg = f"Auto-update: 리뷰 수집 데이터 {msg_type} 완료"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "pull", "--rebase"], check=False)
            subprocess.run(["git", "push"], check=True)
            logging.info(f"☁️ GitHub 자동 푸시 완료: {msg_type} 성공!")
        else:
            logging.info(f"☁️ GitHub 저장: 변경된 데이터가 없습니다.")
    except Exception as e:
        logging.error(f"❌ GitHub 푸시 에러: {e}")

# ===========================
# 🚀 5. WebDriver 최적화
# ===========================
def initialize_driver():
    logging.info("🚀 ChromeDriver 초기화 및 리소스 최적화 적용 중...")
    watchdog.reset()
    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko_KR")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_argument("--log-level=3") 
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    if HEADLESS_MODE: opts.add_argument("--headless=new")
    
    prefs = {
        "profile.managed_default_content_settings.images": 2, 
        "profile.managed_default_content_settings.fonts": 2
    }
    opts.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=opts, service=Service(ChromeDriverManager().install()))
    driver.set_window_size(1440, 900)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver

# ===========================
# 🛠️ 6. 데코레이터 및 유틸리티
# ===========================
def is_match_place(target_name, target_addr, crawled_name, crawled_addr):
    t_name = str(target_name).replace(" ", "")
    c_name = str(crawled_name).replace(" ", "")
    name_match = (t_name in c_name) or (c_name in t_name)
    addr_parts = str(target_addr).split()
    addr_match = all(part in str(crawled_addr) for part in addr_parts)
    return name_match and addr_match

def save_raw_data(data):
    watchdog.reset()
    df = pd.DataFrame(data)
    if not os.path.isfile(RAW_DATA_FILE):
        df.to_csv(RAW_DATA_FILE, index=False, encoding="utf-8-sig")
        logging.info(f"📁 파일 {RAW_DATA_FILE} 생성 및 저장 완료 ({len(data)}건)")
    else:
        df.to_csv(RAW_DATA_FILE, mode='a', header=False, index=False, encoding="utf-8-sig")
        logging.info(f"📁 파일 {RAW_DATA_FILE}에 데이터 누적(Append) 완료 ({len(data)}건)")

def switch_to_frame(driver, frame_id):
    WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, frame_id)))
    time.sleep(1.5)

def get_no_match_data(search_query, place_name="장소 찾기 실패"):
    return [{
        "Search_Query": search_query, "Place_Name": place_name,
        "Nickname": "해당 없음", "Date": "해당 없음", "Review_Content": "해당 없음"
    }]

def safe_extract(default_value):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                if result is None or result == "": raise Exception("Empty Result")
                return result
            except Exception: return default_value
        return wrapper
    return decorator

# ===========================
# 🧩 7. 데이터 추출 헬퍼
# ===========================
@safe_extract("알 수 없음")
def get_nickname(review):
    try: return review.find_element(By.CSS_SELECTOR, "span.pui__NMi-Dp").text
    except: return review.find_element(By.CSS_SELECTOR, "span[class*='NMi-Dp'], span[class*='bgYdt']").text

@safe_extract("날짜 알 수 없음")
def get_date(review):
    date_candidates = review.find_elements(By.XPATH, ".//time | .//span[contains(@class, 'blind')] | .//*[contains(text(), '.')]")
    for t in date_candidates:
        txt = t.get_attribute("textContent").strip()
        match = re.search(r'(\d{2,4}\.)?\d{1,2}\.\d{1,2}\.[월화수목금토일]', txt)
        if match: return match.group(0)
    raise ValueError("날짜 패턴 없음")

@safe_extract("내용 없음")
def get_content(review, driver):
    try:
        text_more_btn = review.find_element(By.XPATH, ".//a[contains(text(), '더보기')]")
        driver.execute_script("arguments[0].click();", text_more_btn)
        time.sleep(0.5)
    except: pass
    
    try: return review.find_element(By.CSS_SELECTOR, "a[data-pui-click-code='rvshowless']").text.strip()
    except: return review.find_element(By.CSS_SELECTOR, "a[data-pui-click-code='rvshowmore']").text.replace("더보기", "").strip()

# ===========================
# 🎯 8. 리뷰 수집 로직
# ===========================
def extract_reviews_unconditionally(driver, search_query, place_name):
    reviews_data = []
    try:
        try:
            review_tab = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[@role='tab' and contains(., '리뷰')] | //span[contains(text(), '리뷰')]/ancestor::a[@role='tab']")))
            driver.execute_script("arguments[0].click();", review_tab)
            time.sleep(3) 
        except Exception:
            logging.warning(f"  [{place_name}] 리뷰 탭을 찾을 수 없습니다.")
            return get_no_match_data(search_query, place_name)
            
        try:
            sort_btn = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[@role='option' and contains(., '최신순')]")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", sort_btn)
            time.sleep(1)
            if sort_btn.get_attribute("aria-selected") != "true":
                driver.execute_script("arguments[0].click();", sort_btn)
                time.sleep(2)
        except: pass

        try:
            more_list_btn = driver.find_elements(By.XPATH, "//a[contains(@class, 'fvwqf') or contains(., '펼쳐서 더보기')]")
            if more_list_btn:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_list_btn[0])
                time.sleep(1)
                driver.execute_script("arguments[0].click();", more_list_btn[0])
                time.sleep(2)
        except: pass

        processed_count = 0
        scroll_attempts = 0
        
        while processed_count < MAX_REVIEWS_TO_SCRAPE and scroll_attempts < 10:
            if watchdog.check_timeout(): raise TimeoutException("Watchdog Timeout Triggered")
            watchdog.reset()
            
            review_blocks = driver.find_elements(By.XPATH, "//li[.//span[contains(@class, 'NMi-Dp')] or .//a[@data-pui-click-code='rvshowmore']]")
            
            if not review_blocks or processed_count >= len(review_blocks):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                scroll_attempts += 1
                review_blocks = driver.find_elements(By.XPATH, "//li[.//span[contains(@class, 'NMi-Dp')] or .//a[@data-pui-click-code='rvshowmore']]")
                if not review_blocks or processed_count >= len(review_blocks): break 
            
            review = review_blocks[processed_count]
            processed_count += 1
            
            nickname = get_nickname(review)
            if nickname == "알 수 없음": continue

            date_text = get_date(review)
            if date_text == "날짜 알 수 없음": continue 

            content = get_content(review, driver)
            
            reviews_data.append({
                "Search_Query": search_query, "Place_Name": place_name,
                "Nickname": nickname, "Date": date_text, "Review_Content": content.replace("\n", " ")
            })
            logging.info(f"    - [수집 진행] {nickname} | {date_text}")

    except Exception as e:
        logging.error(f"  ❌ 리뷰 추출 중 오류 발생: {e}")
        
    if not reviews_data: return get_no_match_data(search_query, place_name)
    return reviews_data

# ===========================
# 🔄 9. 메인 크롤링 순회 로직
# ===========================
def crawl_search_query(driver, search_query, target_address):
    watchdog.start()
    try:
        driver.get("https://map.naver.com/v5/search")
        box = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.input_box>input.input_search")))
        box.clear()
        box.send_keys(search_query)
        box.send_keys(Keys.ENTER)
        time.sleep(random.uniform(4, 6))
        
        switch_to_frame(driver, "searchIframe")
        
        try: litags = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//*[@id='_pcmap_list_scroll_container']//li")))
        except TimeoutException: litags = []

        logging.info(f"🪧 '{search_query}' 검색 결과 순회 시작 - 총 {len(litags)}개 항목 발견")
        place_found = False

        for idx, litag in enumerate(litags, 1):
            if watchdog.check_timeout(): raise TimeoutException("Watchdog Timeout Triggered")
            
            try:
                time.sleep(0.5) 
                if "제주" not in litag.text: continue

                try: click_target = litag.find_element(By.CSS_SELECTOR, "span.TYaxT, span.place_bluelink, div.place_bluelink")
                except: click_target = litag.find_element(By.XPATH, ".//a[not(.//img)] | .//div[contains(@class, 'title') or contains(@class, 'name')]")
                
                driver.execute_script("arguments[0].click();", click_target)
                time.sleep(random.uniform(2, 3))
                
                driver.switch_to.default_content()
                switch_to_frame(driver, "entryIframe")
                
                try: address = driver.find_element(By.CSS_SELECTOR, "span.pz7wy, span.LDgIH, span.IH7vq").text 
                except: 
                    try: address = driver.find_element(By.XPATH, "//span[contains(text(), '제주')]").text
                    except: address = "주소 없음"
                
                try: place_name = driver.find_element(By.CSS_SELECTOR, "span.GHAhO, span.Fc1rA, h1").text
                except: place_name = "이름 없음"

                if is_match_place(search_query, target_address, place_name, address):
                    logging.info(f"  📍 [일치 장소 발견!] [{place_name}] ({address})")
                    place_found = True
                    collected_reviews = extract_reviews_unconditionally(driver, search_query, place_name)
                    save_raw_data(collected_reviews)
                    return True 
                else:
                    logging.info(f"  ⏭️ [주소 불일치] 다음 항목을 확인합니다: [{place_name}]")
                    
            except Exception: pass
            
            try:
                driver.switch_to.default_content()
                switch_to_frame(driver, "searchIframe")
                time.sleep(random.uniform(1, 2))
            except: pass
                
        if not place_found:
            logging.warning(f"  ⚠️ '{search_query}'(주소: {target_address})와 일치하는 장소를 찾지 못했습니다.")
            save_raw_data(get_no_match_data(search_query))
            return True
            
    except Exception as e:
        logging.error(f"  ❌ 크롤링 중 예외 발생: {e}")
        return False
    finally:
        watchdog.stop()

# ===========================
# 🧼 10. 후처리 정제 로직
# ===========================
def is_within_3_months(date_str):
    if date_str == "해당 없음" or date_str == "날짜 알 수 없음": return False
    three_months_ago = CURRENT_DATE - timedelta(days=90)
    
    match1 = re.search(r'^(\d{2,4})\.(\d{1,2})\.(\d{1,2})\.[월화수목금토일]', str(date_str).strip())
    if match1:
        y, m, d = match1.groups()
        y = "20" + y if len(y) == 2 else y
        try: return datetime(int(y), int(m), int(d)) >= three_months_ago
        except: pass

    match2 = re.search(r'^(\d{1,2})\.(\d{1,2})\.[월화수목금토일]', str(date_str).strip())
    if match2:
        m, d = match2.groups()
        y = CURRENT_DATE.year
        try:
            review_date = datetime(int(y), int(m), int(d))
            if review_date > CURRENT_DATE: review_date = review_date.replace(year=y-1)
            return review_date >= three_months_ago
        except: pass
    return False

def post_process_data():
    logging.info("="*60)
    logging.info("🧹 수집 완료. 데이터 정제 및 중복 제거를 시작합니다.")
    
    if not os.path.isfile(RAW_DATA_FILE):
        return logging.error(f"❌ '{RAW_DATA_FILE}' 파일이 없습니다.")

    try:
        df = pd.read_csv(RAW_DATA_FILE)
        df = df.drop_duplicates(subset=['Search_Query', 'Place_Name', 'Nickname', 'Review_Content'], keep='first')
        
        initial_count = len(df)
        df['is_valid'] = df['Date'].apply(is_within_3_months)
        df.loc[df['Date'] == '해당 없음', 'is_valid'] = True
        
        final_df = df[df['is_valid']].drop(columns=['is_valid'])
        final_df.to_csv(FINAL_DATA_FILE, index=False, encoding='utf-8-sig')
        
        logging.info(f"✅ 정제 완료! (총 {initial_count}건 중 {len(final_df)}건 최종 저장)")
    except Exception as e: logging.error(f"❌ 정제 중 오류: {e}")

# ===========================
# 🏁 11. Main
# ===========================
def main():
    driver = None
    try:
        try: 
            df = pd.read_csv(SEARCH_FILE)
            search_list = df[['검색어', '주소']].to_dict('records')
        except Exception as e: 
            return logging.error(f"❌ {SEARCH_FILE} 파일을 열 수 없습니다: {e}")

        progress = load_progress()
        completed_searches = set(progress.get("completed_searches", []))
        
        logging.info(f"📋 총 {len(search_list)}개 타겟 로드 완료 (현재 {len(completed_searches)}개 완료됨)")
        driver = initialize_driver()
        
        # 💡 [핵심] 듀얼 카운터 시스템 (200개 종료 카운터 & 20개 재시작 카운터)
        processed_count = 0 
        interval_count = 0
        
        for item in search_list:
            search_query = item['검색어']
            target_address = item['주소']
            
            # 이미 완료된 검색어는 건너뜀
            if search_query in completed_searches:
                continue
            
            # 💡 [종료 룰] 1회 실행 총 목표치(200개) 달성 시 루프 탈출
            if processed_count >= TOTAL_TARGET_PER_RUN:
                logging.info(f"🛑 1회 실행 목표({TOTAL_TARGET_PER_RUN}개)를 모두 달성했습니다. 크롤러를 안전하게 종료합니다.")
                break

            # 💡 [메모리 환기 룰] 20개마다 브라우저 껐다 켜기 및 중간 저장
            if interval_count >= RESTART_INTERVAL:
                logging.info(f"🧹 메모리 최적화: {RESTART_INTERVAL}개 처리 완료. 브라우저를 재시작하고 데이터를 중간 백업합니다.")
                if driver:
                    try: driver.quit()
                    except: pass
                
                # 정제 및 깃허브 중간 백업
                post_process_data()
                git_commit_and_push(is_intermediate=True)
                
                time.sleep(8)
                driver = initialize_driver()
                interval_count = 0 # 20개 카운터 초기화
                
            save_progress(search_query) 
            
            retry_count = 0
            while retry_count < MAX_RETRY_ATTEMPTS:
                if crawl_search_query(driver, search_query, target_address): 
                    mark_search_completed(search_query)
                    break 
                
                retry_count += 1
                logging.warning(f"⚠️ '{search_query}' 수집 실패. 재시도 중... ({retry_count}/{MAX_RETRY_ATTEMPTS})")
                
                if retry_count < MAX_RETRY_ATTEMPTS:
                    try: driver.quit() 
                    except: pass
                    
                    backoff_time = 5 * (2 ** (retry_count - 1))
                    time.sleep(backoff_time)
                    driver = initialize_driver() 
                else:
                    logging.error(f"❌ '{search_query}' 최대 재시도 횟수 초과. 다음으로 넘어갑니다.")
            
            # 검색어 1개 처리가 완전히 끝났으므로 두 카운터를 나란히 1씩 증가
            processed_count += 1
            interval_count += 1
                    
    finally:
        if driver: 
            try: driver.quit()
            except: pass
            
        post_process_data() 
        git_commit_and_push(is_intermediate=False)

if __name__ == "__main__":
    logging.info("="*60)
    logging.info("🚀 네이버 지도 방문자 리뷰 크롤러 (200개/20개 분할 아키텍처)")
    logging.info("="*60)
    main()