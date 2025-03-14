import os
import time
import random
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, WebDriverException
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

output_dir = "../data_crawled"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def get_driver():
    # chrome_driver_path = "/usr/lib/chromium-browser/chromedriver"  # Cập nhật đường dẫn này theo hệ điều hành của bạn
    options = webdriver.ChromeOptions()

    # Chạy ở chế độ ẩn danh (Incognito)
    options.add_argument('--incognito')

    # Chạy headless (không giao diện)
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')

    # Giả lập như máy thật
    ua = UserAgent()
    user_agent = ua.random  # Tạo User-Agent ngẫu nhiên
    options.add_argument(f'user-agent={user_agent}')  # Sử dụng user-agent ngẫu nhiên thay vì cố định

    # Tắt các tính năng dễ bị phát hiện
    options.add_argument('--disable-blink-features=AutomationControlled')  # Tắt dấu hiệu automation
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # Tắt thông báo "Chrome is being controlled by automated test software"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Giả lập thông tin trình duyệt
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')

    # Cấu hình đường dẫn ChromeDriver
    options.add_argument(
        "webdriver.chrome.driver=/usr/lib/chromium-browser/chromedriver"
    )

    # service = ChromeService(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(options=options)

    # Thêm script để xóa dấu hiệu WebDriver
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })

    return driver

def wait_and_click(driver, by, value, timeout=10):
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(random.uniform(0.5, 1.5))  # Giả lập thời gian click như người thật
        driver.execute_script("arguments[0].click();", element)
        return True
    except Exception as e:
        print(f"Error clicking element: {e}")
        return False

def extract_overview_info(car_soup):
    overview_section = car_soup.find('div', {'data-test': 'vehicleOverviewSection'})
    overview_info = {
        'Exterior': 'N/A', 'Interior': 'N/A', 'Mileage': 'N/A', 'Fuel Type': 'N/A',
        'MPG': 'N/A', 'Transmission': 'N/A', 'Drivetrain': 'N/A', 'Engine': 'N/A',
        'Location': 'N/A', 'Listed Since': 'N/A', 'VIN': 'N/A', 'Stock Number': 'N/A'
    }

    if overview_section:
        overview_items = overview_section.find_all('div', class_='flex items-center')
        for item in overview_items:
            text = item.get_text(strip=True)
            if 'Exterior:' in text:
                overview_info['Exterior'] = text.replace('Exterior:', '').strip()
            elif 'Interior:' in text:
                overview_info['Interior'] = text.replace('Interior:', '').strip()
            elif 'miles' in text:
                overview_info['Mileage'] = text.strip()
            elif 'Fuel Type:' in text:
                overview_info['Fuel Type'] = text.replace('Fuel Type:', '').strip()
            elif 'city /' in text:
                overview_info['MPG'] = text.strip()
            elif 'Transmission' in text:
                overview_info['Transmission'] = text.strip()
            elif 'Drivetrain:' in text:
                overview_info['Drivetrain'] = text.replace('Drivetrain:', '').strip()
            elif 'engine' in text:
                overview_info['Engine'] = text.strip()
            elif 'Location:' in text:
                overview_info['Location'] = text.replace('Location:', '').strip()
            elif 'Listed' in text:
                overview_info['Listed Since'] = text.strip()
            elif 'VIN:' in text:
                overview_info['VIN'] = text.replace('VIN:', '').strip()
            elif 'Stock Number:' in text:
                overview_info['Stock Number'] = text.replace('Stock Number:', '').strip()
    return overview_info

def save_car_data(output_dir, page_number, index, title, price_cash, finance_price, finance_details, overview_info,
                  feature_list):
    sub_dir = os.path.join(output_dir, str(page_number))
    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)

    file_name = os.path.join(sub_dir, f"{index + 1}.txt")

    file_content = (
            f"Title: {title}\n"
            f"Cash Price: {price_cash}\n"
            f"Finance Price: {finance_price}\n"
            f"Finance Details: {finance_details}\n"
            + "\n".join(f"{key}: {value}" for key, value in overview_info.items()) + "\n"
                                                                                     f"Features: {'; '.join(feature_list)}\n"
    )

    with open(file_name, "w", encoding="utf-8") as file:
        file.write(file_content)

    print(f"Data saved to file {file_name}")

def extract_and_save_car_data(page_number):
    driver = get_driver()
    max_retries = 3
    retry_count = 0

    try:
        while retry_count < max_retries:
            try:
                print(f"Fetching data from page {page_number}...")
                driver.get(f'https://www.truecar.com/used-cars-for-sale/listings/?page={page_number}')

                # Giả lập hành vi người dùng: cuộn trang ngẫu nhiên
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * Math.random());")
                time.sleep(random.uniform(1, 3))

                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-test="cardLinkCover"]'))
                )

                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')

                car_hrefs = [car['href'] for car in soup.find_all("a", {"data-test": "cardLinkCover"})]
                print(f"Found {len(car_hrefs)} hrefs on page {page_number}.")

                for index, href in enumerate(car_hrefs):
                    print(f"Processing href {index + 1}/{len(car_hrefs)}: {href}")
                    driver.get(f"https://www.truecar.com{href}")
                    time.sleep(random.uniform(3, 7))  # Thời gian chờ ngẫu nhiên như người thật

                    # Giả lập cuộn trang
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * Math.random());")
                    time.sleep(random.uniform(0.5, 2))

                    car_html_content = driver.page_source
                    car_soup = BeautifulSoup(car_html_content, 'html.parser')

                    title_element = car_soup.find("div", {"data-test": "marketplaceVdpHeader"})
                    title = title_element.get_text(strip=True) if title_element else "No Title"

                    cash_toggle = driver.find_elements(By.XPATH, '//label[@data-test="vdpPricingBlockCashToggle"]')
                    finance_toggle = driver.find_elements(By.XPATH, '//label[@data-test="vdpPricingBlockLoanToggle"]')

                    if cash_toggle and finance_toggle:
                        if wait_and_click(driver, By.XPATH, '//label[@data-test="vdpPricingBlockCashToggle"]'):
                            time.sleep(random.uniform(0.5, 1.5))
                            car_html_content = driver.page_source
                            car_soup = BeautifulSoup(car_html_content, 'html.parser')

                        price_cash_element = car_soup.select_one('div[data-test="unifiedPricingInfoPrice"]')
                        price_cash = price_cash_element.get_text(strip=True) if price_cash_element else "No Cash Price"

                        if wait_and_click(driver, By.XPATH, '//label[@data-test="vdpPricingBlockLoanToggle"]'):
                            time.sleep(random.uniform(0.5, 1.5))
                            car_html_content = driver.page_source
                            car_soup = BeautifulSoup(car_html_content, 'html.parser')

                        finance_price_element = car_soup.select_one('div[data-test="unifiedPricingInfoPrice"]')
                        finance_price = finance_price_element.get_text(
                            strip=True) if finance_price_element else "No Finance Price"

                        finance_details_element = car_soup.select_one('div[data-test="unifiedPricingInfoDisclaimer"]')
                        finance_details = finance_details_element.get_text(
                            strip=True) if finance_details_element else "No Finance Details"
                    else:
                        price_cash_element = car_soup.select_one('div[data-test="unifiedPricingInfoPrice"]')
                        price_cash = price_cash_element.get_text(strip=True) if price_cash_element else "No Cash Price"
                        finance_price = "Not Available"
                        finance_details = "Not Available"

                    overview_info = extract_overview_info(car_soup)

                    if not wait_and_click(driver, By.XPATH, '//button/span[text()="View more details"]/..'):
                        print(f"Error clicking 'View more details' button for href {href}")

                    feature_list = []
                    if wait_and_click(driver, By.XPATH, '//button/span[text()="See all features"]/..'):
                        car_html_content = driver.page_source
                        car_soup = BeautifulSoup(car_html_content, 'html.parser')
                        features = car_soup.select('div.modal-body ul li')
                        feature_list = [feature.get_text(strip=True) for feature in features]
                    else:
                        print(f"Error clicking 'See all features' button for href {href}")

                    save_car_data(output_dir, page_number, index, title, price_cash, finance_price, finance_details,
                                  overview_info, feature_list)

                break

            except (ConnectionResetError, WebDriverException) as e:
                retry_count += 1
                print(f"Error occurred on page {page_number}. Retrying ({retry_count}/{max_retries}): {str(e)}")
                time.sleep(random.uniform(5, 10))  # Delay lâu hơn khi retry

            except NoSuchWindowException:
                print("Browser window closed unexpectedly. Exiting...")
                return
            except Exception as e:
                print(f"Error processing page {page_number}: {str(e)}")
                break
    finally:
        driver.quit()

total_pages = 1

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(extract_and_save_car_data, page_number) for page_number in range(1, total_pages + 1)]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Error processing a page: {str(e)}")
print("Data collection complete.")