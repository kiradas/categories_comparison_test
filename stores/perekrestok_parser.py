import requests
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool
import logging
from retrying import retry

class PerekrestokScraper:
    def __init__(self, num_processes=6):
        self.num_processes = num_processes
        self.links_to_parse = []

    def scrape_data(self):
        self._fetch_category_links()
        with Pool(self.num_processes) as pool:
            result = pool.map(self._process_category, self.links_to_parse)
        resdf = pd.DataFrame([item for sublist in result for item in sublist])
        return resdf

    def _fetch_category_links(self):
        url = 'https://www.perekrestok.ru/cat'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for catg in soup.find_all('div', {'class': "category-filter-item__content"}):
            for cat in catg.find_all('a'):
                self.links_to_parse.append({'cat_title': cat.text, 'cat_url': 'https://www.perekrestok.ru' + cat['href']})

    @retry(wait_fixed=6000, stop_max_attempt_number=2)
    def _get_products_from_category(self, prod_category):
        products_in_cat = []
        url = prod_category['cat_url']
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        page_to_check = 0
        if len(soup.find_all('div', {'class': "notify-content"})) > 0:
            if 'По выбранным фильтрам пока нет товаров' in soup.find_all('div', {'class': "notify-content"})[0].text:
                products_in_cat.append({'cat_title': prod_category['cat_title'], 'cat_url': prod_category['cat_url']})
                page_to_check = 0
        else:
            current_page_links = soup.select('ul.rc-pagination.pagination li a[aria-current="page"]')
            page_texts = [int(link.text) for link in current_page_links if link.get_text().isdigit()]
            if page_texts:
                page_to_check = max(page_texts)
            else:
                page_to_check = 0
            first_page = self._get_products_from_page(soup=soup, cat_title=prod_category['cat_title'], cat_url=prod_category['cat_url'])
            products_in_cat.extend(first_page)
        if page_to_check > 1:
            for page_number in range(2, page_to_check + 1):
                response = requests.get(url + '?page=' + str(page_number))
                if response.status_code == 200:
                    products_in_cat.extend(self._get_products_from_page(soup=BeautifulSoup(response.text, 'html.parser'), cat_title=prod_category['cat_title'], cat_url=prod_category['cat_url']))
                else:
                    print(f"can't get data from the url. Status code: {response.status_code}")
        print(prod_category, 'was scraped')
        return products_in_cat

    def _get_products_from_page(self, soup, cat_title='', cat_url=''):
        prods = []
        for prod in (soup.find_all('div', {'class': "product-card-wrapper"}))[:]:
            prod_url = 'https://www.perekrestok.ru' + prod.find('a', {'class': "product-card__link"})['href']
            for row in (prod.find_all('div', {'class': "product-card__content"})):
                title = row.find('div', {'class': "product-card__title"}).text
                try:
                    product_price = row.find('div', {'class': "product-card__pricing"}).text.replace('\xa0', '')
                except:
                    product_price = None
                try:
                    product_size = row.find('div', {'class': "product-card__size"}).text.replace('\xa0', '')
                except:
                    product_size = None
            prods.append({'title': title, 'product_price': product_price, 'product_size': product_size, 'prod_url': prod_url, 'cat_title': cat_title, 'cat_url': cat_url})
        return prods

    def _process_category(self, prod_category):
        try:
            products_in_cat = self._get_products_from_category(prod_category)
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print("Network Exception:", e)
        except Exception as e:
            print("Other Exception:", e)
        return products_in_cat

if __name__ == '__main__':
    scraper = PerekrestokScraper()
    result_df = scraper.scrape_data()
    result_df.to_parquet('perekrestok.parquet')

