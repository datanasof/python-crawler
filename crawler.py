import logging
import json
from urllib.parse import urljoin
import requests
from datetime import date
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)

main_category_selectors = ["li[class*='Component list-item']>a", ]
all_category_selectors = ["li[class*='Component list-item']>a", "li[class*='list-sub-item']>a",
                          "a[href*=weekly-offers]"]
product_info_selector = "div[class*='product-box'][data-product-id]"


class Crawler:

    def __init__(self, urls=[]):
        self.visited_urls = []
        self.urls_to_visit = urls
        self.products = {'id': [], 'description': [], 'category': [], 'price': [], 'number_of_reviews': [],
                         'stars_from_reviews': []}
        self.current_date = date.today().strftime("%Y-%m-%d")

    def download_url(self, url):
        return requests.get(url).text

    def add_product(self, product):
        if product['id'] not in self.products['id']:
            logging.info(f'Adding product: {product["descr"]} to product_list')
            self.products['id'].append(product['id'])
            self.products['description'].append(product['descr'])
            self.products['category'].append(product['category'])
            self.products['price'].append(product['price'])
            self.products['number_of_reviews'].append(product['nreviews'])
            self.products['stars_from_reviews'].append(product['rating'])

    def process_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        for product_info in soup.select(product_info_selector):
            my_product = None
            try:
                # collect product information
                product = product_info.select("div[hidden='true']")[0]

                id = product.get('data-productid')
                descr = product.get('data-productname')
                category = product.get('data-productcategory')
                price = product.get('data-productprice')

                my_product = {'id': id, 'descr': descr, 'category': category,
                              'price': price, 'nreviews': np.nan, 'rating': np.nan}
            except Exception:
                logging.exception(f'The selected element is not a valid product_element: {product}')

            if my_product is not None:
                # check for/ collect product rating
                if product_info.select("[class='product-rating']"):
                    product_rating = product_info.select("[class='product-rating']")[0]
                    nstars = product_rating.select("[class='on']")[-1].getText()
                    nreviews = ''
                    nreviews_string = product_rating.select("[class='rating-comment']")[0].getText()
                    digit_match = re.match(r'^\W*.*(\d)', nreviews_string)
                    if digit_match:
                        nreviews = digit_match.group(1)

                    my_product['nreviews'] = int(nreviews)
                    my_product['rating'] = int(nstars)

                self.add_product(my_product)

    def get_linked_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for cat_selector in main_category_selectors:
            # using all_category_selectors instead of 'main' will extract all products
            # but it takes more time to process the website

            for link in soup.select(cat_selector):
                path = link.get('href')
                if path and path.startswith('/'):
                    path = urljoin(url, path)
                yield path

    def add_url_to_visit(self, url):
        if url not in self.visited_urls and url not in self.urls_to_visit:
            self.urls_to_visit.append(url)

    def crawl(self, url):
        html = self.download_url(url)
        self.process_html(html)
        for url in self.get_linked_urls(url, html):
            self.add_url_to_visit(url)

    def pd_to_excel(self, file_name):
        # Save the data in excel file

        df = pd.DataFrame(self.products)

        # Dataframe for Sheet1 - sort products by category
        df1 = df.sort_values('category')

        # Dataframe for Sheet2 - sort products by Number of customer reviews
        df2 = df[df.number_of_reviews > 0]
        df2 = df2.sort_values('number_of_reviews', ascending=False)

        with pd.ExcelWriter(file_name) as writer:
            df1.to_excel(writer, columns=['id', 'description', 'category', 'price'],
                         sheet_name=self.current_date, header=True, index=False)
            df2.to_excel(writer, columns=['id', 'description', 'number_of_reviews', 'stars_from_reviews'],
                         sheet_name='most-reviewed-'+self.current_date, header=True, index=False)

    def run(self):
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)
            logging.info(f'Crawling: {url}')
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f'Failed to crawl: {url}')
            finally:
                self.visited_urls.append(url)

        self.pd_to_excel("product_report.xlsx")

        # with open("product_report.json", "w") as outfile:
        #     json.dump(self.products, outfile)

if __name__ == '__main__':
    Crawler(urls=['https://www.technopolis.bg', ]).run()
