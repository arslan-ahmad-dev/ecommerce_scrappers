import os
import sys
import scrapy
import random
import traceback
from scrapy.crawler import CrawlerProcess
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from helpers_method.utils import(
    TextCleaner, OutputHandler, IndexGenerator, UserAgentManager)

text_cleaner = TextCleaner()
output_handler = OutputHandler()
index_generator = IndexGenerator()

class bachon_ki_dunya(scrapy.Spider):
    name = "bachon_ki_dunya_crawler"
    base_url = "https://bachonkidunya.co/"
    count = 0
    custom_settings = {
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_ITEMS": 128,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 20,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 505, 522, 524, 408, 429, 403, 405, 455]
    }
    output_array = []
    filenum = 1

    def start_requests(self):
      self.categories = []
      yield scrapy.Request(
        self.base_url,
        callback = self.parse, 
        headers = self.set_headers(),
      )

    def set_headers(self):
        return {
            'user-agent': random.choice(UserAgentManager.get_user_agents(parent_dir))
        }
  
    def parse(self, response):
        try:
            categories = response.xpath(
                '//ul[@class="sub-menu color-scheme-dark"]/li/a/@href'
            ).getall()
            categories = list(set(categories))
            
            for category in categories:
                if 'https' not in category:
                    category_url = f'{self.base_url}{category}'
                else:
                    category_url = category                
                
                yield scrapy.Request(
                    category_url, 
                    callback = self.process_products_url,
                    headers = self.set_headers()
                )
        
        except Exception as e:
            print(f'Exception in reading categories: {e}')
            traceback.print_exc()

    def process_products_url(self, response): 
        try:
            product_urls = response.xpath('//h3[@class="product-title"]/a/@href').getall()

            for product_url in product_urls:
                if "https://bachonkidunya.co/" not in product_url:
                    product_url = f"{self.base_url}{product_url}"
                else:
                    product_url = product_url
                
                yield scrapy.Request(
                    product_url, 
                    callback = self.process_products, 
                    headers = self.set_headers(),
                    cb_kwargs=dict({
                        'product_url': product_url})
                )
        
        except Exception as e:
            print("*********** Exception: ", e)

    def process_products(self, response, **kwargs):
        try:
            product_url = str(kwargs['product_url'])

            product_title = response.xpath('//h1[@class="product_title entry-title"]/text()').get()
            product_title = text_cleaner.clean_text(product_title) if product_title else ""

            price = response.xpath('//p[@class="price"]//span[@class="woocommerce-Price-amount amount"]/bdi/text()').get()
            price = float(text_cleaner.clean_text(price).replace(",","")) if price else 0.0

            description = response.xpath('//div[@class="woocommerce-product-details__short-description"]/p/text()').getall()
            description = text_cleaner.clean_text(description[-1]) if description else ""
            
            sku = response.xpath('//span[@class="sku"]/text()').get()
            sku = text_cleaner.clean_text(sku.strip()) if sku else ""
            
            image = response.xpath('//figure[@class="woocommerce-product-gallery__wrapper owl-carousel"]/figure/@data-thumb').getall()
            image = text_cleaner.clean_text(image[0]) if image else ""

            image_urls = response.xpath('//figure[@class="woocommerce-product-gallery__wrapper owl-carousel"]/figure/@data-thumb').getall()
            if image_urls:
                new_url =[]
                for image_url in image_urls:
                    new_image = text_cleaner.clean_text(image_url)
                    new_url.append(new_image)
            else:
                new_url = []

            categories_name = response.xpath('//nav[@class="woocommerce-breadcrumb"]/a/text()').getall()
            main_category = categories_name[-2] if categories_name else ""
            sub_category = categories_name[-1] if categories_name else ""

            product_info = {
                'index' : index_generator.get_index(),        
                'item_id' : sku,          
                'url': product_url,
                'sku': sku,
                'title': product_title,
                'price': price,
                'main_category' : main_category,
                'sub_category' : sub_category,
                'image' : image,
                'image_urls' : new_url
            }

            self.pass_data(product_info) 
                                   
        except Exception as e:
            traceback.print_exc()
            print(e)
     
    def pass_data(self, product_info):
        output_handler.send_data_to_output_channel(product_info, "bachon_ki_dunya")
        self.count += 1
        print(product_info)
        print(f"************* COUNT: {self.count}, ITEM ID: {product_info['item_id']}, URL: {product_info['url']} ******")
   
if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(bachon_ki_dunya)
    process.start()
