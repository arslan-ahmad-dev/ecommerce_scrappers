import re
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

class hamza_store(scrapy.Spider):
    name = "hamza_store_crawler"
    base_url = "https://www.hamzastore.pk/"
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
            categories = response.xpath('//ul[@class="list-unstyled components pt-lg-3 pt-md-3 pt-sm-0 pt-0"]//div[@class="col-10 my-auto"]/a/@href').getall()
            categories = list(set(categories))
            
            for category in categories:
                if 'https' not in category:
                    category_url = f'{self.base_url}{category}'
                else:
                    category_url = category                
                
                yield scrapy.Request(
                    category_url, 
                    callback = self.process_products_url,
                    headers = self.set_headers(),
                )
        
        except Exception as e:
            print(f'Exception in reading categories: {e}')
            traceback.print_exc()
    
    def process_products_url(self, response):
        try:
            product_urls = response.xpath('//div[@class="product_img_continer"]/a/@href').getall()

            for product_url in product_urls:
                if "https://www.hamzastore.pk/" not in product_url:
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
            
            product_title = response.xpath('//h1[@class="single-product-title text-capitalize mb-1"][@title]/text()').get()
            product_title = text_cleaner.clean_text(product_title) if product_title else None

            current_price = response.xpath('//span[@class="color-gray font-size24 font-weight-bold prices"]/text()').get()
            current_price = float(current_price.replace("Rs.","").replace(",","")) if current_price else None

            regular_price = response.xpath('//span[@class="discount-price color-dark-gray font-weight-normal "]/text()').get()
            regular_price = float(regular_price.replace("Rs.","").replace(",","")) if regular_price else None

            price = current_price if current_price else regular_price

            if current_price and regular_price:
                discount_price = regular_price - current_price
                discount_percentage = (discount_price/regular_price)*100
                discount_percentage = round(discount_percentage , 2)
            else:
                discount, discount_percentage = None, None

            
            description = response.xpath('//div[@class="html-content pdp-product-highlights"]//li/text()').getall()
            description = text_cleaner.clean_text(', '.join(description))

            brand = response.xpath('//span[@class="font-size18"]/text()').get()
            brand = text_cleaner.clean_text(brand) if brand else None

            image = response.xpath('//meta[@property="og:image" ]').get()
            image_url = re.search(r'content="(.*?)"', image).group(1) if image else None
            image = image_url if image_url else None

            image_urls = [image] if image else None
            if image_urls:
                new_url =[]
                for image_url in image_urls:
                    new_image = text_cleaner.clean_text(image_url)
                    new_url.append(new_image)
            else:
                new_url = None

            categories_name = response.xpath('//nav[@aria-label="breadcrumb"]//li/a/text()').getall()
            main_category = categories_name[-2] if categories_name else None
            sub_category = categories_name[-1] if categories_name else None
            
            product_info = { 
                'index' : index_generator.get_index(),               
                'url': product_url,
                'title': product_title,
                'brand' : brand,
                'price':price,
                'discount_percentage' : discount_percentage,
                'main_category' : main_category,
                'sub_category' : sub_category,
                'description' : description,
                'image' : image,
                'image_urls' : new_url,           
            }

            self.pass_data(product_info) 
                                   
        except Exception as e:
            traceback.print_exc()
            print(e)
     
    def pass_data(self,product_info):
        output_handler.send_data_to_output_channel(product_info, "hamza_store")
        self.count += 1
        print(product_info)
        print(f"************* COUNT: {self.count}, ITEM ID: {product_info['index']}, URL: {product_info['url']} ******")
   
if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(hamza_store)
    process.start()
