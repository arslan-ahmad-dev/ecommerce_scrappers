import re
import os
import sys
import json
import random
import scrapy
import traceback
from price_parser import Price
from scrapy.crawler import CrawlerProcess
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from helpers_method.utils import (
    TextCleaner, UserAgentManager, OutputHandler, IndexGenerator, HashInitialzer)
text_cleaner = TextCleaner()
output_handler = OutputHandler()
index_generator = IndexGenerator()

class wood_craft(scrapy.Spider):
    name = "wood_craft"
    base_url = "https://www.zzounds.com/"
    count = 0
    custom_settings = {
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_ITEMS": 128,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 20,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 505, 522, 524, 408, 429, 403, 405, 455]
    }
    filenum = 1

    def start_requests(self):
        self.skus = []
        yield scrapy.Request(
            "https://www.woodcraft.com/sitemap.xml",
            callback = self.fetch_categories,
            headers=self.set_headers()
        )

    def set_headers(self):
        return {
            'user-agent' : random.choice(UserAgentManager.get_user_agents(parent_dir))
        }
    
    def fetch_categories(self,response):
        try:
            product_pages = re.findall(r'<loc>(.*?)</loc>', str(response.body))
            for product_page in product_pages:
                if "sitemap_products_" in product_page:
                    yield scrapy.Request(
                        product_page,
                        callback = self.product_on_page,
                        headers = self.set_headers(),
                    )
        except Exception as e:
            print(f"Issue in grab method: {str(e)}")
            traceback.print_exc()

    def product_on_page(self, response):
        try:
            products = re.findall(r'<loc>(.*?)</loc>', str(response.body))
            for product in products:
                if "/products/" in product:
                    yield scrapy.Request(
                        product,
                        callback = self.fetch_product_details,
                        headers = self.set_headers(),
                    )
        except Exception as e:
            print(f"Issue in product on page: {str(e)}")

    def fetch_product_details(self, response):
        try:
            title = response.xpath("//h1[@id='product__title']/text()").extract()
            title = text_cleaner.clean_text(title[0]) if title else ''

            brand = re.findall(r'vendor":"(.*?)"', str(response.body))
            brand = text_cleaner.clean_text(brand[0]) if brand else ''

            mpn = response.xpath('//span[@class = "caption-with-letter-spacing product__item-model"]/text()').extract()
            mpn = text_cleaner.clean_text(mpn[0].replace("Model","")) if mpn else ''\

            variants = response.xpath('//script[@id = "additionalVariantData"]/text()').extract()
            variants = json.loads(variants[0])

            varis = re.findall(r'"productVariants"\:(\[.*?\])}', str(response.body))
            varis = json.loads(varis[0].replace('\\\\', '\\').replace('\\/', '/').replace('\\\'', '\''))

            images_list = []
            images  = response.xpath('//li[contains(@id, "Slide-template--")]//div[@class = "product__media media media--transparent"]/img/@src').extract()
            for img in images:
                if "?" in img:
                    img = (img.split("?"))[0]

                image = 'https:' + img if 'https' not in img else img
                images_list.append(image)

            for variant in variants:
                if "options" in variants[variant]:
                    details = HashInitialzer.intialize_hash()
                    
                    details['title'] = title
                    details['brand'] = brand
                    
                    details['mpn'] = mpn
                    details['sku'] = variants[variant]['sku']
                    
                    details['hasVariations'] = True if len(varis)>1 else False
                    details['description'] = text_cleaner.clean_text(variants[variant]['description'])
                    
                    details['availability'] = True if 'true' in variants[variant]['available'] else False
                    details['barcode_type'] = ''
                    
                    details['url'] = "https://www.woodcraft.com" + variants[variant]['url'] if 'https' not in variants[variant]['url'] else variants[variant]['url']
                    details['price'] = self.parse_price(variants[variant]['price'])
                    details['images'] = images_list
                    
                    for option in variants[variant]['options']:
                        
                        if 'color' in option.lower():
                            details['color'] = variants[variant]['options'][option]
                        
                        elif 'size' in option.lower():
                            details['color'] = variants[variant]['options'][option]
                        
                        elif 'Title' not in option:
                            details['title'] = details['title'] + f' ({variants[variant]["options"][option]})'

                        barcodes = response.xpath('//variant-radios/script[@type="application/json"]/text()').extract()
                        barcodes = json.loads(barcodes[0]) if barcodes else []
                        
                        for barcode in barcodes:
                            if str(barcode['id']) == variant:
                                details['barcode'], details['barcode_type'] = self.fetch_barcode_type(barcode['barcode'])
                                details ['image'] = 'https:' + barcode['featured_image']['src']

                    if not details['image']:
                        details['image'] = images_list[0]

                    if details['sku'] not in self.skus:
                        output_handler.send_data_to_output_channel(details, "zulily")
                        self.count += 1
                        print(details)
                        print(f"************* COUNT: {self.count}, ITEM ID: {details['sku']}, URL: {details['url']} ******")

        except Exception as e:
            print(f"Issue in fetch details: {str(e)}")

    def parse_price(self, price):
        if price:
            return Price.fromstring(str(price)).amount_float
        else:
            return None
    
    def fetch_barcode_type(barcode):
        if len(barcode) == 12:
            barcode = barcode
            barcode_type = "UPC"
        elif len(barcode) == 13:
            barcode = barcode
            barcode_type = "EAN"
        elif len(barcode) == 14:
            barcode = barcode
            barcode_type = "GTIN"
        else:
            barcode = ""
            barcode_type = ""
        return barcode, barcode_type
    

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(wood_craft)
    process.start()
