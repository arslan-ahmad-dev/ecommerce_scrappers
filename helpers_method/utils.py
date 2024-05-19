import os
import re
import json
import random
from datetime import datetime

class IndexGenerator:
    @staticmethod
    def get_index():
        index = ""
        dt = datetime.now()
        ts = datetime.timestamp(dt)
        for i in range(5):
            randomLowerLetter = chr(random.randint(ord('a'), ord('z')))
            index += randomLowerLetter
        index += str(int(ts))
        return index


class UserAgentManager:
    @staticmethod
    def get_user_agents(parent_dir):
            user_agents_path = os.path.join(parent_dir, 'user_agents.txt')
            with open(user_agents_path, 'r') as f:
                data = f.read()
                user_agents = data.split('\n')
                return user_agents

class TextCleaner:
    @staticmethod
    def clean_text(text):
        if not text:
            return ''
        text = str(text.encode('ascii', 'ignore')).replace("\\'", "'").replace(
            '\\"', '"').replace("\"", '"').replace("\'", "'").replace("&amp;", "&").replace("\n", "").replace(
            "00bd", "").replace("00be", "").replace("00bc", "").replace("0027", "'").replace('0026', "&").replace(
            "0022", '"')
        text_characters = re.findall(r'(\\[a-z])', text)
        for x in text_characters:
            text = text.replace(x, ' ')
        text = re.sub(r'<style>(.*?)</style>', ' ', text)
        CLEANR = re.compile('<.*?>')
        text = re.sub(CLEANR, ' ', text)
        html_char = re.findall(r'(&.*?;)', text)
        for x in html_char:
            text = text.replace(x, ' ')
        if text:
            text = text.replace("  ", " ").replace("\\", " ")
            text = " ".join(text.split())
        text = text.replace("Description", "").replace("c2", "").replace("a0", "")
        text = text[2:]
        text = text[:-1]
        text = text.replace("}", "").replace("003C", "").replace("003E", "").replace("002Fp", "").replace("003E", "").replace("003Cp", "")
        return text.strip()


class OutputHandler:
    def __init__(self):
        self.output_array = []
        self.filenum = 1

    def send_data_to_output_channel(self, product_info, file_name):
        if len(self.output_array) > 5:
            self.output_current(file_name)
        else:
            try:
                self.output_array.append(product_info)
            except Exception as e:
                print(f'Exception in saving output: {e}')
    

    def output_current(self, file_name):
        try:
            if self.output_array:
                data = json.dumps(self.output_array, indent=4)
                parent_directory = os.path.dirname(os.getcwd())
                directory = os.path.join(parent_directory, f"{file_name}_output_files")
                if not os.path.exists(directory):
                    os.makedirs(directory)
                with open(os.path.join(directory, f"{file_name}{self.filenum}.json"), "w") as f:
                    f.write(data)
                self.filenum += 1
                self.output_array.clear()
        except Exception as e:
            print("Not added to JSON file: " + str(e))


class HashInitialzer:
    @staticmethod
    def intialize_hash():
        details = {
            'url': '',
            'title': '',
            'description': '',
            "barcode": '',
            "barcode_type": "UPC",
            "availability": False,
            "price": 0.0,
            "hasVariations": False,
            "isPriceExcVAT": False,
            "brand": '',
            "mpn": '',
            "sku": '',
            "size": '',
            "color": '',
            "offer": '',
            "image": '',
            "images": []
        }
        return details
