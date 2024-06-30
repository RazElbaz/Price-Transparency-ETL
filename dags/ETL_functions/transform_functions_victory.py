
import xml.etree.ElementTree as ET
import os

def parse_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    store_id = root.find('StoreID').text.strip()
    return root, store_id

def transform_data(**kwargs):
    task_instance = kwargs['task_instance']
    print("Extracting data from XML...")
    xml_dir = "/usr/local/airflow/dags/xml_files_victory"
    transformed_data_all = []

    for filename in os.listdir(xml_dir):
        if filename.endswith(".xml"):
            xml_file = os.path.join(xml_dir, filename)
            print(f"Extracting data from XML file: {xml_file}")
            
            root, store_id = parse_xml(xml_file)
            transformed_data = transform_data_victory(root, store_id)
            
            for item in transformed_data:
                print(item)

            transformed_data_all.extend(transformed_data)
            
    task_instance.xcom_push(key='transformed_data_victory', value=transformed_data_all)


def transform_data_victory(root, store_id):
    items = []

    for product_elem in root.findall('.//Product'):
        item = {}
        item['StoreId'] = store_id
        item['PriceUpdateDate'] = (product_elem.find('PriceUpdateDate').text or '').strip() if product_elem.find('PriceUpdateDate') is not None else ''
        item['ItemCode'] = (product_elem.find('ItemCode').text or '').strip() if product_elem.find('ItemCode') is not None else ''
        item['ItemType'] = int((product_elem.find('ItemType').text or '0').strip()) if product_elem.find('ItemType') is not None else 0
        item['ItemName'] = (product_elem.find('ItemName').text or '').strip() if product_elem.find('ItemName') is not None else ''
        item['ItemPrice'] = float((product_elem.find('ItemPrice').text or '0').strip()) if product_elem.find('ItemPrice') is not None else 0.0
        item['SupermarketChain'] = "Victory"
        items.append(item)

    return items

