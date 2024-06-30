
import xml.etree.ElementTree as ET
import os

def parse_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    store_id = root.find('StoreId').text.strip()
    return root, store_id

def transform_data(**kwargs):
    task_instance = kwargs['task_instance']
    print("Extracting data from XML...")
    xml_dir = "/usr/local/airflow/dags/xml_files_shufersal"
    transformed_data_all = []

    for filename in os.listdir(xml_dir):
        if filename.endswith(".xml"):
            xml_file = os.path.join(xml_dir, filename)
            print(f"Extracting data from XML file: {xml_file}")
            
            root, store_id = parse_xml(xml_file)
            transformed_data = transform_data_shufersal(root, store_id)
            
            for item in transformed_data:
                print(item)

            transformed_data_all.extend(transformed_data)
            
    task_instance.xcom_push(key='transformed_data', value=transformed_data_all)


def transform_data_shufersal(root, store_id):
    items = []

    for item_elem in root.findall('.//Item'):
        item = {}
        item['StoreId'] = store_id
        item['PriceUpdateDate'] = item_elem.find('PriceUpdateDate').text.strip()
        item['ItemCode'] = item_elem.find('ItemCode').text.strip()
        item['ItemType'] = int(item_elem.find('ItemType').text.strip())
        item['ItemName'] = item_elem.find('ItemName').text.strip()
        item['ItemPrice'] = float(item_elem.find('ItemPrice').text.strip())
        item['SupermarketChain'] = "Shufersal"
        items.append(item)

    return items


