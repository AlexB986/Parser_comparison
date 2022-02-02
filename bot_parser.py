import datetime
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import schedule
import time
import json
number = 0
def parser():
    if not os.path.exists(r'/parser'):
        os.mkdir(r'/parser')
        os.mkdir(r'/parser/p')

    pagenation = 1

    data_croniter = datetime.datetime.now()
    data_now = data_croniter.strftime('%m.%d.%Y %H:%M')
    print(data_now)
    while True:

        url = {pagenation}
        print(url)
        headers = {
                   }
        req = requests.get(url, headers=headers)
        src = req.text
        soup = BeautifulSoup(src, 'lxml')
        all_product_href = soup.find_all(
            class_='product_data__gtm-js product_data__pageevents-js ProductCardHorizontal js--ProductCardInListing js--ProductCardInWishlist')
        info = []
        if (len(all_product_href)):
            for item in all_product_href:
                item_memory = item.find(
                    class_='ProductCardHorizontal__properties_value').next_element.next_element.next_element.next_element.next_element.next_element.next_element.next_element.next_element.next_element.text
                memory = item_memory.strip()[:-3]
                if str(memory) != 'GD' is not int(memory) > 6:
                    item_name = item.find('a', class_='Link_type_default').text
                    item_article = item.find(class_='ProductCardHorizontal__vendor-code').text
                    article = str(item_article).strip()
                    article = article[12:]
                    try:
                        item_sale = item.find(
                            class_='ProductCardHorizontal__price_current-price js--ProductCardHorizontal__price_current-price').text
                        item_sale=item_sale.replace(' ','')
                    except AttributeError:
                        item_sale = '0'
                    info.append(
                        {
                        'артикль':article,
                        'время':data_now,
                        'название':item_name,
                        'аперативка':item_memory.strip(),
                        'цена':item_sale.strip(),
                         }
                    )

            pagenation += 1
        else:
            break
        with open(f'/parser{pagenation}.json', mode='w') as file:
            json.dump(info, file, indent=4, ensure_ascii=False)

    direct = os.listdir(r'parser')
    col_files = len(direct)
    number_file = 2

    df = pd.read_json(f'/parser/{direct[1]}')
    df.to_csv(f'p/parser_s_q{number}.csv', mode='w')
    # while True:
    for i in range(2, col_files):
        pd.set_option('display.max_columns', 7)
        df = pd.read_json(f'parser/{direct[i]}')
        number_file += 1
        df.to_csv(f'/p/parser_n_q{number}.csv', mode='a', header=False)
    sump_last_file = os.listdir(r'/parser/p')
    df1 = pd.read_csv('/p/parser_n_q1.csv')
    df2 = pd.read_csv(f'/parser/p/{sump_last_file[-1]}')
    df3 = df1.merge(df2, how='outer', left_on='артикль', right_on='артикль', indicator=True)
    df3['разница цен'] = df3['цена_x'] - df3['цена_y']
    up = df3[df3['разница цен'] < 0][['время_x', 'артикль', 'название_y', 'цена_x', 'цена_y']]
    down = df3[df3['разница цен'] > 0][['время_x', 'артикль', 'название_y', 'цена_x', 'цена_y']]
    print('подорожание', up)
    print('дешевеет', down)
    # df3.to_csv('primer.csv')

schedule.every(1).seconds.do(parser)
while True:
    schedule.run_pending()
    time.sleep(1)
    number += 1
    print(number)














