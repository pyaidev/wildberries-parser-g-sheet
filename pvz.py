import gspread
import json
import requests
from oauth2client.service_account import ServiceAccountCredentials
import geopandas as gpd
import pandas as pd
import schedule
import time


credentials_file = 'ccc.json'
main_sheet_name = 'ВБ Адреса ПВЗ'


def parsing_all():
    url = "https://www.wildberries.ru/webapi/spa/modules/pickups"
    headers = {'User-Agent': "Mozilla/5.0", 'content-type': "application/json", 'x-requested-with': 'XMLHttpRequest'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        filename = "data.json"
        with open(filename, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        print(f"Парсинг завершен. Получено {len(data['value']['pickups'])} пунктов выдачи")
    else:
        print(f"Ошибка при получении данных с сервера. Код ответа: {response.status_code}")


def reverse_geocoding():
    with open('data.json', 'r') as json_file:
        data = json.load(json_file)
    df = pd.DataFrame(data['value']['pickups'])
    df['coordinates'] = df['coordinates'].apply(lambda x: tuple(map(float, x)))
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['coordinates'].apply(lambda x: x[1]),
                                                           df['coordinates'].apply(lambda x: x[0])))

    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    result = gpd.sjoin(gdf, world, how='left')
    result.to_csv('data.csv', index=False)
    print(f"Обратное геокодирование завершено. Получено {len(result)} пунктов выдачи")




def update_data():
    df = pd.read_csv('data.csv')
    df = df[df['iso_a3'] == 'RUS']
    print(df)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    client = gspread.authorize(creds)

    main_sheet = client.open(main_sheet_name).sheet1
    main_sheet.clear()
    main_rows = [["id", "address"]]
    for index, row in df.iterrows():
        if row['isWb'] == True:
            main_rows.append([row['id'], row['address']])

    main_sheet.insert_rows(main_rows)

    print(f"Обновлено {len(df)} пунктов выдачи")


if __name__ == '__main__':
    schedule.every().day.at("17:38").do(parsing_all)
    schedule.every().day.at("17:39").do(reverse_geocoding)
    schedule.every().day.at("17:39").do(update_data)

    while True:
        schedule.run_pending()
        time.sleep(1)