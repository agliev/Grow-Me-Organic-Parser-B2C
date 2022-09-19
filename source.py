import requests
import pandas as pd
import os


# Необходимо менять headers перед каждым запуском кода TODO Автоматизировать обновление headers - попытка с selenium плодов не дала  
HEADERS = {'Cookie': 'tk_or=%22https%3A%2F%2Fwww.google.com%2F%22; _fbp=fb.1.1654861680272.1737963368; _hjSessionUser_2739501=eyJpZCI6IjYyY2QzNDU0LTk4ZWMtNTBiYi1iMWJjLWQwMjNkYTM0ZTRlZiIsImNyZWF0ZWQiOjE2NTQ4NjE2ODA1NDQsImV4aXN0aW5nIjp0cnVlfQ==; usetiful-visitor-ident=a8f91f19-1fa3-4ac8-31aa-1708dae1bb09; crisp-client%2Fsession%2F2dc87a17-fcbe-4c6b-ac38-cbd56e9e2376=session_b17d699e-12b9-4efa-8bc5-d18afb10ef1f; _tcfpup=1657190242155; ti_ukp=88464a3d.db6d.32db.0076.2f019bfe974e; _gcl_au=1.1.538012047.1662667798; tk_lr=%22%22; _oauth_id_token_cookie=9e8f4929d202042783aa52ae3a630e02-6a5f2260fe2ba76b642d0b4682f22516-9633a0206ac7cc321c72a3b40e8a2c48-649ca187c6da8240099945dec9c5f00f; _oauth_id_token_cookie_sub=632017f23f3cf5f60c8b7151; tk_r3d=%22%22; _ga=GA1.2.2136330129.1654861678; _gid=GA1.2.255347858.1663605189; _gat_gtag_UA_206779665_1=1; _hjSession_2739501=eyJpZCI6IjdjYjAyYzVhLWZiZTktNDg5OS05Njk5LTM0NTgwYmZmMjgxMCIsImNyZWF0ZWQiOjE2NjM2MDUxOTE0MTcsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; PHPSESSID=8dgd2lvuv48ec6bluk18uv5aq6; _oauth_id_token=KVS_SUB632017f23f3cf5f60c8b7151; _ga_E61RPSQPM7=GS1.1.1663605188.58.0.1663605191.0.0.0'}

def get_industries() -> list():
        industries = [d['query'] for d in requests.get("https://apps.growmeorganic.com/api-product/load-filters-companies",
                                               headers=HEADERS).json()['industries']]
        return industries


def comp_parser(data:dict,
                industry:str, 
                country:str,
                region:str,
                location:str,
                headers:dict=HEADERS) -> pd.DataFrame():

        n_pages = int(requests.post(url='https://apps.growmeorganic.com/api-product/lookup-local-businesses',
                                        data=data,
                                        headers=headers).json()['pages'][-1])
        
        result = pd.DataFrame()
        
        for page in range(1,n_pages+1):
        
                data =  {'query': industry,
                        'sort': 'no_sort',
                        'location': location,
                        'page': f'{page}',
                        'period':'last_year',
                        'region': region,
                        'country': country}

                dict_list = requests.post( url='https://apps.growmeorganic.com/api-product/lookup-local-businesses',
                                        data=data,
                                        headers=headers).json()['data']
                
                

                for data in dict_list:
                        series = pd.Series(data).drop(['extensions', 'is_saved'])
                        result = pd.concat([result,series], axis=1)

        return result.T.reset_index(drop=True)
                
def add_info(df:pd.DataFrame(),
             terr:dict,
             industry:str):

        df['city'] = terr['full_name'].split(',')[0]
        df['region'] = terr['full_name'].split(',')[1]
        df['country'] = terr['full_name'].split(',')[2]
        df['full_terr'] = terr['full_name']
        df['industry'] = industry

        return df
        

def terr_parser(n:int, m:int, terr_dict:dict=dict()) -> pd.DataFrame():

        ''' Выгружает территориальные данные в виде Датафрейма.
            Диапазон выгрузки определяется вводимыми числами.
        '''



        for _ in range(n,m):

                try:
                        terr = requests.get(url=f'https://apps.growmeorganic.com/api-product/search-country-local-businesses-locations-region?region={_}',
                                            headers=HEADERS).json()['locations']
                        
                        for territory in terr:
                        
                                if territory['country_code'] in terr_dict.keys():
                                        terr_dict[territory['country_code']].append(territory)
                                
                                else:
                                        terr_dict[territory['country_code']] = [territory]

                except:
                        pass
        
        return terr_dict 


def b2c(city:pd.Series,
        terr_dict:dict,
        codes_dict:dict,
        result:pd.DataFrame=pd.DataFrame(),
        industries:list=get_industries()):
        

        for industry in industries:
                for terr in  terr_dict[codes_dict[city.country]]:
                        if terr['full_name'].split(',')[1].strip() not in ['Ukrain','Russia']:

                                try:
                                        if (city['name'] == terr['name']) and (terr['type']=='City'):

                                                data = {'query': industry,
                                                        'sort': 'no_sort',
                                                        'location': str(terr['id']),
                                                        'page': '1',
                                                        'period':'last_year',
                                                        'region': str(terr['parent_id']),
                                                        'country': codes_dict[city.country].lower}
                                                
                                                # Выгружаем доступные данные о компаниях
                                                df = comp_parser(data=data,
                                                                industry=data['query'],
                                                                country=data['country'],
                                                                location=data['location'],
                                                                region=data['region'])

                                                # Добавлям информацию из запроса
                                                df = add_info(df, terr, industry)

                                                # Добавляем полученные данные к собранным ранее и сохраняем в excel формате
                                                result = pd.concat([result, df], axis=0).reset_index(drop=True)
                                                result.to_excel(f"data/cities/{city['name']}.xlsx")
                                except:
                                        pass



def codes_to_dict():

        '''Создает словарь - Страна:Код страны из двух лат. букв
        '''

        iso_codes = pd.read_excel('country_codes.xlsx')[['Country','Alpha-2 code']]

        codes_dict = dict()

        for _, row  in iso_codes.iterrows():
                codes_dict[row[0]] = row[1]


        keys = list(codes_dict.keys())

        for key in keys:
                if ' (the)' in key:
                        codes_dict[key.replace(' (the)','')] = codes_dict[key]
        
        return codes_dict 


def get_needed_ind(industries:list=get_industries(),
                   path:str='data/cities'):

        city_needs_ind_dict = dict()

        city_files = [name for name in os.listdir(path)]

        for file in city_files: 
                downloaded_ind = set(pd.read_excel(f'data/cities/{file}')["industry"].values)
                city_needs_ind_dict[file[:-5]] = [ind for ind in industries if ind not in downloaded_ind]

        return city_needs_ind_dict


def prepairing(n, m, path='data/cities') -> tuple:

                print('Getting industries!')
                industries = get_industries()

                print('Searching for needed industries!')
                needed_ind = get_needed_ind(industries=industries, path=path)

                print('Getting terr data!')
                terr_dict = terr_parser(n, m) # Значения подставляются в зависимости от необходимого региона

                print('Prepairing codes for countries!')
                codes_dict = codes_to_dict()

                print('Loadind cities')
                cities = pd.read_csv('csvData.csv') # данные о 100 самых крупных городах Европы

                cities_to_do = [] # Список городов, для выгрузки которых нужен специализированный terr_dict - особенности данных в выгружаемой базе

                print('Prepairing is done!')

                return industries, needed_ind, terr_dict, codes_dict, cities, cities_to_do