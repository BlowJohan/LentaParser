import requests as rq
import pandas as pd
from datetime import datetime, timedelta
from IPython import display


class LentaRuParser:
    def __init__(self):
        pass

    def get_url(self, param_dict: dict) -> str:
        """
        Возвращает URL для запроса json таблицы со статьями

        url = 'https://lenta.ru/search/v2/process?'\
        + 'from=0&'\                       # Смещение
        + 'size=1000&'\                    # Кол-во статей
        + 'sort=2&'\                       # Сортировка по дате (2), по релевантности (1)
        + 'title_only=0&'\                 # Точная фраза в заголовке
        + 'domain=1&'\                     # ??
        + 'modified%2Cformat=yyyy-MM-dd&'\ # Формат даты
        + 'type=1&'\                       # Материалы. Все материалы (0). Новость (1)
         + 'bloc=4&'\                       # Рубрика. Экономика (4). Все рубрики (0)
        + 'modified%2Cfrom=2020-01-01&'\
        + 'modified%2Cto=2020-11-01&'\
        + 'query='                         # Поисковой запрос
        """
        has_type = int(param_dict['type']) != 0
        has_bloc = int(param_dict['bloc']) != 0

        url = 'https://lenta.ru/search/v2/process?' \
              + 'from={}&'.format(param_dict['from']) \
              + 'size={}&'.format(param_dict['size']) \
              + 'sort={}&'.format(param_dict['sort']) \
              + 'title_only={}&'.format(param_dict['title_only']) \
              + 'domain={}&'.format(param_dict['domain']) \
              + 'modified%2Cformat=yyyy-MM-dd&' \
              + 'type={}&'.format(param_dict['type']) * has_type \
              + 'bloc={}&'.format(param_dict['bloc']) * has_bloc \
              + 'modified%2Cfrom={}&'.format(param_dict['dateFrom']) \
              + 'modified%2Cto={}&'.format(param_dict['dateTo']) \
              + 'query={}'.format(param_dict['query'])

        return url

    def get_search_table(self, param_dict: dict) -> pd.DataFrame:

        """
        Возвращает pd.DataFrame со списком статей
        """
        url = self.get_url(param_dict)
        result = rq.get(url)

        if result.status_code == 200 and result.headers["content-type"].strip().startswith("application/json"):
            json = result.json()['matches']
            return pd.DataFrame(json)

    def get_articles(self,
                     param_dict,
                     time_step=1,
                     save_every=5,
                     save_excel=True) -> pd.DataFrame:
        """
        Функция для скачивания статей интервалами через каждые time_step дней
        Делает сохранение таблицы через каждые save_every * time_step дней

        param_dict: dict
        ### Параметры запроса
        ###### project - раздел поиска, например, rbcnews
        ###### category - категория поиска, например, TopRbcRu_economics
        ###### dateFrom - с даты
        ###### dateTo - по дату
        ###### offset - смещение поисковой выдачи
        ###### limit - лимит статей, максимум 100
        ###### query - поисковой запрос (ключевое слово), например, РБК

        """

        param_copy = param_dict.copy()
        time_step = timedelta(days=time_step)
        date_from = datetime.strptime(param_copy['dateFrom'], '%Y-%m-%d')
        date_to = datetime.strptime(param_copy['dateTo'], '%Y-%m-%d')
        if date_from > date_to:
            raise ValueError('dateFrom should be less than dateTo')

        out = pd.DataFrame()
        save_counter = 0

        while date_from <= date_to:
            param_copy['dateTo'] = (date_from + time_step).strftime('%Y-%m-%d')
            if date_from + time_step > date_to:
                param_copy['dateTo'] = date_to.strftime('%Y-%m-%d')

            print('Parsing articles from ' + param_copy['dateFrom'] + ' to ' + param_copy['dateTo'])

            out = pd.concat([out, self.get_search_table(param_copy)])
            date_from += time_step + timedelta(days=1)
            param_copy['dateFrom'] = date_from.strftime('%Y-%m-%d')
            save_counter += 1

            if save_counter == save_every:
                display.clear_output(wait=True)
                out.to_excel("/tmp/checkpoint_table.xlsx")
                print('Checkpoint saved!')
                save_counter = 0

        if save_excel:
            out.to_excel("lenta_{}_{}.xlsx".format(
                param_dict['dateFrom'],
                param_dict['dateTo']
            ))
        print('Finish')

        return out


use_parser = "LentaRu"

query = ''
offset = 0
size = 1000000
sort = "3"
title_only = "0"
domain = "1"
material = "0"
bloc = "4"
dateFrom = "2019-01-01"
dateTo = "2019-02-01"

param_dict = {
        'query': query,
        'from': str(offset),
        'size': str(size),
        'dateFrom': dateFrom,
        'dateTo': dateTo,
        'sort': sort,
        'title_only': title_only,
        'type': material,
        'bloc': bloc,
        'domain': domain
    }

parser = LentaRuParser()
tbl = parser.get_articles(param_dict=param_dict,
                          time_step=1,
                          save_every=5,
                          save_excel=True)
print(len(tbl.index))
tbl.head()
