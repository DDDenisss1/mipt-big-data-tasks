# Hive Task: Analysis of KKT Transaction Data

*Учебная задача курса "Хранение и обработка больших объемов данных", ФПМИ МФТИ, 2026*

---

## 📋 Условие задачи

### Исходные данные

**Датасет:** Данные о транзакциях на контрольно-кассовых терминалах (ККТ)  
**Путь на кластере:** `/data/hive/fns2`  
**Формат:** JSON-объекты, каждая строка — отдельная транзакция  
**Протокол:** [http://kktspb.ru/PravoKKT/FNC/2017/www_protokol_informacionnogo_obmena_ofd-fns_ver_3.pdf](http://kktspb.ru/PravoKKT/FNC/2017/www_protokol_informacionnogo_obmena_ofd-fns_ver_3.pdf)

Поля JSON-объекта:
- `kktRegId` — номер ККТ
- `userInn` — ИНН налогоплательщика (владельца ККТ)
- `subtype` — тип транзакции
- Другие поля согласно протоколу

**Важно:** Не все поля обязательны для заполнения. В случае незаполненного поля его значение равно `null`, пусто или отсутствует в JSON-объекте.

### Задание **[424] [0,3 балла]**

1. Создать базу данных и таблицу/таблицы в Hive для работы с данными о транзакциях на ККТ.
   
2. Провести извлечение значений полей JSON-объектов в значения колонок таблиц Hive.
   
3. В случае невозможности извлечения вложенных полей JSON-объектов, возможно оставить значением столбца в таблице Hive как текст, JSON-объект как строку.

**Результат:** Выборка первых 50 строк из созданной таблицы с данными о транзакциях ККТ.

### Формат вывода

- **В HDFS:** таблица с данными о транзакциях
- **На печать:** первые 50 строк таблицы

---

### Рекомендации

1. Для парсинга JSON-файлов используйте: jsonserde.JsonSerDe. Пример:
```sql
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

use kkt;

DROP TABLE IF EXISTS kkt_document_json;
CREATE external TABLE kkt_document_json (
    subtype String,
    ofdId String,
    protocolSubversion BIGINT,
    ...
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
...
```

### Дополнительные комментарии
1. Во всех задачах кроме 3-й выведите только TOP-50 строк в ответ (чтоб не перегружать систему лишним выводом).
2. В датасете есть некорректные строки, поэтому при парсинге данных используйте опцию "ignore.malformed.json" = "true".
3. В задачах 3 и 5 отсортируйте результат по UserInn в порядке возрастания. UserInn строка, поэтому сортируйте лексикографически.
