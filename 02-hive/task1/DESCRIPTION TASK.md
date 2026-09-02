## 🛠 Стек технологий

- **Apache Hive** (Cloudera CDH 5.11.2) на кластере МФТИ
- **Apache Hadoop HDFS** (хранение данных в `/data/hive/fns2`)
- **JsonSerDe** (`org.openx.data.jsonserde.JsonSerDe`) — десериализатор JSON-строк в колонки Hive
- **YARN** (управление ресурсами кластера)
- **Bash** (скрипт запуска `run.sh`)

## 💡 Архитектура решения

Решение задачи **[424]** состоит из двух SQL-скриптов: создания внешней таблицы с парсингом JSON и выборки первых 50 строк для проверки корректности данных.

### 🔹 Шаг 1: Создание внешней таблицы (`00_task1-create-table-serde.sql`)

**Подключение JsonSerDe:**
sql
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;


- Библиотека `json-serde-1.3.8` позволяет Hive читать JSON-файлы построчно и мапить поля JSON-объекта на колонки таблицы
- Без этого JAR Hive не сможет распарсить вложенные JSON-структуры

**Настройки окружения:**

```sql
SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
```

recursive=true + subdirectories=true — позволяют Hive читать данные из вложенных поддиректорий в /data/hive/fns2 (данные разбиты по папкам)
print.header=false — отключает вывод заголовков колонок (важно для корректной работы автотестов)
Создание таблицы:
