# Spark Task: Shortest Path Search with BFS

*Учебная задача курса "Хранение и обработка больших объемов данных", ФПМИ МФТИ, 2026*

---

##  Условие задачи

### Исходные данные

**Датасет:** Граф социальных связей Twitter  
**Путь на кластере:** 
- Полный: `/data/twitter/twitter_sample.txt`
- Тестовый: `/data/twitter/twitter_sample_small.txt`

**Формат данных:**

user_id \t follower_id

Каждая строка представляет направленное ребро графа (user_id → follower_id)

### Задание

Найти длину кратчайшего пути между вершинами **12** и **34** ориентированного графа, реализовав алгоритм **BFS (Breadth-First Search / Поиск в ширину)**.

**Требования:**
1. Решить задачу **двумя способами**:
   - С помощью **RDD API**
   - С помощью **DataFrame API**
   
2. Замерить **CPU time** (не wall time, поскольку он измеряет время с учётом загруженности кластера)

3. Вывести путь в формате: `12,42,57,34` (последовательность вершин через запятую, без пробелов)

4. Оптимизация: можно остановить алгоритм раньше, чем закончится поиск в ширину, так как достаточно найти **один путь**

По возможности, необходимо избегать написания UDF, поскольку UDF ухудшают производительность. Вместо этого внимательно изучите возможности pyspark.sql.functions. Вам точно пригодится этот модуль.

🔹**Стартовый фрагмент кода**

От этого фрагмента кода можно отталкиваться при решении задачи. Этот код не эффективный поэтому он не будет работать в системе проверки. Его цель - дать понимание, от чего отталкиваться в задаче.

```python
def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  prev_v, prev_d, next_v = item[0], item[1][0], item[1][1]
  return (next_v, prev_d + 1)

def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)

n = 400  # number of partitions
edges = sc.textFile("/data/twitter/twitter_sample_small.txt").map(parse_edge)
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

x = 12
d = 0
distances = sc.parallelize([(x, d)]).partitionBy(n)
while True:
  candidates = distances.join(forward_edges, n).map(step)
  new_distances = distances.fullOuterJoin(candidates, n).map(complete, True).persist()
  count = new_distances.filter(lambda i: i[1] == d + 1).count()
  if count > 0:
    d += 1
    distances = new_distances
  else:
    break
```
🔹Код для создания SparkContext.

```python
from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("my_super_app").setMaster("local[3]")  # конфиг, в котором указываем название приложения и режим выполнения (local[*] для локального запуска, yarn для запуска через YARN). В систему сдаём код с мастером YARN.
sc = SparkContext(conf=config)  # создаём контекст, пользуясь конфигом
```


