# 📘 Otimização de Join com PySpark

Este notebook demonstra como aplicar boas práticas de performance em operações de **join** utilizando o **Apache Spark** com PySpark, especialmente ao trabalhar com arquivos no formato `.parquet`.

---

## 🔧 Tecnologias Utilizadas

- Python 3.x
- Apache Spark (via PySpark)
- Formato de arquivos: `.parquet`

---

## 📂 Etapas Realizadas

### ✅ 1. Leitura dos Dados

```python
df_video = spark.read.parquet("videos-preparados.snappy.parquet")
df_comments = spark.read.parquet("videos-comments-tratados.snappy.parquet")
```

---

### ✅ 2. Criação de Tabelas Temporárias

```python
df_video.createOrReplaceTempView("videos")
df_comments.createOrReplaceTempView("comentarios")
```

---

### ✅ 3. Join com Spark SQL

```sql
SELECT *
FROM videos v
JOIN comentarios c
ON v.`Video ID` = c.`Video ID`
```

---

### ✅ 4. Otimização com `repartition` e `coalesce`

```python
df_video_rep = df_video.repartition("Video ID")
df_comments_rep = df_comments.repartition("Video ID")

df_video_rep.createOrReplaceTempView("videos_rep")
df_comments_rep.createOrReplaceTempView("comentarios_rep")

join_video_comments_rep = spark.sql("""
    SELECT *
    FROM videos_rep v
    JOIN comentarios_rep c
    ON v.`Video ID` = c.`Video ID`
""")

join_video_comments_rep = join_video_comments_rep.coalesce(1)
```

---

## 💡 Objetivo

O notebook tem como objetivo demonstrar como o uso de `repartition` pode melhorar o desempenho de joins distribuídos e como `coalesce` ajuda a consolidar os dados após o processamento, evitando a geração de múltiplos arquivos de saída.

---

## ▶️ Como Executar

1. Instale o PySpark:
   ```bash
   pip install pyspark
   ```

2. Execute o notebook:
   - Com Jupyter Notebook:
     ```bash
     jupyter notebook otimizacao.ipynb
     ```
   - Ou em ambiente como Google Colab (subindo os arquivos `.parquet` previamente).

---

## 📝 Arquivo

- `otimizacao.ipynb`: Notebook contendo todo o código do processo descrito acima.
