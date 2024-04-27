// Databricks notebook source
// MAGIC %md
// MAGIC Conferindo se os dados estão na pasta

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC lendo dados na camada bronze

// COMMAND ----------

val path="dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df= spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC tranformar campos do json em coluna

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")
display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC Remover colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC Salvando na camada silver

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

// MAGIC %md
// MAGIC
