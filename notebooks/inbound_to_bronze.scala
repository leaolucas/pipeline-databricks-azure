// Databricks notebook source
// MAGIC %md
// MAGIC Verificando se o arquivo foi carregado

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/inbound")

// COMMAND ----------

val path="dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados=spark.read.json(path)

// COMMAND ----------

// MAGIC %md
// MAGIC visualizando os dados do arquivo

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC excluindo colunas desnecessárias

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC Criando uma coluna para identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col 

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC salvando na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


