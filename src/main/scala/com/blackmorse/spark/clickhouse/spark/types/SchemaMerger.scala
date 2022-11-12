package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import org.apache.spark.sql.types.{StructField, StructType}

object SchemaMerger {
  def mergeSchemas(schema: StructType, clickhouseFields: Seq[ClickhouseField]): Seq[(StructField, ClickhouseField)] =
    schema.map(structField => {
      val clickhouseField = clickhouseFields.find(_.name == structField.name)
        .getOrElse(throw new Exception(s"Different schemas for the dataframe and clickhouse table." +
          s"Clickhouse schema: $clickhouseFields \n DataFrame: $schema"))
      (structField, clickhouseField)
    })
}
