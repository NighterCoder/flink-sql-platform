package com.flink.platform.shims112

import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
/**
  *
  * Created by 凌战 on 2021/3/29
  */
object Flink112ScalaShims {

  def fromDataSet(btenv: BatchTableEnvironment, ds: DataSet[_]): Table = {
    btenv.fromDataSet(ds)
  }

  def toDataSet(btenv: BatchTableEnvironment, table: Table): DataSet[Row] = {
    btenv.toDataSet[Row](table)
  }
}
