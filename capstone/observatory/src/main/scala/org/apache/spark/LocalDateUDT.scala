package org.apache.spark

import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType}

import java.time.LocalDate

class LocalDateUDT extends UserDefinedType[LocalDate]{
  override def sqlType: DataType = org.apache.spark.sql.types.DateType
  override def userClass: Class[LocalDate] = classOf[LocalDate]

  override def serialize(d: LocalDate): Any = d.toEpochDay.toInt

  override def deserialize(datum: Any): LocalDate = LocalDate.ofEpochDay(datum.asInstanceOf[Int])
}

object LocalDateUDT {
  def register(): Unit = UDTRegistration.register(classOf[LocalDate].getName, classOf[LocalDateUDT].getName)

}
