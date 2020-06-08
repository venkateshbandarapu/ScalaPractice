import java.sql.Timestamp

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class ListAccum extends AccumulatorV2[Timestamp,Timestamp]{
  var svcDatesList= new mutable.ArrayBuffer[Timestamp]

  override def isZero: Boolean = svcDatesList.isEmpty

  override def copy(): AccumulatorV2[Timestamp, Timestamp] = null

  override def reset(): Unit = svcDatesList.clear()

  override def add(v: Timestamp): Unit = svcDatesList+=v

  override def merge(other: AccumulatorV2[Timestamp, Timestamp]): Unit = null

  override def value: Timestamp = svcDatesList.last

  def value(index:Int):Timestamp=svcDatesList.apply(index)

  def lastValue():Timestamp=svcDatesList.last

  def getList():List[Timestamp]=svcDatesList.toList
}
