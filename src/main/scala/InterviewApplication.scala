import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InterviewApplication {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("spark-interview")
      .master("local")
      .getOrCreate()

    val customerSensors = session.read
      .option("sep", ";")
      .csv("data/customer_sensors.csv")

    val sensors = session.read
      .option("sep", ";")
      .csv("data/sensors.csv")

    test(customerSensors, sensors)
  }

  def test(customerSensors: DataFrame, sensors: DataFrame): Unit = {
    customerSensors.limit(3)
      .foreach((r: Row) => println(r))

    sensors.limit(3)
      .foreach((r: Row) => println(r))
  }

  def task1(customerSensors: DataFrame, sensors: DataFrame): Unit = {

  }

}