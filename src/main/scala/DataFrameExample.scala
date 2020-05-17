import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
object DataFrameExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    //joinExample(spark)
  }
    def problem1(spark: SparkSession): Unit = {
      val dftag = spark
        .read
        // If header is true than it will take column namme from CSV file and if false it will make column name as _c0, _c1 like that
        .option("header", "true")
        //This essentially instructs Spark to automatically infer the data type for each column when reading the CSV file
        .option("inferSchema", "true")
        .csv("C:\\Users\\skumar3\\Documents\\sample_file\\emp_data.txt")
        .toDF()
      //dftag.printSchema()
      //dftag.show()

      dftag.filter("ename = 'SMITH'").show()
      println(s"numder of person = ${dftag.filter("ename == 'SMITH'").count()}")
      dftag.filter("ename like 'S%'").filter("designation = 'CLERK'").show()
      dftag.groupBy("ename").count().show()
      dftag.groupBy("ename").count().filter("count =>2").show()
    }

    def problem2(spark: SparkSession): Unit = {
      val dfQuestionsCSV = spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dateFormat","yyyy-MM-dd HH:mm:ss")
        .csv("C:\\Users\\skumar3\\Documents\\sample_file\\questions_10K.csv")
        .toDF()

      val dfQuestion = dfQuestionsCSV.select(
        dfQuestionsCSV.col("Id").cast("String"),
        dfQuestionsCSV.col("CreationDate").cast("timestamp"),
        dfQuestionsCSV.col("ClosedDate").cast("timestamp"),
        dfQuestionsCSV.col("DeletionDate").cast("Date"),
        dfQuestionsCSV.col("Score").cast("Integer"),
        dfQuestionsCSV.col("OwnerUserId").cast("Integer"),
        dfQuestionsCSV.col("AnswerCount").cast("Integer")
      )

      //dfQuestionsCSV.printSchema()
      //dfQuestion.printSchema()
      //dfQuestion.show(11)
      val dfQuestionSubset = dfQuestion.filter("score > 400 and score < 410").toDF()
      dfQuestionSubset.show(11)

    }
  def joinExample(spark: SparkSession): Unit = {
    val dfEmp = spark.read.option("header", "true").option("inferSchema", "true").option("dateFormat","yyyy-MM-dd HH:mm:ss").csv("C:\\Users\\skumar3\\Documents\\sample_file\\emp.txt").toDF()
    val dfDept = spark.read.option("header", "true").option("inferSchema", "true").option("dateFormat","yyyy-MM-dd HH:mm:ss").csv("C:\\Users\\skumar3\\Documents\\sample_file\\dept.txt").toDF()

    dfDept.show()
    dfEmp.show()
    println("***********join Example**********")

    dfEmp.join(dfDept,"dept_id").show()
  }


  }

