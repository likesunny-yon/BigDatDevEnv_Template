
// POM > https://mvnrepository.com/artifact/org.apache.spark/spark-core
// POM > https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// POM > https://mvnrepository.com/artifact/org.apache.spark/spark-hive (<scope>compile</scope>)
// POM > https://mvnrepository.com/artifact/org.postgresql/postgresql
// POM > https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-slf4j-impl
// POM > https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api

package com.mycomp

import common.{SparkCommon,PostgresCommon}
import org.apache.spark.sql.{SaveMode,SparkSession}
import org.slf4j.LoggerFactory

/**
 * @author ${user.name}
 */
object Scala_Spark_Hive {

    private val logger = LoggerFactory.getLogger(getClass.getName)
  
    def main(args: Array[String]): Unit = {

        try{

            logger.info("main method started ...")

            val spark : SparkSession = SparkCommon.createSparkSession(true).get

            val pgTable = "newschema.course_catalog"

            val pgCourseDataframe = PostgresCommon.fetchDataFrameFromPgTable(spark, pgTable).get

            logger.info("main method ended ...")
            
            pgCourseDataframe.show()

        } catch {

            case e:Exception =>
                logger.error("An error has occured in the main method" + e.printStackTrace())

        }

    }

}
