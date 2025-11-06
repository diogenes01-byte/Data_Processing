// Databricks notebook source
// MAGIC %md
// MAGIC ## Practica procesamiento de datos con Spark
// MAGIC
// MAGIC ### Empaquetado "Spark standalone"

// COMMAND ----------

// Ejercicio 1: Crear un DataFrame y realizar operaciones básicas

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Crear sesión de Spark
implicit val spark = SparkSession.builder()
  .appName("ExamenEjercicio1")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Datos de prueba
val estudiantes = Seq(
  ("Ana", 20, 9.5),
  ("Diogenes", 29, 10.0),
  ("Princesa de asturias", 21, 8.8),
  ("Ronaldo", 19, 6.5),
  ("Julian", 23, 9.0)
).toDF("nombre", "edad", "calificacion")

// Función DF
def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
  println("=== EJERCICIO 1 ===")
  
  // 1. Mostrar esquema
  println("\n1. Esquema del DataFrame:")
  estudiantes.printSchema()
  
  // 2. Filtrar calificación > 8
  val filtrados = estudiantes.filter(col("calificacion") > 8)
  println("\n2. Estudiantes con calificación > 8:")
  filtrados.show()
  
  // 3. Seleccionar nombres y ordenar descendente
  val resultado = estudiantes
    .select(col("nombre"), col("calificacion"))
    .orderBy(col("calificacion").desc)
  
  println("\n3. Nombres ordenados por calificación descendente:")
  resultado.show()
  
  resultado
}

// Ejecutar y mostrar resultados
println("INICIANDO EJECUCIÓN:")
val resultadoFinal = ejercicio1(estudiantes)
println("EJECUCIÓN COMPLETADA")

// COMMAND ----------

// Ejercicio 2: UDF (User Defined Function)

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Crear sesión de Spark
implicit val spark = SparkSession.builder()
  .appName("ExamenEjercicio2")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Datos de prueba
val numeros = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")

// Función UDF
def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
  println("=== EJERCICIO 2 ===")
  
  // 1. Mostrar datos originales
  println("\n1. Datos originales:")
  numeros.show()
  
  // 2. Definir UDF para par/impar
  val esPar = (numero: Int) => if (numero % 2 == 0) "Par" else "Impar"
  
  // 3. Registrar UDF
  val esParUDF = udf(esPar)
  
  // 4. Aplicar UDF al DataFrame
  val resultado = numeros.withColumn("tipo", esParUDF(col("numero")))
  
  println("\n2. Resultado con UDF aplicado:")
  resultado.show()
  
  resultado
}

// Ejecutar y mostrar resultados
println("INICIANDO EJECUCIÓN EJERCICIO 2:")
val resultadoFinal = ejercicio2(numeros)
println("EJECUCIÓN COMPLETADA")

// COMMAND ----------

// Ejercicio 3: Joins y agregaciones

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Crear sesión de Spark
implicit val spark = SparkSession.builder()
  .appName("ExamenEjercicio3")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Datos de prueba - Estudiantes
val estudiantes = Seq(
  (1, "Ana"),
  (2, "Luis"), 
  (3, "Maria"),
  (4, "Carlos")
).toDF("id", "nombre")

// Datos de prueba - Calificaciones
val calificaciones = Seq(
  (1, "Matemáticas", 8.5),
  (1, "Historia", 9.0),
  (2, "Matemáticas", 7.0),
  (2, "Historia", 6.5),
  (3, "Matemáticas", 9.5),
  (3, "Historia", 8.0),
  (4, "Matemáticas", 5.5),
  (4, "Historia", 6.0)
).toDF("id_estudiante", "asignatura", "calificacion")

// Función para realizar Joins y agregaciones
def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
  println("=== EJERCICIO 3 ===")
  
  // 1. Mostrar datos originales
  println("\n1. DataFrame Estudiantes:")
  estudiantes.show()
  
  println("\n2. DataFrame Calificaciones:")
  calificaciones.show()
  
  // 2. Realizar join entre los DataFrames
  val joinDF = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
  
  println("\n3. Resultado del Join:")
  joinDF.show()
  
  // 3. Calcular promedio de calificaciones por estudiante
  val promedioDF = joinDF
    .groupBy("id", "nombre")
    .agg(avg("calificacion").alias("promedio_calificacion"))
    .orderBy("id")
  
  println("\n4. Promedio de calificaciones por estudiante:")
  promedioDF.show()
  
  promedioDF
}

// Ejecutar y mostrar resultados
println("INICIANDO EJECUCIÓN EJERCICIO 3:")
val resultadoFinal = ejercicio3(estudiantes, calificaciones)
println("EJECUCIÓN COMPLETADA")

// COMMAND ----------

// Ejercicio 4: Uso de RDDs (Resilient Distributed Datasets)

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD

// Crear sesión de Spark
implicit val spark = SparkSession.builder()
  .appName("ExamenEjercicio4")
  .master("local[*]")
  .getOrCreate()

// Datos de prueba
val palabras = List("gato", "perro", "gato", "elefante", "perro", "perro", "gato", "ratón")

// Función para uso de RDDs
def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
  println("=== EJERCICIO 4 ===")
  
  // 1. Mostrar lista original
  println("\n1. Lista original de palabras:")
  println(palabras.mkString("[", ", ", "]"))
  
  // 2. Crear RDD a partir de la lista
  val palabrasRDD = spark.sparkContext.parallelize(palabras)
  
  println("\n2. RDD creado (primeras 10 elementos):")
  palabrasRDD.take(10).foreach(println)
  
  // 3. Contar ocurrencias de cada palabra
  val conteoRDD = palabrasRDD
    .map(palabra => (palabra, 1))
    .reduceByKey(_ + _)
  
  println("\n3. Conteo de ocurrencias por palabra:")
  val resultados = conteoRDD.collect()
  resultados.foreach { case (palabra, count) => 
    println(s"$palabra: $count") 
  }
  
  conteoRDD
}

// Ejecutar y mostrar resultados
println("INICIANDO EJECUCIÓN EJERCICIO 4:")
val resultadoFinal = ejercicio4(palabras)
println("\nEJECUCIÓN COMPLETADA")

// Mostrar que el RDD está disponible para más operaciones
println(s"\nTipo del resultado: ${resultadoFinal.getClass.getSimpleName}")
println("Primeros elementos del RDD:")
resultadoFinal.take(5).foreach(println)

// COMMAND ----------

// Ejercicio 5: Procesamiento de archivos

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Crear sesión de Spark
implicit val spark = SparkSession.builder()
  .appName("ExamenEjercicio5")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Función para procesamiento de datos
def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
  println("=== EJERCICIO 5 ===")
  
  println("\n1. Esquema del DataFrame de ventas:")
  ventas.printSchema()
  
  println("\n2. Primeras 10 filas del DataFrame:")
  ventas.show(10)
  
  val resultado = ventas
    .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
    .groupBy("id_producto")
    .agg(sum("ingreso_total").alias("ingreso_total_producto"))
    .orderBy(col("ingreso_total_producto").desc)
  
  println("\n3. Ingreso total por producto (ordenado de mayor a menos):")
  resultado.show()
  
  resultado
}

val ventasDF = spark.table("ventas")

// Ejecutar
println("INICIANDO EJECUCIÓN EJERCICIO 5:")
val resultadoFinal = ejercicio5(ventasDF)
println("EJECUCIÓN COMPLETADA")

// COMMAND ----------

