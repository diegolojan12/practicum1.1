package bim2.semana11.presentacion

import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// Case class con nombres iguales a los headers del CSV
case class Goleador(
                     JUGADOR: String,
                     CLUB: String,
                     NACIONALIDAD: String,
                     GOLES: Int,
                     AUTOGOL: String
                   )

// Derivación automática del decoder
given CsvRowDecoder[Goleador, String] = deriveCsvRowDecoder[Goleador]

// ============================================
// Objeto con funciones estadísticas genéricas
// ============================================
object Estadisticos:
  def suma(datos: List[Int]): Int = datos.sum

  def promedio(datos: List[Int]): Double =
    if datos.isEmpty then 0.0
    else datos.sum.toDouble / datos.length

  def maximo(datos: List[Int]): Int =
    if datos.isEmpty then 0
    else datos.max

  def minimo(datos: List[Int]): Int =
    if datos.isEmpty then 0
    else datos.min

  def conteo[A](datos: List[A]): Int = datos.length

  def conteoUnicos[A](datos: List[A]): Int = datos.distinct.length

  def frecuencias[A](datos: List[A]): Map[A, Int] =
    datos.groupBy(identity).map((k, v) => k -> v.length)


// ============================================
// Objeto principal - Lectura y procesamiento
// ============================================
object EstadisticasGoleador extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/Goleadores_LigaPro_2019.csv")

  val run: IO[Unit] =
    val lecturaCSV: IO[List[Goleador]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Goleador](';'))
      .compile
      .toList

    lecturaCSV.flatMap { goleadores =>
      val colGoles: List[Int] = goleadores.map(_.GOLES)
      val colClubes: List[String] = goleadores.map(_.CLUB)
      val colNacionalidades: List[String] = goleadores.map(_.NACIONALIDAD)

      (
        IO.println("=" * 55) >>
          IO.println("       ESTADÍSTICAS - COLUMNA GOLES") >>
          IO.println("=" * 55) >>
          IO.println(s"  Total registros:      ${Estadisticos.conteo(colGoles)}") >>
          IO.println(s"  Suma total:           ${Estadisticos.suma(colGoles)}") >>
          IO.println(s"  Promedio:             %.2f".format(Estadisticos.promedio(colGoles))) >>
          IO.println(s"  Máximo:               ${Estadisticos.maximo(colGoles)}") >>
          IO.println(s"  Mínimo:               ${Estadisticos.minimo(colGoles)}") >>
          IO.println("") >>
          IO.println("=" * 55) >>
          IO.println("       ESTADÍSTICAS - COLUMNA CLUB") >>
          IO.println("=" * 55) >>
          IO.println(s"  Total registros:      ${Estadisticos.conteo(colClubes)}") >>
          IO.println(s"  Clubes únicos:        ${Estadisticos.conteoUnicos(colClubes)}") >>
          IO.println("") >>
          IO.println("=" * 55) >>
          IO.println("       ESTADÍSTICAS - COLUMNA NACIONALIDAD") >>
          IO.println("=" * 55) >>
          IO.println(s"  Total registros:          ${Estadisticos.conteo(colNacionalidades)}") >>
          IO.println(s"  Nacionalidades únicas:    ${Estadisticos.conteoUnicos(colNacionalidades)}") >>
          IO.println("=" * 55)
        )
    }
