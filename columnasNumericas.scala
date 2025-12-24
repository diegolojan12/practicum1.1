import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

case class Movie(
                  id: Double,
                  budget: Double,
                  popularity: Double,
                  revenue: Double,
                  runtime: Double,
                  vote_average: Double,
                  vote_count: Double
                )

given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]

object Estadisticos:
  def promedio(datos: List[Double]): Double =
    if datos.isEmpty then 0.0 else datos.sum / datos.length

  // Nueva estadística: Suma Total
  def sumaTotal(datos: List[Double]): Double = datos.sum

  // Nueva estadística: Desviación Estándar
  def desviacionEstandar(datos: List[Double]): Double =
    if datos.isEmpty then 0.0
    else
      val media = promedio(datos)
      val varianza = datos.map(x => Math.pow(x - media, 2)).sum / datos.length
      Math.sqrt(varianza)

object columnasNumericas extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/pi-movies-complete-2025-12-04.csv")

  def imprimirMetricas(nombre: String, datos: List[Double]): IO[Unit] =
    val avg  = Estadisticos.promedio(datos)
    val sum  = Estadisticos.sumaTotal(datos)
    val std  = Estadisticos.desviacionEstandar(datos)
    // Ajuste de formato para incluir Suma y Desviación (Std)
    IO.println(f"  > $nombre%-12s | Promedio: $avg%10.2f | Suma: $sum%15.2f | Desv. Est: $std%10.2f")

  val run: IO[Unit] =
    val lecturaCSV: IO[List[Movie]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Movie](';'))
      .compile
      .toList

    lecturaCSV.flatMap { peliculas =>
      IO.println("=" * 100)
      IO.println("              INFORME ACTUALIZADO DE MÉTRICAS")
      IO.println("=" * 100) >>
        imprimirMetricas("Budget", peliculas.map(_.budget)) >>
        imprimirMetricas("Revenue", peliculas.map(_.revenue)) >>
        imprimirMetricas("Popularity", peliculas.map(_.popularity)) >>
        imprimirMetricas("Runtime", peliculas.map(_.runtime)) >>
        imprimirMetricas("Vote Avg", peliculas.map(_.vote_average)) >>
        imprimirMetricas("Vote Count", peliculas.map(_.vote_count)) >>
        IO.println("=" * 100) >>
        IO.println(s"  Total registros procesados: ${peliculas.length}") >>
        IO.println("=" * 100)
    }