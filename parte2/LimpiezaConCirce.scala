import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import io.circe.*
import io.circe.parser.*
import io.circe.generic.auto.*

// ========================================
// MODELOS DE DATOS JSON
// ========================================

case class Genre(id: Int, name: String)
case class ProductionCompany(id: Int, name: String)
case class ProductionCountry(iso_3166_1: String, name: String)
case class SpokenLanguage(iso_639_1: String, name: String)
case class CrewMember(
                       credit_id: String,
                       department: String,
                       gender: Int,
                       id: Int,
                       job: String,
                       name: String,
                       profile_path: Option[String]
                     )

// ========================================
// MODELO DE DATOS CRUDO DEL CSV
// ========================================

case class MovieRaw(
                     adult: String,
                     belongs_to_collection: String,
                     budget: Double,
                     genres: String,                    // JSON
                     homepage: String,
                     id: Double,
                     imdb_id: String,
                     original_language: String,
                     original_title: String,
                     overview: String,
                     popularity: Double,
                     poster_path: String,
                     production_companies: String,      // JSON
                     production_countries: String,      // JSON
                     release_date: String,
                     revenue: Double,
                     runtime: Double,
                     spoken_languages: String,          // JSON
                     status: String,
                     tagline: String,
                     title: String,
                     video: String,
                     vote_average: Double,
                     vote_count: Double,
                     crew: String                       // JSON - NUEVA COLUMNA
                   )

// ========================================
// MODELO DE DATOS PROCESADO
// ========================================

case class Movie(
                  adult: String,
                  belongs_to_collection: String,
                  budget: Double,
                  genres: List[String],              // Extraído de JSON
                  homepage: String,
                  id: Double,
                  imdb_id: String,
                  original_language: String,
                  original_title: String,
                  overview: String,
                  popularity: Double,
                  poster_path: String,
                  production_companies: List[String], // Extraído de JSON
                  production_countries: List[String], // Extraído de JSON
                  release_date: String,
                  revenue: Double,
                  runtime: Double,
                  spoken_languages: List[String],    // Extraído de JSON
                  status: String,
                  tagline: String,
                  title: String,
                  video: String,
                  vote_average: Double,
                  vote_count: Double,
                  release_year: Double,
                  release_month: Double,
                  release_day: Double,
                  `return`: Double,
                  // Nuevos campos extraídos de crew
                  director: Option[String],
                  producers: List[String],
                  writers: List[String],
                  crew_size: Int
                )

given CsvRowDecoder[MovieRaw, String] = deriveCsvRowDecoder[MovieRaw]

// ========================================
// UTILIDADES PARA CIRCE
// ========================================

object CirceUtils:

// Parsear JSON de géneros
def parseGenres(jsonStr: String): List[String] =
  parse(jsonStr).flatMap(_.as[List[Genre]]) match
case Right(genres) => genres.map(_.name)
case Left(_) => List.empty

// Parsear JSON de compañías de producción
def parseProductionCompanies(jsonStr: String): List[String] =
  parse(jsonStr).flatMap(_.as[List[ProductionCompany]]) match
case Right(companies) => companies.map(_.name)
case Left(_) => List.empty

// Parsear JSON de países de producción
def parseProductionCountries(jsonStr: String): List[String] =
  parse(jsonStr).flatMap(_.as[List[ProductionCountry]]) match
case Right(countries) => countries.map(_.name)
case Left(_) => List.empty

// Parsear JSON de idiomas hablados
def parseSpokenLanguages(jsonStr: String): List[String] =
  parse(jsonStr).flatMap(_.as[List[SpokenLanguage]]) match
case Right(languages) => languages.map(_.name)
case Left(_) => List.empty

// Parsear JSON de crew y extraer información clave
def parseCrew(jsonStr: String): (Option[String], List[String], List[String], Int) =
  parse(jsonStr).flatMap(_.as[List[CrewMember]]) match
case Right(crew) =>
val director = crew.find(_.job == "Director").map(_.name)
val producers = crew.filter(_.department == "Production").map(_.name)
val writers = crew.filter(_.department == "Writing").map(_.name)
(director, producers, writers, crew.size)
case Left(_) =>
(None, List.empty, List.empty, 0)

// ========================================
// TRANSFORMADOR DE DATOS
// ========================================

object Transformador:

def parsearFecha(fecha: String): (Double, Double, Double) =
  try {
    val partes = fecha.split("-")
    if (partes.length == 3) {
      val year = partes(0).toDouble
      val month = partes(1).toDouble
      val day = partes(2).toDouble
      (year, month, day)
    } else (0.0, 0.0, 0.0)
  } catch {
    case _: Exception => (0.0, 0.0, 0.0)
  }

def calcularReturn(budget: Double, revenue: Double): Double =
  if (budget > 0) (revenue - budget) / budget else 0.0

def procesarMovie(raw: MovieRaw): Movie =
val (year, month, day) = parsearFecha(raw.release_date)
val roi = calcularReturn(raw.budget, raw.revenue)

// Parsear todos los campos JSON usando Circe
val genres = CirceUtils.parseGenres(raw.genres)
val companies = CirceUtils.parseProductionCompanies(raw.production_companies)
val countries = CirceUtils.parseProductionCountries(raw.production_countries)
val languages = CirceUtils.parseSpokenLanguages(raw.spoken_languages)
val (director, producers, writers, crewSize) = CirceUtils.parseCrew(raw.crew)

Movie(
  adult = raw.adult,
  belongs_to_collection = raw.belongs_to_collection,
  budget = raw.budget,
  genres = genres,
  homepage = raw.homepage,
  id = raw.id,
  imdb_id = raw.imdb_id,
  original_language = raw.original_language,
  original_title = raw.original_title,
  overview = raw.overview,
  popularity = raw.popularity,
  poster_path = raw.poster_path,
  production_companies = companies,
  production_countries = countries,
  release_date = raw.release_date,
  revenue = raw.revenue,
  runtime = raw.runtime,
  spoken_languages = languages,
  status = raw.status,
  tagline = raw.tagline,
  title = raw.title,
  video = raw.video,
  vote_average = raw.vote_average,
  vote_count = raw.vote_count,
  release_year = year,
  release_month = month,
  release_day = day,
  `return` = roi,
  director = director,
  producers = producers,
  writers = writers,
  crew_size = crewSize
)

// ========================================
// LIMPIADOR DE DATOS
// ========================================

object Limpiador:

def eliminarValoresNulos(peliculas: List[Movie]): List[Movie] =
  peliculas.filter { m =>
    m.id > 0 &&
      m.budget > 0 &&
      m.revenue > 0 &&
      m.runtime > 0 &&
      m.popularity > 0 &&
      m.vote_count > 0 &&
      !m.title.trim.isEmpty &&
      !m.original_title.trim.isEmpty &&
      m.genres.nonEmpty &&  // Validar que tenga géneros
      m.crew_size > 0       // Validar que tenga crew
  }

def validarRangos(peliculas: List[Movie]): List[Movie] =
  peliculas.filter { m =>
    m.release_year >= 1888 && m.release_year <= 2025 &&
      m.release_month >= 1 && m.release_month <= 12 &&
      m.release_day >= 1 && m.release_day <= 31 &&
      m.runtime > 0 && m.runtime < 500 &&
      m.vote_average >= 0 && m.vote_average <= 10 &&
      m.`return` >= -1.0 &&
      m.crew_size > 0 && m.crew_size < 1000 // Límite razonable de crew
  }

def filtrarOutliersIQR(peliculas: List[Movie]): List[Movie] =
  if (peliculas.isEmpty) return Nil

def detectarOutliers(datos: List[Double]): (Double, Double) =
  if (datos.size < 4) return (0.0, Double.MaxValue)
val ordenados = datos.sorted
val q1 = ordenados((ordenados.size * 0.25).toInt)
val q3 = ordenados((ordenados.size * 0.75).toInt)
val iqr = q3 - q1
(math.max(0, q1 - 1.5 * iqr), q3 + 1.5 * iqr)

val (bLow, bHigh) = detectarOutliers(peliculas.map(_.budget))
val (rLow, rHigh) = detectarOutliers(peliculas.map(_.revenue))
val (pLow, pHigh) = detectarOutliers(peliculas.map(_.popularity))

peliculas.filter { m =>
  m.budget >= bLow && m.budget <= bHigh &&
    m.revenue >= rLow && m.revenue <= rHigh &&
    m.popularity >= pLow && m.popularity <= pHigh
}

// ========================================
// ANALIZADOR DE CREW
// ========================================

object AnalizadorCrew:

def analizarDirectores(peliculas: List[Movie]): Map[String, Int] =
  peliculas
    .flatMap(_.director)
    .groupBy(identity)
    .view.mapValues(_.size)
    .toMap
    .toList
    .sortBy(-_._2)
    .take(10)
    .toMap

def analizarProductores(peliculas: List[Movie]): Map[String, Int] =
  peliculas
    .flatMap(_.producers)
    .groupBy(identity)
    .view.mapValues(_.size)
    .toMap
    .toList
    .sortBy(-_._2)
    .take(10)
    .toMap

def estadisticasCrew(peliculas: List[Movie]): (Double, Double, Int, Int) =
val sizes = peliculas.map(_.crew_size.toDouble)
val promedio = sizes.sum / sizes.size
val ordenados = sizes.sorted
val mediana = ordenados(sizes.size / 2)
val min = ordenados.head.toInt
val max = ordenados.last.toInt
(promedio, mediana, min, max)

// ========================================
// APLICACIÓN PRINCIPAL
// ========================================

object LimpiezaConCirce extends IOApp.Simple:
val filePath = Path("src/main/resources/data/pi_movies_complete.csv")

def run: IO[Unit] =
val lecturaCSV: IO[List[Movie]] = Files[IO]
  .readAll(filePath)
  .through(text.utf8.decode)
  .through(decodeUsingHeaders[MovieRaw](';'))
  .map(Transformador.procesarMovie)
  .compile
  .toList
  .handleErrorWith { e =>
    IO.println(s"Error al leer el CSV: ${e.getMessage}") >> IO.pure(Nil)
  }

lecturaCSV.flatMap { peliculasOriginales =>
  val total = peliculasOriginales.length

  // Proceso de limpieza
  val etapa1 = Limpiador.eliminarValoresNulos(peliculasOriginales)
  val etapa2 = Limpiador.validarRangos(etapa1)
  val etapa3 = Limpiador.filtrarOutliersIQR(etapa2)

  // Análisis de crew
  val topDirectores = AnalizadorCrew.analizarDirectores(etapa3)
  val topProductores = AnalizadorCrew.analizarProductores(etapa3)
  val (avgCrew, medCrew, minCrew, maxCrew) = AnalizadorCrew.estadisticasCrew(etapa3)

  // Reporte
  IO.println("=" * 90) >>
    IO.println("     REPORTE DE LIMPIEZA CON ANÁLISIS JSON (CIRCE)") >>
    IO.println("=" * 90) >>
    IO.println("") >>
    IO.println("1. PROCESO DE LIMPIEZA") >>
    IO.println("-" * 90) >>
    IO.println(f"Registros originales:                ${total}%,7d") >>
    IO.println(f"Después de eliminar nulos:           ${etapa1.length}%,7d (${total - etapa1.length}%,d eliminados)") >>
    IO.println(f"Después de validar rangos:           ${etapa2.length}%,7d (${etapa1.length - etapa2.length}%,d eliminados)") >>
    IO.println(f"Después de filtrar outliers:         ${etapa3.length}%,7d (${etapa2.length - etapa3.length}%,d eliminados)") >>
    IO.println(f"Conservados:                         ${etapa3.length.toDouble / total * 100}%.2f%%") >>
    IO.println("") >>
    IO.println("2. ANÁLISIS DE CREW (JSON)") >>
    IO.println("-" * 90) >>
    IO.println(f"Tamaño promedio de crew:             ${avgCrew}%.2f personas") >>
    IO.println(f"Mediana de crew:                     ${medCrew}%.0f personas") >>
    IO.println(f"Rango: ${minCrew} - ${maxCrew} personas") >>
    IO.println("") >>
    IO.println("Top 10 Directores:") >>
    topDirectores.toList.sortBy(-_._2).zipWithIndex.foldLeft(IO.unit) { case (acc, ((nombre, count), i)) =>
      acc >> IO.println(f"  ${i + 1}%2d. ${nombre}%-40s ${count}%3d películas")
    } >>
    IO.println("") >>
    IO.println("Top 10 Productores:") >>
    topProductores.toList.sortBy(-_._2).zipWithIndex.foldLeft(IO.unit) { case (acc, ((nombre, count), i)) =>
      acc >> IO.println(f"  ${i + 1}%2d. ${nombre}%-40s ${count}%3d películas")
    } >>
    IO.println("") >>
    IO.println("=" * 90) >>
    IO.println("✓ Limpieza completada con análisis JSON usando Circe") >>
    IO.println("=" * 90)
}