import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// Definición de los datos crudos del CSV (24 columnas originales)
case class MovieRaw(
                     adult: String,
                     belongs_to_collection: String,
                     budget: Double,
                     genres: String,
                     homepage: String,
                     id: Double,
                     imdb_id: String,
                     original_language: String,
                     original_title: String,
                     overview: String,
                     popularity: Double,
                     poster_path: String,
                     production_companies: String,
                     production_countries: String,
                     release_date: String,
                     revenue: Double,
                     runtime: Double,
                     spoken_languages: String,
                     status: String,
                     tagline: String,
                     title: String,
                     video: String,
                     vote_average: Double,
                     vote_count: Double
                   )

// Modelo procesado con columnas calculadas (28 atributos)
case class Movie(
                  adult: String,
                  belongs_to_collection: String,
                  budget: Double,
                  genres: String,
                  homepage: String,
                  id: Double,
                  imdb_id: String,
                  original_language: String,
                  original_title: String,
                  overview: String,
                  popularity: Double,
                  poster_path: String,
                  production_companies: String,
                  production_countries: String,
                  release_date: String,
                  revenue: Double,
                  runtime: Double,
                  spoken_languages: String,
                  status: String,
                  tagline: String,
                  title: String,
                  video: String,
                  vote_average: Double,
                  vote_count: Double,
                  release_year: Double,
                  release_month: Double,
                  release_day: Double,
                  `return`: Double
                )

given CsvRowDecoder[MovieRaw, String] = deriveCsvRowDecoder[MovieRaw]
given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]

object Transformador:
  // Extraer año, mes y día de release_date
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

  // Calcular ROI (Return on Investment)
  def calcularReturn(budget: Double, revenue: Double): Double =
    if (budget > 0) (revenue - budget) / budget else 0.0

  // Convertir MovieRaw a Movie con columnas calculadas
  def procesarMovie(raw: MovieRaw): Movie =
    val (year, month, day) = parsearFecha(raw.release_date)
    val roi = calcularReturn(raw.budget, raw.revenue)

    Movie(
      adult = raw.adult,
      belongs_to_collection = raw.belongs_to_collection,
      budget = raw.budget,
      genres = raw.genres,
      homepage = raw.homepage,
      id = raw.id,
      imdb_id = raw.imdb_id,
      original_language = raw.original_language,
      original_title = raw.original_title,
      overview = raw.overview,
      popularity = raw.popularity,
      poster_path = raw.poster_path,
      production_companies = raw.production_companies,
      production_countries = raw.production_countries,
      release_date = raw.release_date,
      revenue = raw.revenue,
      runtime = raw.runtime,
      spoken_languages = raw.spoken_languages,
      status = raw.status,
      tagline = raw.tagline,
      title = raw.title,
      video = raw.video,
      vote_average = raw.vote_average,
      vote_count = raw.vote_count,
      release_year = year,
      release_month = month,
      release_day = day,
      `return` = roi
    )

// Estadísticas de calidad de datos por columna
case class CalidadColumna(
                           columna: String,
                           total: Int,
                           nulos: Int,
                           ceros: Int,
                           negativos: Int,
                           vacios: Int,
                           porcentajeValidos: Double
                         )

object AnalizadorCalidad:
  def analizarCalidadNumerica(nombre: String, datos: List[Double], total: Int): CalidadColumna =
    val nulos = datos.count(d => d.isNaN || d.isInfinite)
    val ceros = datos.count(_ == 0.0)
    val negativos = datos.count(_ < 0.0)
    val validos = total - nulos - ceros - negativos

    CalidadColumna(
      columna = nombre,
      total = total,
      nulos = nulos,
      ceros = ceros,
      negativos = negativos,
      vacios = 0,
      porcentajeValidos = if (total > 0) (validos.toDouble / total * 100) else 0.0
    )

  def analizarCalidadTexto(nombre: String, datos: List[String], total: Int): CalidadColumna =
    val vacios = datos.count(s => s == null || s.trim.isEmpty)
    val validos = total - vacios

    CalidadColumna(
      columna = nombre,
      total = total,
      nulos = 0,
      ceros = 0,
      negativos = 0,
      vacios = vacios,
      porcentajeValidos = if (total > 0) (validos.toDouble / total * 100) else 0.0
    )

object DetectorOutliers:
  // Cálculo de cuartiles con interpolación
  def calcularCuartil(ordenados: List[Double], percentil: Double): Double =
    if (ordenados.isEmpty) return 0.0
    val pos = percentil * (ordenados.size - 1)
    val lower = ordenados(pos.toInt)
    val upper = if (pos.toInt + 1 < ordenados.size) ordenados(pos.toInt + 1) else lower
    val fraction = pos - pos.toInt
    lower + fraction * (upper - lower)

  // Detección de outliers usando método IQR
  def detectarOutliersIQR(datos: List[Double]): (Double, Double, Int, Int) =
    if (datos.size < 4) return (0.0, 0.0, 0, 0)

    val ordenados = datos.sorted
    val q1 = calcularCuartil(ordenados, 0.25)
    val q3 = calcularCuartil(ordenados, 0.75)
    val iqr = q3 - q1

    val limiteInferior = math.max(0, q1 - 1.5 * iqr)
    val limiteSuperior = q3 + 1.5 * iqr

    val outliersInferiores = datos.count(d => d < limiteInferior)
    val outliersSuperiores = datos.count(d => d > limiteSuperior)

    (limiteInferior, limiteSuperior, outliersInferiores, outliersSuperiores)

  // Detección de outliers usando Z-score
  def detectarOutliersZScore(datos: List[Double], umbral: Double = 3.0): Int =
    if (datos.isEmpty) return 0

    val media = datos.sum / datos.size
    val varianza = datos.map(x => math.pow(x - media, 2)).sum / datos.size
    val desviacion = math.sqrt(varianza)

    if (desviacion == 0.0) 0
    else datos.count(d => math.abs((d - media) / desviacion) > umbral)

object Limpiador:
  // Paso 1: Eliminar valores nulos y ceros en columnas críticas
  def eliminarValoresNulos(peliculas: List[Movie]): List[Movie] =
    peliculas.filter { m =>
      m.id > 0 &&
        m.budget > 0 &&
        m.revenue > 0 &&
        m.runtime > 0 &&
        m.popularity > 0 &&
        m.vote_count > 0 &&
        !m.title.trim.isEmpty &&
        !m.original_title.trim.isEmpty
    }

  // Paso 2: Validar rangos lógicos
  def validarRangos(peliculas: List[Movie]): List[Movie] =
    peliculas.filter { m =>
      m.release_year >= 1888 && m.release_year <= 2025 &&
        m.release_month >= 1 && m.release_month <= 12 &&
        m.release_day >= 1 && m.release_day <= 31 &&
        m.runtime < 500 && // Películas extremadamente largas
        m.vote_average >= 0 && m.vote_average <= 10 &&
        m.`return` >= -1.0 // ROI no puede ser menor a -100%
    }

  // Paso 3: Filtrar outliers usando IQR
  def filtrarOutliersIQR(peliculas: List[Movie]): List[Movie] =
    if (peliculas.isEmpty) return Nil

    val (bLow, bHigh) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.budget))._1 ->
      DetectorOutliers.detectarOutliersIQR(peliculas.map(_.budget))._2
    val (rLow, rHigh) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.revenue))._1 ->
      DetectorOutliers.detectarOutliersIQR(peliculas.map(_.revenue))._2
    val (pLow, pHigh) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.popularity))._1 ->
      DetectorOutliers.detectarOutliersIQR(peliculas.map(_.popularity))._2

    peliculas.filter { m =>
      m.budget >= bLow && m.budget <= bHigh &&
        m.revenue >= rLow && m.revenue <= rHigh &&
        m.popularity >= pLow && m.popularity <= pHigh
    }

  // Método flexible: permite 1 outlier por registro
  def filtrarOutliersFlexible(peliculas: List[Movie]): List[Movie] =
    if (peliculas.isEmpty) return Nil

    val (bLow, bHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.budget))
    val (rLow, rHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.revenue))
    val (pLow, pHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.popularity))
    val (retLow, retHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.`return`))

    peliculas.filter { m =>
      val fueraDeRango = Seq(
        m.budget < bLow || m.budget > bHigh,
        m.revenue < rLow || m.revenue > rHigh,
        m.popularity < pLow || m.popularity > pHigh,
        m.`return` < retLow || m.`return` > retHigh
      ).count(identity)

      fueraDeRango < 2 // Permite 1 outlier
    }

object Estadisticos:
  def calcularEstadisticas(datos: List[Double]): Map[String, Double] =
    if (datos.isEmpty) return Map.empty

    val ordenados = datos.sorted
    val n = ordenados.size
    val media = datos.sum / n
    val varianza = datos.map(x => math.pow(x - media, 2)).sum / n
    val mediana = if (n % 2 == 1) ordenados(n / 2)
    else (ordenados(n / 2 - 1) + ordenados(n / 2)) / 2.0

    Map(
      "min" -> ordenados.head,
      "max" -> ordenados.last,
      "media" -> media,
      "mediana" -> mediana,
      "desv_std" -> math.sqrt(varianza),
      "q1" -> DetectorOutliers.calcularCuartil(ordenados, 0.25),
      "q3" -> DetectorOutliers.calcularCuartil(ordenados, 0.75)
    )

object LimpiezaDatos extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/pi_movies_complete.csv")

  def run: IO[Unit] =
    val lecturaCSV: IO[List[Movie]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .map(Transformador.procesarMovie) // Transformar a Movie con columnas calculadas
      .compile
      .toList
      .handleErrorWith { e =>
        IO.println(s"Error al leer el CSV: ${e.getMessage}") >> IO.pure(Nil)
      }

    lecturaCSV.flatMap { peliculasOriginales =>
      val total = peliculasOriginales.length

      // ============================================
      // ANÁLISIS DE CALIDAD DE DATOS
      // ============================================
      val calidadBudget = AnalizadorCalidad.analizarCalidadNumerica(
        "budget", peliculasOriginales.map(_.budget), total
      )
      val calidadRevenue = AnalizadorCalidad.analizarCalidadNumerica(
        "revenue", peliculasOriginales.map(_.revenue), total
      )
      val calidadPopularity = AnalizadorCalidad.analizarCalidadNumerica(
        "popularity", peliculasOriginales.map(_.popularity), total
      )
      val calidadTitle = AnalizadorCalidad.analizarCalidadTexto(
        "title", peliculasOriginales.map(_.title), total
      )

      // ============================================
      // DETECCIÓN DE OUTLIERS
      // ============================================
      val (bLow, bHigh, bOutInf, bOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.budget)
      )
      val (rLow, rHigh, rOutInf, rOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.revenue)
      )
      val (pLow, pHigh, pOutInf, pOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.popularity)
      )

      val budgetZScore = DetectorOutliers.detectarOutliersZScore(
        peliculasOriginales.map(_.budget)
      )
      val revenueZScore = DetectorOutliers.detectarOutliersZScore(
        peliculasOriginales.map(_.revenue)
      )

      // ============================================
      // PROCESO DE LIMPIEZA POR ETAPAS
      // ============================================
      val etapa1 = Limpiador.eliminarValoresNulos(peliculasOriginales)
      val etapa2 = Limpiador.validarRangos(etapa1)
      val etapa3Estricta = Limpiador.filtrarOutliersIQR(etapa2)
      val etapa3Flexible = Limpiador.filtrarOutliersFlexible(etapa2)

      // Estadísticas finales
      val statsBudget = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.budget))
      val statsRevenue = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.revenue))
      val statsPopularity = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.popularity))

      // ============================================
      // REPORTE COMPLETO
      // ============================================
      IO.println("=" * 90) >>
        IO.println("              REPORTE DE LIMPIEZA DE DATOS - DATASET DE PELÍCULAS") >>
        IO.println("=" * 90) >>
        IO.println("") >>
        IO.println("1. ANÁLISIS DE CALIDAD DE DATOS (Valores Nulos, Ceros y Vacíos)") >>
        IO.println("-" * 90) >>
        IO.println(f"Columna           Total    Nulos    Ceros    Negativos  Vacíos   %% Válidos") >>
        IO.println(f"${calidadBudget.columna}%-15s ${calidadBudget.total}%,7d  ${calidadBudget.nulos}%,7d  ${calidadBudget.ceros}%,7d  ${calidadBudget.negativos}%,10d  ${calidadBudget.vacios}%,7d  ${calidadBudget.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadRevenue.columna}%-15s ${calidadRevenue.total}%,7d  ${calidadRevenue.nulos}%,7d  ${calidadRevenue.ceros}%,7d  ${calidadRevenue.negativos}%,10d  ${calidadRevenue.vacios}%,7d  ${calidadRevenue.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadPopularity.columna}%-15s ${calidadPopularity.total}%,7d  ${calidadPopularity.nulos}%,7d  ${calidadPopularity.ceros}%,7d  ${calidadPopularity.negativos}%,10d  ${calidadPopularity.vacios}%,7d  ${calidadPopularity.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadTitle.columna}%-15s ${calidadTitle.total}%,7d  ${calidadTitle.nulos}%,7d  ${calidadTitle.ceros}%,7d  ${calidadTitle.negativos}%,10d  ${calidadTitle.vacios}%,7d  ${calidadTitle.porcentajeValidos}%6.2f%%") >>
        IO.println("") >>
        IO.println("2. DETECCIÓN DE VALORES ATÍPICOS (OUTLIERS)") >>
        IO.println("-" * 90) >>
        IO.println("Método IQR (Rango Intercuartílico):") >>
        IO.println(f"  Budget:") >>
        IO.println(f"    Límites:       [$bLow%,.2f - $bHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${bOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${bOutSup}%,d registros") >>
        IO.println(f"    Total:         ${bOutInf + bOutSup}%,d (${(bOutInf + bOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println(f"  Revenue:") >>
        IO.println(f"    Límites:       [$rLow%,.2f - $rHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${rOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${rOutSup}%,d registros") >>
        IO.println(f"    Total:         ${rOutInf + rOutSup}%,d (${(rOutInf + rOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println(f"  Popularity:") >>
        IO.println(f"    Límites:       [$pLow%,.2f - $pHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${pOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${pOutSup}%,d registros") >>
        IO.println(f"    Total:         ${pOutInf + pOutSup}%,d (${(pOutInf + pOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("Método Z-Score (|z| > 3):") >>
        IO.println(f"  Budget:        ${budgetZScore}%,d outliers (${budgetZScore.toDouble / total * 100}%.2f%%)") >>
        IO.println(f"  Revenue:       ${revenueZScore}%,d outliers (${revenueZScore.toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("3. PROCESO DE LIMPIEZA POR ETAPAS") >>
        IO.println("-" * 90) >>
        IO.println(f"Registros originales:                    ${peliculasOriginales.length}%,7d") >>
        IO.println(f"Después de eliminar nulos/ceros:         ${etapa1.length}%,7d (${(total - etapa1.length)}%,d eliminados)") >>
        IO.println(f"Después de validar rangos:               ${etapa2.length}%,7d (${(etapa1.length - etapa2.length)}%,d eliminados)") >>
        IO.println(f"Después de filtrar outliers (estricto):  ${etapa3Estricta.length}%,7d (${(etapa2.length - etapa3Estricta.length)}%,d eliminados)") >>
        IO.println(f"Después de filtrar outliers (flexible):  ${etapa3Flexible.length}%,7d (${(etapa2.length - etapa3Flexible.length)}%,d eliminados)") >>
        IO.println("") >>
        IO.println(f"Porcentaje de datos conservados:         ${etapa3Flexible.length.toDouble / total * 100}%.2f%%") >>
        IO.println(f"Porcentaje de datos eliminados:          ${(total - etapa3Flexible.length).toDouble / total * 100}%.2f%%") >>
        IO.println("") >>
        IO.println("4. ESTADÍSTICAS DESCRIPTIVAS (DATOS LIMPIOS - Método Flexible)") >>
        IO.println("-" * 90) >>
        IO.println(f"Budget:") >>
        IO.println(f"  Mínimo:       ${statsBudget("min")}%,.2f") >>
      IO.println(f"  Q1:            ${statsBudget("q1")}%,.2f") >>
      IO.println(f"  Mediana:       ${statsBudget("mediana")}%,.2f") >>
      IO.println(f"  Media:         ${statsBudget("media")}%,.2f") >>
      IO.println(f"  Q3:            ${statsBudget("q3")}%,.2f") >>
      IO.println(f"  Máximo:        ${statsBudget("max")}%,.2f") >>
      IO.println(f"  Desv. Est.:    ${statsBudget("desv_std")}%,.2f") >>
      IO.println("") >>
        IO.println(f"Revenue:") >>
        IO.println(f"  Mínimo:        ${statsRevenue("min")}%,.2f") >>
      IO.println(f"  Mediana:       ${statsRevenue("mediana")}%,.2f") >>
      IO.println(f"  Media:         ${statsRevenue("media")}%,.2f") >>
      IO.println(f"  Máximo:        ${statsRevenue("max")}%,.2f") >>
      IO.println(f"  Desv. Est.:    ${statsRevenue("desv_std")}%,.2f") >>
      IO.println("") >>
        IO.println(f"Popularity:") >>
        IO.println(f"  Mínimo:        ${statsPopularity("min")}%,.2f") >>
        IO.println(f"  Mediana:       ${statsPopularity("mediana")}%,.2f") >>
        IO.println(f"  Media:         ${statsPopularity("media")}%,.2f") >>
        IO.println(f"  Máximo:        ${statsPopularity("max")}%,.2f") >>
        IO.println(f"  Desv. Est.:    ${statsPopularity("desv_std")}%,.2f") >>
        IO.println("") >>
        IO.println("=" * 90) >>
        IO.println("✓ Limpieza completada exitosamente") >>
        IO.println("✓ Dataset listo para análisis exploratorio") >>
        IO.println("=" * 90)
    }
