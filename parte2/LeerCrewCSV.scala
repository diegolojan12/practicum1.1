import scala.io.Source
import java.io.PrintWriter
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._

case class Crew(
                 credit_id: Option[String],
                 department: Option[String],
                 gender: Option[Int],
                 id: Option[Int],
                 job: Option[String],
                 name: Option[String],
                 profile_path: Option[String]
               )

case class FilaCSV(datos: Array[String], crewLimpia: List[Crew])

object LeerCrewCSV extends App {

  val ruta = "src/main/resources/data/pi-movies-complete-2026-01-08 (1).csv"
  val rutaSalida = "src/main/resources/data/pi-movies-complete-2026-01-08-limpio.csv"

  val source = Source.fromFile(ruta, "UTF-8")
  val lines = source.getLines().toList
  source.close()

  val headers = lines.head.split(";").map(_.trim)
  val crewIndex = headers.indexOf("crew")

  if (crewIndex == -1) {
    println("La columna 'crew' no existe en el CSV")
    sys.exit(1)
  }

  def cleanCrewJson(crewJson: String): String = {
    crewJson
      .trim
      .replaceAll("'", "\"")
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replaceAll("""\\""", "")
  }

  def normalizarTexto(texto: String): Option[String] = {
    val limpio = texto.trim.replaceAll("\\s+", " ")
    if (limpio.isEmpty) None else Some(limpio)
  }

  def limpiarCrew(crews: List[Crew]): List[Crew] = {
    crews
      .map(c => c.copy(
        credit_id = c.credit_id.flatMap(n => normalizarTexto(n)),
        name = c.name.flatMap(n => normalizarTexto(n)),
        department = c.department.flatMap(d => normalizarTexto(d)),
        job = c.job.flatMap(j => normalizarTexto(j)),
        profile_path = c.profile_path.flatMap(p => normalizarTexto(p))
      ))
      .distinct
  }

  // Decodificador personalizado de Circe para CSV
  implicit val csvDecoder: Decoder[Array[String]] = new Decoder[Array[String]] {
    final def apply(c: HCursor): Decoder.Result[Array[String]] = {
      Right(c.value.asString.map(parseCSVLine).getOrElse(Array.empty))
    }
  }

  def parseCSVLine(line: String): Array[String] = {
    val (fields, lastBuilder, _) = line.foldLeft(
      (Vector.empty[String], new StringBuilder, false)
    ) {
      case ((fields, current, inQuotes), char) => char match {
        case '"' =>
          (fields, current, !inQuotes)

        case ';' if !inQuotes =>
          (fields :+ current.toString, new StringBuilder, false)

        case _ =>
          current.append(char)
          (fields, current, inQuotes)
      }
    }

    (fields :+ lastBuilder.toString).toArray
  }

  def escaparCSV(texto: String): String = {
    if (texto.contains(";") || texto.contains("\"") || texto.contains("\n")) {
      "\"" + texto.replace("\"", "\"\"") + "\""
    } else {
      texto
    }
  }

  // Procesar cada fila manteniendo la estructura original
  val filasLimpias: List[FilaCSV] = lines.tail.map { line =>
    val parts = parseCSVLine(line)

    val crewLimpia = if (parts.length > crewIndex) {
      val crewStr = parts(crewIndex).trim

      if (crewStr.nonEmpty && crewStr != "[]") {
        try {
          val jsonLimpio = cleanCrewJson(crewStr)
          decode[List[Crew]](jsonLimpio) match {
            case Right(crews) => limpiarCrew(crews)
            case Left(_) => List.empty[Crew]
          }
        } catch {
          case _: Exception => List.empty[Crew]
        }
      } else {
        List.empty[Crew]
      }
    } else {
      List.empty[Crew]
    }

    FilaCSV(parts, crewLimpia)
  }

  // Exportar a nuevo CSV solo con la columna crew
  val writer = new PrintWriter(rutaSalida, "UTF-8")

  // Escribir encabezado
  writer.println("crew")

  // Escribir solo la columna crew limpia
  filasLimpias.foreach { fila =>
    val crewJson = fila.crewLimpia.asJson.noSpaces
    writer.println(escaparCSV(crewJson))
  }

  writer.close()

  // Estadísticas generales
  val totalFilas = filasLimpias.size
  val filasConCrew = filasLimpias.count(_.crewLimpia.nonEmpty)
  val totalCrewMembers = filasLimpias.map(_.crewLimpia.size).sum
  val todosCrew = filasLimpias.flatMap(_.crewLimpia)

  println("=" * 60)
  println("RESUMEN DE PROCESAMIENTO CON CIRCE")
  println("=" * 60)
  println(s"Total de filas procesadas: $totalFilas")
  println(s"Filas con crew: $filasConCrew")
  println(s"Total de miembros de crew: $totalCrewMembers")
  println(s"Promedio de crew por fila: ${if (filasConCrew > 0) totalCrewMembers.toDouble / filasConCrew else 0}")

  // Estadísticas de campos null
  val creditIdsNull = todosCrew.count(_.credit_id.isEmpty)
  val nombresNull = todosCrew.count(_.name.isEmpty)
  val deptosNull = todosCrew.count(_.department.isEmpty)
  val gendersNull = todosCrew.count(_.gender.isEmpty)
  val idsNull = todosCrew.count(_.id.isEmpty)
  val jobsNull = todosCrew.count(_.job.isEmpty)
  val profilesNull = todosCrew.count(_.profile_path.isEmpty)

  println(s"\nCampos con valores null:")
  println(s"  - Credit IDs null: $creditIdsNull")
  println(s"  - Nombres null: $nombresNull")
  println(s"  - Departamentos null: $deptosNull")
  println(s"  - Gender null: $gendersNull")
  println(s"  - IDs null: $idsNull")
  println(s"  - Jobs null: $jobsNull")
  println(s"  - Profile paths null: $profilesNull")

  // Distribución por género
  println("\n" + "=" * 60)
  println("DISTRIBUCIÓN POR GÉNERO")
  println("=" * 60)
  todosCrew
    .filter(_.gender.isDefined)
    .groupBy(_.gender.get)
    .map { case (gender, list) => (gender, list.size) }
    .toSeq
    .sortBy(_._1)
    .foreach { case (gender, count) =>
      val label = gender match {
        case 0 => "No especificado"
        case 1 => "Femenino"
        case 2 => "Masculino"
        case _ => s"Otro ($gender)"
      }
      println(f"  $label%-20s: $count%,6d registros")
    }

  // Estadísticas por departamento
  println("\n" + "=" * 60)
  println("ESTADÍSTICAS POR DEPARTAMENTO (TOP 10)")
  println("=" * 60)
  todosCrew
    .filter(_.department.isDefined)
    .groupBy(_.department.get)
    .map { case (dept, list) => (dept, list.size) }
    .toSeq
    .sortBy(-_._2)
    .take(10)
    .foreach { case (dept, count) =>
      println(f"  $dept%-30s: $count%,6d registros")
    }

  // Estadísticas por trabajo
  println("\n" + "=" * 60)
  println("TRABAJOS MÁS COMUNES (TOP 10)")
  println("=" * 60)
  todosCrew
    .filter(_.job.isDefined)
    .groupBy(_.job.get)
    .map { case (job, list) => (job, list.size) }
    .toSeq
    .sortBy(-_._2)
    .take(10)
    .foreach { case (job, count) =>
      println(f"  $job%-30s: $count%,6d registros")
    }

  // Muestra de filas con sus crews
  println("\n" + "=" * 60)
  println("EJEMPLO DE FILAS LIMPIAS (3 primeras con crew)")
  println("=" * 60)
  filasLimpias
    .filter(_.crewLimpia.nonEmpty)
    .take(3)
    .zipWithIndex
    .foreach { case (fila, idx) =>
      println(s"\nFila ${idx + 1}:")
      println(s"  Total de crew: ${fila.crewLimpia.size}")
      println(s"  Crew limpia:")
      println(fila.crewLimpia.asJson.spaces2)
    }

  println("\n" + "=" * 60)
  println("PROCESAMIENTO COMPLETADO ✓")
  println(s"Archivo guardado en: $rutaSalida")
  println("=" * 60)
}