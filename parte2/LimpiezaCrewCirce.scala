import scala.io.Source
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

object LeerCrewCSV extends App {

  val ruta = "src/main/resources/data/pi-movies-complete-2026-01-08 (1).csv"

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

  val listaCrew: List[Crew] = lines.tail.flatMap { line =>
    val parts = parseCSVLine(line)

    if (parts.length > crewIndex) {
      val crewStr = parts(crewIndex).trim

      if (crewStr.nonEmpty && crewStr != "[]") {
        try {
          val jsonLimpio = cleanCrewJson(crewStr)

          decode[List[Crew]](jsonLimpio) match {
            case Right(crews) => crews
            case Left(error) =>
              List.empty[Crew]
          }
        } catch {
          case e: Exception => List.empty[Crew]
        }
      } else {
        List.empty[Crew]
      }
    } else {
      List.empty[Crew]
    }
  }

  val crewLimpio = limpiarCrew(listaCrew)

  // Contar campos null
  val creditIdsNull = crewLimpio.count(_.credit_id.isEmpty)
  val nombresNull = crewLimpio.count(_.name.isEmpty)
  val deptosNull = crewLimpio.count(_.department.isEmpty)
  val gendersNull = crewLimpio.count(_.gender.isEmpty)
  val idsNull = crewLimpio.count(_.id.isEmpty)
  val jobsNull = crewLimpio.count(_.job.isEmpty)
  val profilesNull = crewLimpio.count(_.profile_path.isEmpty)

  println("=" * 60)
  println("RESUMEN DE PROCESAMIENTO CON CIRCE")
  println("=" * 60)
  println(s"Total de registros Crew procesados: ${crewLimpio.size}")
  println(s"\nCampos con valores null:")
  println(s"  - Credit IDs null: $creditIdsNull")
  println(s"  - Nombres null: $nombresNull")
  println(s"  - Departamentos null: $deptosNull")
  println(s"  - Gender null: $gendersNull")
  println(s"  - IDs null: $idsNull")
  println(s"  - Jobs null: $jobsNull")
  println(s"  - Profile paths null: $profilesNull")

  println("\n" + "=" * 60)
  println("PRIMEROS 5 REGISTROS (incluyendo nulls)")
  println("=" * 60)
  crewLimpio.take(5).foreach { crew =>
    println("\nMiembro del equipo:")
    println(s"  Credit ID: ${crew.credit_id.getOrElse("null")}")
    println(s"  Nombre: ${crew.name.getOrElse("null")}")
    println(s"  Departamento: ${crew.department.getOrElse("null")}")
    println(s"  Gender: ${crew.gender.map(_.toString).getOrElse("null")}")
    println(s"  ID: ${crew.id.map(_.toString).getOrElse("null")}")
    println(s"  Trabajo: ${crew.job.getOrElse("null")}")
    println(s"  Perfil: ${crew.profile_path.getOrElse("null")}")
  }

  // Estadísticas por género
  println("\n" + "=" * 60)
  println("DISTRIBUCIÓN POR GÉNERO")
  println("=" * 60)
  crewLimpio
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
  crewLimpio
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
  crewLimpio
    .filter(_.job.isDefined)
    .groupBy(_.job.get)
    .map { case (job, list) => (job, list.size) }
    .toSeq
    .sortBy(-_._2)
    .take(10)
    .foreach { case (job, count) =>
      println(f"  $job%-30s: $count%,6d registros")
    }

  // Exportar JSON limpio usando Circe
  println("\n" + "=" * 60)
  println("JSON LIMPIO CON NULLS (Muestra de 3 registros)")
  println("=" * 60)
  val muestra = crewLimpio.take(3)
  println(muestra.asJson.spaces2)

  // Ejemplo con registro completo
  println("\n--- Ejemplo de registro completo ---")
  val ejemploCompleto = Crew(
    credit_id = Some("53f5e242c3a36833f7003a15"),
    department = Some("Directing"),
    gender = Some(0),
    id = Some(40016),
    job = Some("Director"),
    name = Some("Angelina Maccarone"),
    profile_path = None
  )
  println(ejemploCompleto.asJson.spaces2)

  println("\n" + "=" * 60)
  println("PROCESAMIENTO COMPLETADO ✓")
  println("=" * 60)
}