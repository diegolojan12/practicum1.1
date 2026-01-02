# Análisis de Películas con Circe y Scala

##  Descripción del Proyecto

Este proyecto realiza un análisis completo de un dataset de películas, implementando limpieza de datos y procesamiento de columnas JSON usando la librería **Circe** en Scala. El proyecto demuestra el manejo avanzado de datos estructurados y semi-estructurados.

##  Objetivos

1. Aprender el uso de la librería **Circe** para manejo de JSON en Scala
2. Procesar columnas JSON del dataset (genres, crew, production_companies, etc.)
3. Implementar solución específica para el análisis de la columna **crew**
4. Realizar limpieza completa de datos
5. Generar reportes estadísticos detallados

##  Librería Circe

### ¿Qué es Circe?

Circe es una librería funcional para procesamiento de JSON en Scala. Ofrece:

- **Type-safe**: Validación de tipos en tiempo de compilación
- **Funcional**: Compatible con cats y cats-effect
- **Eficiente**: Alto rendimiento con mínima reflexión
- **Flexible**: Soporta decodificación automática y manual

### Características Principales

```scala
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._
```

**Ventajas:**
- Decodificación automática con `generic.auto._`
- Manejo funcional de errores con `Either`
- Soporte para JSON anidado complejo
- Cursores para navegación de JSON

##  Estructura del Proyecto

```
src/main/scala/
├── EjemploCirce.scala              # Ejemplos básicos de Circe
├── LimpiezaConCirce.scala          # Limpieza de datos con JSON
├── AnalisisColumnasJSON.scala      # Análisis detallado de columnas JSON
└── columnasNumericas.scala         # Análisis de columnas numéricas
```

##  Dataset

**Archivo:** `pi_movies_complete.csv`

### Columnas Principales (28 atributos)

**Columnas Originales (24):**
- `id`, `title`, `original_title`
- `budget`, `revenue`, `runtime`
- `vote_average`, `vote_count`, `popularity`
- `release_date`, `status`
- **Columnas JSON:**
  - `genres` - Lista de géneros
  - `production_companies` - Compañías productoras
  - `production_countries` - Países de producción
  - `spoken_languages` - Idiomas hablados
  - `crew` - Personal de producción (directores, productores, etc.)

**Columnas Calculadas (4):**
- `release_year`, `release_month`, `release_day`
- `return` - ROI calculado como `(revenue - budget) / budget`

## Implementación

### 1. Ejemplos Básicos de Circe (`EjemploCirce.scala`)

Archivo didáctico con 6 ejemplos progresivos:

```scala
// Ejemplo 1: Parsear JSON simple
val json = """{"nombre": "Ana", "edad": 28}"""
parse(json).flatMap(_.as[Persona])

// Ejemplo 2: Convertir objeto a JSON
val pelicula = Pelicula("Inception", 2010, "Nolan", List("Sci-Fi"))
val json = pelicula.asJson

// Ejemplo 3: Arrays JSON
parse(jsonArray).flatMap(_.as[List[Pelicula]])

// Ejemplo 4: JSON anidado
case class Actor(nombre: String, personaje: String)
case class PeliculaCompleta(titulo: String, actores: List[Actor])

// Ejemplo 5: Manejo de errores
resultado match {
  case Right(data) => // Éxito
  case Left(error) => // Error
}

// Ejemplo 6: Navegación con cursores
json.hcursor
  .downField("pelicula")
  .downField("info")
  .get[String]("titulo")
```

### 2. Modelos de Datos JSON

```scala
// Géneros
case class Genre(id: Int, name: String)

// Compañías de producción
case class ProductionCompany(id: Int, name: String)

// Países
case class ProductionCountry(iso_3166_1: String, name: String)

// Idiomas
case class SpokenLanguage(iso_639_1: String, name: String)

// Crew (personal de producción)
case class CrewMember(
  credit_id: String,
  department: String,
  gender: Int,
  id: Int,
  job: String,
  name: String,
  profile_path: Option[String]
)
```

### 3. Procesamiento de Columnas JSON

#### Utilidades Circe (`CirceUtils`)

```scala
object CirceUtils:
  
  def parseGenres(jsonStr: String): List[String] =
    parse(jsonStr).flatMap(_.as[List[Genre]]) match
      case Right(genres) => genres.map(_.name)
      case Left(_) => List.empty
  
  def parseProductionCompanies(jsonStr: String): List[String] =
    parse(jsonStr).flatMap(_.as[List[ProductionCompany]]) match
      case Right(companies) => companies.map(_.name)
      case Left(_) => List.empty
  
  def parseCrew(jsonStr: String): (Option[String], List[String], List[String], Int) =
    parse(jsonStr).flatMap(_.as[List[CrewMember]]) match
      case Right(crew) =>
        val director = crew.find(_.job == "Director").map(_.name)
        val producers = crew.filter(_.department == "Production").map(_.name)
        val writers = crew.filter(_.department == "Writing").map(_.name)
        (director, producers, writers, crew.size)
      case Left(_) =>
        (None, List.empty, List.empty, 0)
```

### 4. Solución para Columna Crew

La columna `crew` contiene información compleja sobre el personal de producción:

**Estructura JSON de crew:**
```json
[
  {
    "credit_id": "52fe4284c3a36847f8024f49",
    "department": "Directing",
    "gender": 2,
    "id": 7879,
    "job": "Director",
    "name": "Christopher Nolan",
    "profile_path": "/path.jpg"
  },
  {
    "department": "Production",
    "job": "Producer",
    "name": "Emma Thomas"
  }
]
```

**Extracción de Información:**

```scala
// Extraer director
val director = crew.find(_.job == "Director").map(_.name)

// Extraer productores
val producers = crew.filter(_.department == "Production").map(_.name)

// Extraer escritores
val writers = crew.filter(_.department == "Writing").map(_.name)

// Contar tamaño del crew
val crewSize = crew.size
```

**Análisis Implementado:**

1. **Top Directores** - Directores más prolíficos
2. **Top Productores** - Productores más activos
3. **Top Escritores** - Guionistas más frecuentes
4. **Departamentos** - Distribución de roles
5. **Estadísticas** - Tamaño promedio, mediana, rangos

### 5. Proceso de Limpieza de Datos

#### Etapa 1: Eliminar Valores Nulos/Ceros

```scala
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
    m.genres.nonEmpty &&      // Validar JSON procesado
    m.crew_size > 0           // Validar crew procesado
  }
```

#### Etapa 2: Validar Rangos Lógicos

```scala
def validarRangos(peliculas: List[Movie]): List[Movie] =
  peliculas.filter { m =>
    m.release_year >= 1888 && m.release_year <= 2025 &&
    m.release_month >= 1 && m.release_month <= 12 &&
    m.release_day >= 1 && m.release_day <= 31 &&
    m.runtime > 0 && m.runtime < 500 &&
    m.vote_average >= 0 && m.vote_average <= 10 &&
    m.`return` >= -1.0 &&
    m.crew_size > 0 && m.crew_size < 1000
  }
```

#### Etapa 3: Filtrar Outliers (Método IQR)

```scala
def detectarOutliers(datos: List[Double]): (Double, Double) =
  val ordenados = datos.sorted
  val q1 = ordenados((ordenados.size * 0.25).toInt)
  val q3 = ordenados((ordenados.size * 0.75).toInt)
  val iqr = q3 - q1
  val limiteInferior = math.max(0, q1 - 1.5 * iqr)
  val limiteSuperior = q3 + 1.5 * iqr
  (limiteInferior, limiteSuperior)
```

**Columnas analizadas para outliers:**
- `budget`
- `revenue`
- `popularity`

## Ejecución

### Requisitos

```scala
libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.5.0",
  "co.fs2" %% "fs2-core" % "3.7.0",
  "co.fs2" %% "fs2-io" % "3.7.0",
  "org.gnieh" %% "fs2-data-csv" % "1.6.1",
  "org.gnieh" %% "fs2-data-csv-generic" % "1.6.1",
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5"
)
```

### Compilar y Ejecutar

```bash
# Compilar el proyecto
sbt compile

# Ejecutar ejemplos básicos de Circe
sbt "runMain EjemploCirce"

# Ejecutar análisis de columnas JSON
sbt "runMain AnalisisColumnasJSON"

# Ejecutar limpieza completa con JSON
sbt "runMain LimpiezaConCirce"

# Ejecutar análisis numérico
sbt "runMain columnasNumericas"
```

## Resultados Esperados

### 1. Análisis de Géneros

```
Top 10 géneros más frecuentes:
  1. Drama                      18,000 películas
  2. Comedy                     12,500 películas
  3. Thriller                    9,200 películas
  4. Action                      8,800 películas
  5. Romance                     7,500 películas
```

### 2. Análisis de Compañías

```
Top 10 productoras:
  1. Warner Bros.                2,150 películas
  2. Universal Pictures          1,980 películas
  3. Paramount Pictures          1,750 películas
  4. 20th Century Fox           1,620 películas
  5. Columbia Pictures           1,450 películas
```

### 3. Análisis de Crew

```
Tamaño promedio de crew:        45.32 personas
Mediana de crew:                38 personas
Rango de tamaños:               1 - 312 personas

Top 10 Directores:
  1. Steven Spielberg            28 películas
  2. Woody Allen                 25 películas
  3. Martin Scorsese            23 películas
```

### 4. Limpieza de Datos

```
Registros originales:                    45,000
Después de eliminar nulos:               38,500 (6,500 eliminados)
Después de validar rangos:               37,200 (1,300 eliminados)
Después de filtrar outliers:             34,800 (2,400 eliminados)

Conservados:                             77.33%
Eliminados:                              22.67%
```

## Lecciones Aprendidas

### Ventajas de Circe

1. **Type Safety** - Errores detectados en compilación
2. **Manejo de Errores** - `Either` para errores funcionales
3. **Decodificación Automática** - Menos código boilerplate
4. **Performance** - Muy eficiente para JSON grandes

### Desafíos Encontrados

1. **JSON Malformado** - Algunos registros con JSON inválido
2. **Campos Opcionales** - Manejo con `Option[T]`
3. **Validación** - Necesidad de validar después de parsear
4. **Memoria** - Procesamiento de 45,000+ registros

### Soluciones Implementadas

1. **Manejo Robusto de Errores**
   ```scala
   parse(json).flatMap(_.as[T]) match
     case Right(data) => data
     case Left(_) => defaultValue
   ```

2. **Validación Post-Parsing**
   ```scala
   peliculas.filter(_.genres.nonEmpty)
   ```

3. **Procesamiento Streaming con fs2**
   ```scala
   Files[IO].readAll(path)
     .through(decodeUsingHeaders[T])
     .map(procesarMovie)
     .compile.toList
   ```

##  Análisis Avanzado Crew

El análisis de crew proporciona insights valiosos:

### Métricas Implementadas

1. **Frecuencia de Participación**
   - Directores más prolíficos
   - Productores más activos
   - Guionistas frecuentes

2. **Distribución de Roles**
   - Departamentos más grandes
   - Especialización por rol
   - Colaboraciones frecuentes

3. **Estadísticas de Tamaño**
   - Promedio de personas por película
   - Variabilidad (min, max, mediana)
   - Detección de producciones grandes/pequeñas

## Conclusiones

1. **Circe es Poderoso**: Excelente para trabajar con JSON complejo
2. **Limpieza Esencial**: ~22% de datos requirieron limpieza
3. **Crew es Complejo**: La columna crew tiene estructura rica en información
4. **Scala Funcional**: Combinación de fs2, cats-effect y Circe es robusta
5. **Type Safety**: Reduce errores significativamente

## Referencias

- [Circe Documentation](https://circe.github.io/circe/)
- [fs2 Documentation](https://fs2.io/)
- [Cats Effect](https://typelevel.org/cats-effect/)
- [Scala 3 Book](https://docs.scala-lang.org/scala3/book/)

##  Autor

Proyecto desarrollado como parte del análisis de datos de películas con Scala funcional.
Diego Sebastian Lojan Sisalima
---

**Nota**: Este proyecto demuestra el poder de Scala para procesamiento de datos estructurados y semi-estructurados, combinando programación funcional con análisis estadístico robusto.
