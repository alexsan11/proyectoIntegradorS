
import cats.effect.IO
import com.github.tototoshi.csv.*
import org.nspl._
import org.nspl.awtrenderer.*
import org.saddle._
import cats.*
import doobie.*
import doobie.implicits.*
import java.io.File
import cats.effect.unsafe.implicits.global
import doobie.Transactor
import breeze.plot._
import breeze.plot.plot
import org.nspl.saddle._




object Graficas {
  @main
  def integral2() =
    val pathDataFile3 = "C://Users//aysanchez6//Desktop//data//dsPartidosYGoles.csv"
    val reader3 = CSVReader.open(new File(pathDataFile3))
    val contentFile3: List[Map[String, String]] = reader3.allWithHeaders()
    reader3.close()

    // Direccion de los archivos csv de aliniaciones por torno
    val pathDataFile4 = "C://Users//aysanchez6//Desktop//data//dsAlineacionesXTorneo-2.csv"
    val reader4 = CSVReader.open(new File(pathDataFile4))
    val contenFile4: List[Map[String, String]] = reader4.allWithHeaders()
    reader4.close()
    val rutaArchivo = "C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\CSV_PartidosECantidad.png"

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/dbmundial",
      user = "root",
      password = "UTPL2023",
      logHandler = None
    )
    /*densityNumeroC(contenFile4)
    frecuenciaGoles(contentFile3)
    ganadoresdeMundiales(contentFile3)*/

    // metodo grafico DB
    /*CapacidadMaxE(EstadiosCapMgrafico().transact(xa).unsafeRunSync())
    graficaEdadGoles(edadGoles().transact(xa).unsafeRunSync())
    graficaPromedioJug(promedioedadJug().transact(xa).unsafeRunSync())
    graficaDatosEstadios(obtenerDatosEstadios().transact(xa).unsafeRunSync())
    graficaFrePosition(frecuenciaPosition().transact(xa).unsafeRunSync())
    graficaJugadorGoles(jugadorgoles().transact(xa).unsafeRunSync())*/
    graficaNequiposT(numEquiposT().transact(xa).unsafeRunSync())

  // numero de camiseta de los delanteros-> Genera grafico density plot
  def densityNumeroC(data: List[Map[String, String]]): Unit =
    val listNroShirt: List[Double] = data
      .filter(row => row("squads_position_name") == "forward" && row("squads_shirt_number") != "0")
      .map(row => row("squads_shirt_number").toDouble)

    val densityforwardNumber = xyplot(density(listNroShirt.toVec.toSeq) -> line())(
      par
        .xlab("Nro de camiseta jugador") // eje x
        .ylab("frecuencia") // eje y
        .main("numero de camiseta DL") // grafica nombre
    )

    pngToFile(new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\CSV_NumeroCamisetaDelanteros.png"), densityforwardNumber.build, 1000)
  // Frecuencia de goles de cada mundial
  def frecuenciaGoles(data: List[Map[String, String]]): Unit =
    val frecuenci = data
      .map(x => (x("matches_home_team_score"), x("matches_away_team_score")))
      .groupBy(_._2)
      .map(k => (k._1, k._2.size.toDouble))

    val densitFrecuencia = xyplot(density(frecuenci.toSeq.map(_._2).toIndexedSeq) -> line())(
      par
        .xlab("Numero de Goles")
        .ylab("Frec.")
        .main("Frecuencia de goles Mundial")

    )
    pngToFile(new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\CSV_FrecuenciaGoles.png"), densitFrecuencia.build, 1000)

  //cantidad de mundiales que ha ganado cada pais
  def ganadoresdeMundiales(data: List[Map[String, String]]) =
    val winners = data
      .map(row => (row("matches_tournament_id"), row("tournaments_winner")))
      .distinct
      .groupBy(_._2)
      .map(row => (row._1, row._2.size.toDouble))

    val ganadoresIndices = Index(winners.map(r => r._1).toArray)
    val valores = Vec(winners.map(valores => valores._2).toArray)

    val series = Series(ganadoresIndices, valores)
    val bar1 = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(2)),
      color = RedBlue(80, 171))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("pais ganadores")
        .ylab("max cantidad de mundiales ganados")
        .main("mundiales")
    )
    pngToFile(new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\CSV_FrecGanadores.png"), bar1.build, 1000)

  // graficar char desde la DB
  // 1. sacar la capacidad de estadios de una pais especifico
  def EstadiosCapMgrafico(): ConnectionIO[List[(String, Double)]] = {
    sql"""
          SELECT s.name as StadiumName, s.capacity as Capacity
    FROM stadium s
    INNER JOIN country c ON s.countryId = c.countryId
    WHERE c.countryName = 'Brazil';
       """
      .query[(String, Double)]
      .to[List]
  }

  def CapacidadMaxE(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("Estadio")
        .ylab("Capacidad")
        .main("Capacidad Maxima")
    )

    pngToFile(new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_EstadiosCap.png"), barPlot.build, 5000)
  }

  // Gráfica de dispersión sobre la relación entre la edad de los jugadores (1990) y el número de goles que han marcado en los partidos.
  def edadGoles(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
      SELECT (YEAR(CURRENT_DATE) - YEAR(birthDay)) AS edad, COUNT(goalId) AS total_goles
          FROM player
          INNER JOIN squad ON player.playerid = squad.playerid
          INNER JOIN goal ON squad.playerid = goal.playerId
          WHERE YEAR(birthDay) >=1990
          GROUP BY edad;
    """
      .query[(Int, Int)]
      .to[List]
  }

  def graficaEdadGoles(data: List[(Int, Int)]): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2), '+')
    p.xlabel = "Edad"
    p.ylabel = "Goles"
    f.saveas("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_PartidosEGoles.png")

  }

  // Gráfica de promedio de edad de jugadores en funcion de su posicion del torneo de "2022"
  def promedioedadJug(): ConnectionIO[List[(String, Double)]] = {
    sql"""
      SELECT s.positionName, AVG(YEAR(CURDATE()) - YEAR(p.birthDay)) AS averageAge
    FROM squad s
    JOIN player p ON s.playerid = p.playerid
    JOIN tournament t ON s.tournamentid = t.tournamentid
    WHERE t.tournamentYear = 2022  -- Filtrar por el año del torneo
    GROUP BY s.positionName;
    """
      .query[(String, Double)]
      .to[List]
  }

  def graficaPromedioJug(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("Posiciones")
        .ylab("Promedio")
        .main("Promedio Edades- Posiciones")
    )

    pngToFile(
      new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_PromedioJug.png"),
      barPlot.build,
      5000
    )
  }
  //grafica para obtener numero de partidos jugados en los estadios de cada capacidad
  def obtenerDatosEstadios(): ConnectionIO[List[(String, Double)]] = {
    sql"""
        SELECT s.name, count(*)
          FROM stadium s
          INNER JOIN MATCHS m ON s.stadiumId = m.stadiumId
          WHERE  capacity > 70000
          GROUP BY s.name
      """.query[(String, Double)].to[List]
  }
// sacar la catndiada de partidos jugados del los estadios mayores a 70000 de capacidad
  def graficaDatosEstadios(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("ESTADIOS")
        .ylab("PARTIDOS JUGADOS")
        .main("PARTIDOS JUGADOS POR ESTADIO")
    )

    pngToFile(
      new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_PARTIDOS.png"),
      barPlot.build,
      5000
    )
  }

  def frecuenciaPosition(): ConnectionIO [List[(String, Double)]] ={
    sql"""
        SELECT
        CASE
            WHEN p.goalKeeper = 1 THEN 'Goalkeeper'
            WHEN p.defender = 1 THEN 'Defender'
            WHEN p.midfielder = 1 THEN 'Midfielder'
            WHEN p.forward = 1 THEN 'Forward'
        END AS positionName,
        COUNT(*) AS numPlayers
    FROM player p
    GROUP BY positionName;
      """.query[(String, Double)].to[List]

  }
  def graficaFrePosition(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("Posiciones")
        .ylab("Frecuencia")
        .main("CANTIDAD DE JUGADORES EN DIFERENTES POSICIONES")
    )

    pngToFile(
      new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_CantidadPosicion.png"),
      barPlot.build,
      5000
    )

  }
// catindad de partidos que se jugo por cada munidial
  def jugadorgoles(): ConnectionIO[List[(String, Double)]] = {
    sql"""
        SELECT  p.familyName, COUNT(g.goalId) AS numGoals
    FROM player p
    INNER JOIN goal g ON p.playerId = g.playerId
    GROUP BY p.playerId
    HAVING COUNT(g.goalId) > 12;
      """
      .query[(String, Double)]
      .to[List]
  }

  def graficaJugadorGoles(data: List[(String, Double)]): Unit = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = barplotHorizontal(
      series,
      color = RedBlue(-2, 2)
    )(par
      .xLabelRotation(-90)
      .xlab("NOMBRE DEL JUAGOR CON MAS DE 10 GOLES")
      .ylab("CANTIDAD DE GOLES")
      .main("Número de partidos del Mundial por estadio")
    )

    pngToFile(
      new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_golesJuagador.png"),
      barPlot.build,
      5000
    )
  }
  def numEquiposT(): ConnectionIO[List[(String, Double)]] = {
    sql"""
       SELECT
        t.tournamentId,
        COUNT(g.goalId) AS TotalGoals
    FROM tournament t
    INNER JOIN matchs m ON t.tournamentId = m.tournamentId
    INNER JOIN goal g ON m.matchId = g.matchId
    WHERE t.tournamentYear IN (2022, 2018, 2014, 2010, 2006)
    GROUP BY t.tournamentId;
      """
      .query[(String, Double)]
      .to[List]

  }

  def graficaNequiposT(data: List[(String, Double)]): Unit = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = barplotHorizontal(
      series,
      color = Color.GREEN
    )(par
      .xLabelRotation(-90)
      .xlab("AÑO MUNDIAL")
      .ylab("CANTIDAD DE GOLES")
      .main("GOLES POR LOS MUNDIALES")
    )

    pngToFile(
      new File("C:\\Users\\aysanchez6\\Desktop\\ciclo3\\ProgramacionFR\\proyectoIntegrador\\GraficaDB\\BD_cantidadgolesM.png"),
      barPlot.build,
      5000
    )


    }



}
