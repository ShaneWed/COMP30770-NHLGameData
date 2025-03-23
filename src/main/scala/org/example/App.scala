package org.example

import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    println("Initializing SparkConf & SparkContext")
    val conf = new SparkConf()
      .setAppName("sparkTest")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)

    //goalsOccurences(sc)
    //panarinHatTricks(sc)
    homeAwayGoals(sc)

    sc.stop()
  }

  private def homeAwayGoals(sc: SparkContext): Unit = {
    val startTime = System.currentTimeMillis()

    val game = "/home/shane/Documents/UCD/COMP30770/project/gameNoHeader.csv"
    val rddGame = sc.textFile(game)

    val rddGameMapped = rddGame.map(column => column.split(","))

    val header = rddGameMapped.first()
    val rddGameFormatted = rddGameMapped.filter(row => row != header)
      .map(row => {
        val awayTeamId = row(4).replaceAll("\"", "").trim.toInt
        val homeTeamId = row(5).replaceAll("\"", "").trim.toInt
        val awayGoals = row(6).replaceAll("\"", "").trim.toInt
        val homeGoals = row(7).replaceAll("\"", "").trim.toInt
        (awayTeamId, homeTeamId, awayGoals, homeGoals)
      })

    val awayTeamGoals = rddGameFormatted.map {
      case (awayTeamId, _, awayGoals, _) => (awayTeamId, (awayGoals, 1))
    }

    val homeTeamGoals = rddGameFormatted.map {
      case (_, homeTeamId, _, homeGoals) => (homeTeamId, (homeGoals, 1))
    }

    val awayGoalTotal = awayTeamGoals.reduceByKey((a, b) => {
      val (awayGoals1, count1) = a
      val (awayGoals2, count2) = b
      (awayGoals1 + awayGoals2, count1 + count2)
    })

    val homeGoalTotal = homeTeamGoals.reduceByKey((a, b) => {
      val (homeGoals1, count1) = a
      val (homeGoals2, count2) = b
      (homeGoals1 + homeGoals2, count1 + count2)
    })

    val combinedGoals = awayGoalTotal.fullOuterJoin(homeGoalTotal).mapValues {
      case (Some(away), Some(home)) => (away._1, away._2, home._1, home._2)
      case (Some(away), None) => (away._1, away._2, 0, 0)
      case (None, Some(home)) => (0, 0, home._1, home._2)
      case _ => (0, 0, 0, 0)
    }

    val averages = combinedGoals.mapValues {
      case (awayGoals, awayCount, homeGoals, homeCount) =>
        val goalTotal = awayGoals + homeGoals
        val gamesPlayed = awayCount + homeCount
        val averageAwayGoals = if (awayGoals != 0) awayGoals.toDouble / awayCount else 0
        val averageHomeGoals = if (homeGoals != 0) homeGoals.toDouble / homeCount else 0
        (awayGoals, homeGoals, gamesPlayed, goalTotal, averageAwayGoals, averageHomeGoals)
    }

    val sorted = averages.sortBy({case (teamId, _) => teamId}, ascending = true)

    println("\n\n\n\n\nRuntime: system" + (System.currentTimeMillis() - startTime) + "\n\n\n\n\n")
    println("\n\n\n\n\nRuntime: sc" + (System.currentTimeMillis() - sc.startTime) + "\n\n\n\n\n")
    sorted.saveAsTextFile("goalAverages")
  }

  private def goalsOccurences(sc: SparkContext): Unit = {
    val numOfGoals = 4
    val gameSkaterStats = "/home/shane/Documents/UCD/COMP30770/project/extracted/game_skater_stats.csv"
    val game = "/home/shane/Documents/UCD/COMP30770/project/extracted/game.csv"

    val rddGameSkaterStats = sc.textFile(gameSkaterStats)
    val rddGame = sc.textFile(game)

    val rddGameSkaterStatsSplit = rddGameSkaterStats.map(line => line.replace("\"", "").split(",").map(_.trim))
    val header = rddGameSkaterStatsSplit.first()
    val rddGameSkaterStatsWithoutHeader = rddGameSkaterStatsSplit.filter(row => row != header)

    val playerStats = rddGameSkaterStatsWithoutHeader.filter(columns => {
      try {
        columns(5).toInt >= 1
      } catch {
        case e: Exception => false
      }
    })

    val playerStatsFormatted = playerStats.map(columns => columns.mkString(", "))

    playerStatsFormatted.saveAsTextFile(numOfGoals + "goalGames-stats")
  }

  private def panarinHatTricks(sc: SparkContext): Unit = {
    val playerId = 8478550 // Artemi Panarin
    val gameSkaterStats = "/home/shane/Documents/UCD/COMP30770/project/extracted/game_skater_stats.csv"
    val game = "/home/shane/Documents/UCD/COMP30770/project/extracted/game.csv"

    val rddGameSkaterStats = sc.textFile(gameSkaterStats)
    val rddGame = sc.textFile(game)

    val rddGameSkaterStatsSplit = rddGameSkaterStats.map(line => line.replace("\"", "").split(",").map(_.trim))
    val header = rddGameSkaterStatsSplit.first()
    val rddGameSkaterStatsWithoutHeader = rddGameSkaterStatsSplit.filter(row => row != header)

    val playerStats = rddGameSkaterStatsWithoutHeader.filter(columns => {
      try {
        columns(1).toInt == playerId && columns(5).toInt == 3
      } catch {
        case e: Exception => false
      }
    })

    val playerStatsFormatted = playerStats.map(columns => columns.mkString(", "))

    playerStatsFormatted.saveAsTextFile(playerId + "-stats")
  }
}
