/**
  * Created by Doul on 05/07/2017.
  */

package model

import play.api.libs.json.Json

case class Movie (
                 budget: Int,
                 gross: Int,
                 title: String,
                 genres: Seq[String],
                 score: Float,
                 year: Int,
                 direction: Option[String],
                 actors: Seq[String],
                 reviews: Seq[Review],
                 finalScore: Option[Float]
                 )

object Movie{
  implicit val movieFormat = Json.format[Movie]
}

