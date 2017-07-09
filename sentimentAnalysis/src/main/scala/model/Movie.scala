/**
  * Created by Doul on 05/07/2017.
  */

package model

case class Movie (
                 budget: Long,
                 gross: Long,
                 title: String,
                 genres: Seq[String],
                 score: Float,
                 year: Int,
                 director: Option[String],
                 actors: Seq[String],
                 reviews: Seq[Review],
                 sentimentScore: Option[Float]
                 )


