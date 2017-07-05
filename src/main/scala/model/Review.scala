package model

import play.api.libs.json.Json

/**
  * Created by Doul on 05/07/2017.
  */
case class Review (
                  title: String,
                  score: Float,
                  content: String,
                  sentiment: Int
                  )

object Review {
  implicit val reviewFormat = Json.format[Review]
}