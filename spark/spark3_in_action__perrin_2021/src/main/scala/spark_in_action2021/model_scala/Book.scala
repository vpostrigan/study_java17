package spark_in_action2021.model_scala

import java.time.LocalDate
import java.util.Date

case class Book(authorId:Int, title:String, releaseDate:LocalDate, link:String, id:Int=0)
