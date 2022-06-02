package BigData.Transformations

import BigData.Case.Names
import BigData.Case.Actors

object Transformations {

  def isactor(actors_row: Actors): Boolean = {
    if(actors_row.category==null) return false;
    return actors_row.category=="actor";
  }

  def shorterthan(names_row: Names,h:Int): Boolean = {
    if(names_row.height==null) return false;
    return Integer.valueOf(names_row.height)<h;
  }

}
