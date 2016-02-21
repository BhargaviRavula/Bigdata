import org.apache.spark.util.Vector

def corr(x : Vector, y : Vector) : Double = {
    
    
    val N = x.length;
    var sum_sq_x = 0.0;
    var sum_sq_y = 0.0;
    var sum_coproduct = 0.0;
    var mean_x = x(0);
    var mean_y = y(0);
    for (i <- 2 to N) {
      val sweep = (i - 1.0) / i;
      val delta_x = x(i-1) - mean_x;
      val delta_y = y(i-1) - mean_y;
      sum_sq_x += (delta_x * delta_x * sweep);
      sum_sq_y += (delta_y * delta_y * sweep);
      sum_coproduct += (delta_x * delta_y * sweep);
      mean_x += (delta_x / i);
      mean_y += (delta_y / i);
    }
    val pop_sd_x = Math.sqrt( sum_sq_x / N );
    val pop_sd_y = Math.sqrt( sum_sq_y / N );
    val cov_x_y = sum_coproduct / N;
    return cov_x_y / (pop_sd_x * pop_sd_y);
  }
   
val data = sc.textFile("/itemMovie");
val mData = sc.textFile("/movie");
val initialMat = data.map(line=>(line.split(" ")(0),parseVector((line.split(" ").drop(1).mkString(" ")))))
val movies = mData.map(line=>(line.split("::")(0),(line.split("::").drop(1).mkString(" "))))
val joined = movies.join(initialMat)
val formatted= joined.map(line=>(line._1+" "+line._2._1,line._2._2))

val movieId=readLine()
var movieData = initialMat.filter(line=>line._1==movieId)
var id= movieData.map(p=>p._1).collect
var compVal = movieData.map(p=>p._2).collect
var pCof = formatted.map(p=>(corr(compVal(0),p._2),(p._1,p._2)))
pCof.filter(p=>p._2._1.split(" ")(0)!=id(0)).map(p=>(p._1,p._2._1)).top(1).foreach(println)










   
   
