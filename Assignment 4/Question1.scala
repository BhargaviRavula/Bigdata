case class Object(id:String,ratings:Array[Double]) {
  def difference(that: Object) : Double = {
	  var thisLength=0.0
	  var thatLength=0.0
	  var difference = 0.0
	  var totalLength = 0.0
	  for (i <- 0 until ratings.length){
	  	thisLength = this.ratings(i);
	  	thatLength = that.ratings(i);
		difference = thisLength - thatLength;
		totalLength += java.lang.Math.pow(difference,2);
	  }
	  var score = java.lang.Math.sqrt(totalLength)
	  
	  return score;
  }
}

val itemuserdat = sc.textFile("hdfs://cshadoop1/bxr140530/itemusermat")

val itemuserArr = itemuserdat.toArray

val movies = collection.mutable.ArrayBuffer[Object]()

for(i <- 0 until itemuserArr.length){
	val id = itemuserArr(i).split(' ')(0);
	val values = itemuserArr(i).split(" ",2)(1);
	
	val arr = values.split(' ');
	val arrdouble = arr.map(_.toDouble);
	movies += new Object(id, arrdouble);
}

val K = 10

val randomIndices = collection.mutable.HashSet[Int]()
val random = new scala.util.Random()
while (randomIndices.size < K) {
    randomIndices += random.nextInt(movies.size)
}

//get random values
val centroids = collection.mutable.ArrayBuffer[Array[Double]]()
for(j <- 0 until movies.size){
	if(randomIndices.contains(j)){
		val point = movies(j).ratings;
   		centroids += point;
	}
}

val D = Array.ofDim[Double](10,1000)
val G = Array.ofDim[Int](10,1000)

for (i <- 0 until D.size){
	val temp = new Object("temp",centroids(i));
	for(j<- 0 until movies.size)
	{
     	val dist = temp.difference(movies(j));
		D(i)(j) = dist;
	}
}

//get the movie with the least distance from centroid

for(i<-0 until movies.size){
	var min = 1000.0;
	var minRow = 0;
	for(j<-0 until D.size){
		if(D(j)(i) < min){
			min = D(j)(i);
			minRow = j;
		}
	}
	for(t<-0 until G.size){
		if(t == minRow){
			G(t)(i) = 1;
		}else{
			G(t)(i) = 0;
		}
	}
}

var newCentroids = collection.mutable.ArrayBuffer[Array[Double]]()

for(y<-0 until G.size){
	var count = 0;
	var sum = Array.ofDim[Double](6040);
	for(z<-0 until G(y).size){
		if(G(y)(z) == 1){
			count += 1;
			for(x<-0 until movies(z).ratings.size){
				sum(x) += movies(z).ratings(x);
			}
		}
	}
	//get the new centroid
	for(t<-0 until 6040){	//6040 is the number of users
		if(count>0){
			sum(t) = sum(t)/count;
		}	
	}

	newCentroids += sum;

}

//repeat
//compare two matrices by p.deep == q.deep

var newD = Array.ofDim[Double](10,1000)
var newG = Array.ofDim[Int](10,1000)

while(G.deep != newG.deep){
	//copy newG into G
	for(i<-0 until G.size){			//keeping a copy of prev computation
		for(j<-0 until G(i).size){
			G(i)(j) = newG(i)(j);
		}
	}

		//calculate distances
	for (i <- 0 until newD.size){
		val temp = new Object("temp",newCentroids(i));
		for(j<- 0 until movies.size)
		{
	     	val dist = temp.difference(movies(j));
			newD(i)(j) = dist;
		}
	}

	//get the movie with the least distance from centroid
	//and fill up G matrix accordingly
	for(i<-0 until movies.size){
		var min = 1000.0;
		var minRow = 0;
		for(j<-0 until newD.size){
			if(newD(j)(i) < min){
				min = newD(j)(i);
				minRow = j;
			}
		}
		for(t<-0 until newG.size){
			if(t == minRow){
				newG(t)(i) = 1;
			}else{
				newG(t)(i) = 0;
			}
		}
	}

	newCentroids = collection.mutable.ArrayBuffer[Array[Double]]()

	for(y<-0 until newG.size){
		var count = 0;
		var sum = Array.ofDim[Double](6040);
		for(z<-0 until newG(y).size){
			if(newG(y)(z) == 1){
				count += 1;
				for(x<-0 until movies(z).ratings.size){
					sum(x) += movies(z).ratings(x);
				}
			}
		}
		//get the new centroid
		for(t<-0 until 6040){	
			if(count>0){
				sum(t) = sum(t)/count;
			}	
		}

		newCentroids += sum;
	}
}



//got new groups
var groups = collection.mutable.ArrayBuffer[collection.mutable.ArrayBuffer[String]]()
for(y<-0 until newG.size){
	var movieCollection = collection.mutable.ArrayBuffer[String]()
	//finding movies in each group
	for(z<-0 until newG(y).size){
		if(newG(y)(z) == 1){
			movieCollection += movies(z).id; 
		}
	}
	groups += movieCollection;


}

val moviesData = sc.textFile("hdfs://cshadoop1.utdallas.edu/hw4fall/movies.dat")
var moviesMap= moviesData.map(l=>l.split("::")).map(l=> (l(0),l(1),l(2)))

//printing the names
for(p<-0 until groups.size){
	var count = 0;
	for(j<-0 until groups(p).size){
		if(count<5){
			val m = moviesMap.filter(x=>x._1.contains(groups(p)(j)))
			m.foreach(x=>println(x))
		}
		count += 1;
	}
}







