// Raw implementation of the LZ77 compression algorithm for RDDs
/*
// ****** Sample Usage
// Copy and paste all the code into the Scala Spark shell. Run the
// following command for a sample output

var t1 = sc.parallelize(Array(1,2,3))

t1 = t1.union(t1)

t1.collect()

for(i <- 1 to 10) {
 t1 = t1.map(x => x+1)
 }

t1.collect()
//res90: Array[Int] = Array(11, 12, 13, 11, 12, 13)

var g = rddGraph(t1)

lz77_compress(g.reverse)
*/

import scala.reflect.ClassTag


class RDDMetaData(_id: Int, _rddtype: String, _deps: Array[Int]) {
  def id() : Int = _id
  def rddtype() : String = _rddtype
  def deps() : Array[Int] = _deps
}


def rddGraph(rdd: org.apache.spark.rdd.RDD[_]): Array[RDDMetaData] = {
  var graph = Array[RDDMetaData]()
  var deps = Array[Int]()
  for(i <- 0 to rdd.dependencies.length-1) {
    graph = graph ++ rddGraph(rdd.dependencies(i).rdd)
    deps = deps ++ Array(rdd.dependencies(i).rdd.id)
  }

  var cur = new RDDMetaData(rdd.id, rdd.getClass.getName, deps)

  graph = Array(cur) ++ graph
  return graph
}


def printGraph(l: Array[RDDMetaData]) {
  for(i <- 0 to l.length-1) {
    var cur = l(i)
    println(cur.id)
    println(cur.rddtype)
    var pars = cur.deps
    for(j <- 0 to pars.length-1) {
      var pid = pars(j)
      println(s"  $pid")
    }
    println("--------")
  }
}



def compare(v1: RDDMetaData, v2: RDDMetaData): Boolean = {
  // if(v1.id == v2.id && v1.rddtype == v2.rddtype && v1.deps.sorted == v2.deps.sorted)
  if(v1.rddtype == v2.rddtype)
    return true
  else 
    return false
} 


// This implementation is for an array of the RDDMetaData class
def lz77_compress(items: Array[RDDMetaData]): Array[(Int, Int, _)] = {
  var compress = Array[(Int, Int, _)]()

  var w_start = 0
  var i=0
  while(i < items.length) {
    var back = i-1
    var moveback = 0
    var newi = 0
    var mx = 0
    var mx_ix = 0

    while(back >= 0) {
      if(compare(items(back), items(i))) {
        var check = 0

        while(i+check < items.length && compare(items(back+check),items(i+check)) ) {
          check += 1
        }

        if(check > mx) {
          mx_ix = back
          mx = check
        }
      }
      back -= 1
    }

    moveback = i-mx_ix
    newi = i+mx

    i = newi + 1
    if(i <= items.length) {
      compress = compress :+ (mx_ix, mx, items(i-1))
    }
    else {
      compress = compress :+ (mx_ix, mx, Unit)
    }
  }
  return compress
}











// This implementation is for an array of strings
/*
def lz77_compress(str: String): Array[(Int, Int, _)] = {
  var compress = Array[(Int, Int, _)]()

  var w_start = 0
  var i=0
  while(i < str.length) {
    var back = i-1
    var moveback = 0
    var newi = 0
    var mx = 0
    var mx_ix = 0

    while(back >= 0) {
      if(str(back) == str(i)) {
        var check = 0

        while(i+check < str.length && str(back+check) == str(i+check)) {
          check += 1
        }

        if(check > mx) {
          mx_ix = back
          mx = check
        }
      }
      back -= 1
    }

    moveback = i-mx_ix
    newi = i+mx

    i = newi + 1
    if(i <= str.length) {
      compress = compress :+ (mx_ix, mx, str(i-1))
    }
    else {
      compress = compress :+ (mx_ix, mx, Unit)
    }
  }
  return compress
}


def lz77_decompress(compressed: Array[(Int, Int, _)]) : String = {
  var str = ""

  for(cur <- 0 to compressed.length-1) {
    var item = compressed(cur)
    var start = item._1
    var count = item._2
    var next = item._3

    for(i <- 0 to count-1) {
      str += str(start+i)
    } 

    if(next != Unit) {
      str += next
    }
  }
  return str
}

*/
