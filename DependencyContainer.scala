// The dependency container class is a wrapper for the LZ77 compression
// functionality.


class DependencyContainer() {
  class RDDMetaData(_id: Int, _rddtype: String, _deps: Array[Int]) {
    def id() : Int = _id
    def rddtype() : String = _rddtype
    def deps() : Array[Int] = _deps
  }

  def compare(v1: RDDMetaData, v2: RDDMetaData): Boolean = {
    // if(v1.id == v2.id && v1.rddtype == v2.rddtype && v1.deps.sorted == v2.deps.sorted)
    if(v1.rddtype == v2.rddtype)
      return true
    else 
      return false
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


  // function to compress a repetitive dependency tree
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

}