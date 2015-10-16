class RDD(private var _id: Int, private var deps: Seq[RDD]) {
	def id: _id = {
		if (_id == null) 
			0
		_id
	}
	def getDependencies: Seq[RDD] = deps
}



class T() {
  object Ctr {
    var ctr_l: Seq[Int] = null

    def set(l: Seq[Int]) {
      ctr_l = l
    }
    def add(x: Int) {
      ctr_l = ctr_l :+ x
    }
    def getl(): Seq[Int] = ctr_l
  }

  def add(x: Int) {
    Ctr.add(x)
  }
  def getl() {
    return Ctr.getl()
  }
}




object Ctr {
  var m_l: Seq[Int] = null

  def set(l: Seq[Int]) {
    m_l = l
  }
  def add(x: Int) {
    m_l = m_l :+ x
  }
  def getl(): Seq[Int] = m_l
}


val splitedLines = input.map(line => line.split(" ")).map(words => (words(0), 1)).reduceByKey{(a,b) => a + b}