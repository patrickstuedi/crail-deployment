case class record(key: Array[Byte], value: Array[Byte])
val rec1 = record(Array(0,1,2,3,4,5,6,7,8,9), Array(10,11,12,13,14,15,16,17,18,19,20))
val rec2 = record(Array(1,2,3,4,5,6,7,8,9,0), Array(10,11,12,13,14,15,16,17,18,19,20))
val rec3 = record(Array(2,3,4,5,6,7,8,9,0,1), Array(10,11,12,13,14,15,16,17,18,19,20))
val recordDS = util.Random.shuffle(Seq(rec3, rec2, rec1)).toDS
val sortedDS = recordDS.sort("key")
recordDS.show
recordDS.schema

sortedDS.schema
sortedDS.show
