import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

class tests extends FunSuite
{
  test("checkProf")
  {
    val mapper = new authorMapper()
    assert(mapper.isUIC_CS_prof("Jakob Eriksson"))
  }

  test("checkProf2")
  {
    val mapper = new authorMapper()
    assert(!mapper.isUIC_CS_prof("Jakob ee"))
  }

  test("isInt")
  {
    val reducer = new authorReducer()
    assert(reducer.toInt(new Text("1234")) > 0)
  }

  test("isInt2")
  {
    val reducer = new authorReducer()
    assert(reducer.toInt(new Text("qwer")) == 0)
  }

  test("isArr")
  {
    val reducer = new authorReducer()
    val it = Iterable[Text](new Text(""))
    assert(reducer.toArr(it).length > 0)
  }

  test("isArr2")
  {
    val reducer = new authorReducer()
    val it = Iterable[Text]()
    assert(reducer.toArr(it).length == 0)
  }
}
