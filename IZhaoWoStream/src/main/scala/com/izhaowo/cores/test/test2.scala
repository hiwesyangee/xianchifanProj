package com.izhaowo.cores.test

import com.izhaowo.cores.engine.XianChiFanUtils

object test2 {
  def main(args: Array[String]): Unit = {
    //        ctime月：901eb097-8f4d-4e00-8d2e-e650041bd79a,1,2019,2,3
    //        wedding月：901eb097-8f4d-4e00-8d2e-e650041bd79a,2,2019,3,2
    XianChiFanUtils.updateWorkerLimitAll("901eb097-8f4d-4e00-8d2e-e650041bd79a", 1, 2019, 2, 3)

    //    XianChiFanUtils.updateWorkerLimitAll("901eb097-8f4d-4e00-8d2e-e650041bd79a", 2, 2019, 3, 2)

  }

}
