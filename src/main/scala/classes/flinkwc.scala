package classes

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object flinkwc{
    def main(args: Array[String]): Unit = {
        // the port to connect to
        val port: Int = 9000

        // get the execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // get input data by connecting to the socket
        val text = env.socketTextStream("localhost", port, '\n')

        // parse the data, group it, window it, and aggregate the counts
        val windowCounts = text
          .flatMap { w => w.split("\\s") }
          .map { w => (w, 1) }
          .keyBy(0)
          .timeWindow(Time.seconds(5), Time.seconds(1))
          .sum(1)

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1)

        env.execute("Socket Window WordCount")
    }
}
