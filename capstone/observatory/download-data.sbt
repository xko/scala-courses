
import sbt.io.IO._

val DataUrl = url("https://moocs.scala-lang.org/files/scala-capstone-data.zip")

lazy val downloadData = taskKey[Seq[File]](s"Download additional data from $DataUrl")

downloadData := {
  val log = streams.value.log
  val download = FileFunction.cached( streams.value.cacheDirectory / "data") { _ =>
    log.info(s"Downloading and unzipping $DataUrl - takes a while...")
    val files = unzipURL(DataUrl, (Compile / resourceDirectory).value.getParentFile )
    log.info("...done downloading")
    files
  }
  download(Set(file("download-data.sbt"))).toSeq
}

Compile / compile := (Compile / compile).dependsOn(downloadData).value

Compile / unmanagedResources / excludeFilter ~= ( _ ||
  ( new SimpleFileFilter(_.toPath.getParent.endsWith("main/resources"))
    && "*.csv" )
)

