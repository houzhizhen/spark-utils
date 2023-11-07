package org.houzhizhen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import java.net.URI

package object zlass {

  def getJavaClassPath(i: Int = 0): String = {

    val classpath = System.getProperty("java.class.path")
    System.out.println("getJavaClassPath: " + classpath)
    System.out.flush()
    val sb = new StringBuilder(i)
    sb.append(" ")
    sb.append(classpath)
    val classPathValues = classpath.split(File.pathSeparator)
    for (classPath <- classPathValues) {
      println(classPath)
    }
    sb.toString()
  }

  def uploadFilesInClassPath(pathStr: String, conf: Configuration): String = {
    val uri = new URI(pathStr)
    val path = new Path(pathStr)
    val fs = FileSystem.get(uri, conf)
    val classpath = System.getProperty("java.class.path")
    val classPathValues = classpath.split(File.pathSeparator)
    for (classPath <- classPathValues) {
      val file = new File(classPath)
      if (file.isDirectory) {
        uploadDirectory(file, new Path(path, file.getName), fs)
      } else if (file.isFile() && file.exists()){
        uploadFile(file, new Path(path, file.getName), fs);
      }
    }
    // Don't close the file system, since it may be used by framework.
    // fs.close();

    val sb = new StringBuilder()
    sb.append(" ")
    sb.append(classpath)
    sb.toString()
  }

  def uploadFile(file: File, path: Path, fs: FileSystem): Unit = {
    if (fs.exists(path)) {
      println("File " + file.getAbsolutePath + " already exists in target path.")
      return;
    }
    println("uploading file: " + file.getPath + " to " + path.toString)
    fs.copyFromLocalFile(new Path(file.getAbsolutePath), path)
  }
  def uploadDirectory(file: File, path: Path, fs: FileSystem): Unit = {
    if (fs.exists(path)) {
      println("Directory " + file.getAbsolutePath + " already exists in target path.")
      return;
    }
    println("uploading directory: " + file.getPath + " to " + path.toString)
    fs.mkdirs(path);
    for (child <- file.listFiles()) {
      if (child.isDirectory) {
        uploadDirectory(child, new Path(path, child.getName), fs)
      } else if (child.isFile() && child.exists()) {
        uploadFile(child, new Path(path, child.getName), fs)
      } else {
        println("File " + child.getAbsolutePath + " is not a directory or file.")
      }
    }
  }
}
